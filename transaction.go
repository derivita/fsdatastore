// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package datastore

import (
	"context"
	"strings"

	gax "github.com/googleapis/gax-go/v2"
	"golang.org/x/xerrors"
	pb "google.golang.org/genproto/googleapis/firestore/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// based on https://github.com/googleapis/google-cloud-go/blob/2e7525dafa4cad3994a5b04ad2d49fc6da1bb506/firestore/transaction.go
// Transaction represents a Firestore transaction.
type transaction struct {
	c              *client
	ctx            context.Context
	id             []byte
	writes         []*pb.Write
	maxAttempts    int
	readOnly       bool
	readAfterWrite error
}

var (
	// Defined here for testing.
	errWriteReadOnly     = xerrors.New("firestore: write in read-only transaction")
	errNestedTransaction = xerrors.New("firestore: nested transaction")
)

type currentTransactionKey struct{}

func currentTransactionForRead(c context.Context, documents []string, collectionIDs []string) ([]byte, error) {
	if tx, ok := c.Value(currentTransactionKey{}).(*transaction); ok {
		if len(tx.writes) > 0 {
			for _, write := range tx.writes {
				var document string
				switch op := write.Operation.(type) {
				case *pb.Write_Update:
					document = op.Update.Name
				case *pb.Write_Delete:
					document = op.Delete
				case *pb.Write_Transform:
					document = op.Transform.Document
				}
				for _, read := range documents {
					if read == document {
						tx.readAfterWrite = xerrors.Errorf("firestore: %v: read after write in transaction", document)
						return nil, tx.readAfterWrite
					}
				}
				for _, col := range collectionIDs {
					if strings.Contains(document, "/"+col+"/") {
						tx.readAfterWrite = xerrors.Errorf("firestore: %v: read after write in transaction", document)
						return nil, tx.readAfterWrite
					}
				}
			}
		}
		return tx.id, nil
	}
	return nil, nil
}

func currentTransactionForWrite(c context.Context) (*transaction, error) {
	if tx, ok := c.Value(currentTransactionKey{}).(*transaction); ok {
		if tx.readOnly {
			return nil, errWriteReadOnly
		}
		return tx, nil
	}
	return nil, nil
}

// ErrConcurrentTransaction is returned when a transaction is rolled back due
// to a conflict with a concurrent transaction.
var ErrConcurrentTransaction = xerrors.New("datastore: concurrent transaction")

// RunInTransaction runs f in a transaction. It calls f with a transaction
// context tc that f should use for all App Engine operations.
//
// If f returns nil, RunInTransaction attempts to commit the transaction,
// returning nil if it succeeds. If the commit fails due to a conflicting
// transaction, RunInTransaction retries f, each time with a new transaction
// context. It gives up and returns ErrConcurrentTransaction after three
// failed attempts. The number of attempts can be configured by specifying
// TransactionOptions.Attempts.
//
// If f returns non-nil, then any datastore changes will not be applied and
// RunInTransaction returns that same error. The function f is not retried.
//
// Note that when f returns, the transaction is not yet committed. Calling code
// must be careful not to assume that any of f's changes have been committed
// until RunInTransaction returns nil.
//
// Since f may be called multiple times, f should usually be idempotent.
// datastore.Get is not idempotent when unmarshaling slice fields.
//
// Nested transactions are not supported; c may not be a transaction context.
func RunInTransaction(c context.Context, f func(tc context.Context) error, opts *TransactionOptions) (err error) {
	client := getClient()
	c = client.startSpan(c, "fsdatastore.RunInTransaction")
	defer func() { client.endSpan(c, err) }()

	if c.Value(currentTransactionKey{}) != nil {
		return errNestedTransaction
	}
	readOnly := false
	if opts != nil {
		readOnly = opts.ReadOnly
	}
	attempts := 3
	if opts != nil && opts.Attempts > 0 {
		attempts = opts.Attempts
	}
	t := &transaction{
		c:           client,
		ctx:         c,
		maxAttempts: attempts,
		readOnly:    readOnly,
	}
	var txOpts *pb.TransactionOptions
	if t.readOnly {
		txOpts = &pb.TransactionOptions{
			Mode: &pb.TransactionOptions_ReadOnly_{&pb.TransactionOptions_ReadOnly{}},
		}
	}
	var backoff gax.Backoff
	// TODO(jba): use other than the standard backoff parameters?
	// TODO(jba): get backoff time from gRPC trailer metadata? See
	// extractRetryDelay in https://code.googlesource.com/gocloud/+/master/spanner/retry.go.
	for i := 0; i < t.maxAttempts; i++ {
		var res *pb.BeginTransactionResponse
		res, err = t.c.c.BeginTransaction(t.ctx, &pb.BeginTransactionRequest{
			Database: client.databaseID,
			Options:  txOpts,
		})
		if err != nil {
			return err
		}
		t.id = res.Transaction
		t.writes = nil
		t.readAfterWrite = nil
		err = f(context.WithValue(c, currentTransactionKey{}, t))
		// Read after write can only be checked client-side, so we make sure to check
		// even if the user does not.
		if err == nil && t.readAfterWrite != nil {
			err = t.readAfterWrite
		}
		if err != nil {
			t.rollback()
			// Prefer f's returned error to rollback error.
			return err
		}
		_, err = t.c.c.Commit(t.ctx, &pb.CommitRequest{
			Database:    t.c.databaseID,
			Writes:      t.writes,
			Transaction: t.id,
		})
		// If a read-write transaction returns Aborted, retry.
		// On success or other failures, return here.
		if t.readOnly || status.Code(err) != codes.Aborted {
			// According to the Firestore team, we should not roll back here
			// if err != nil. But spanner does.
			// See https://code.googlesource.com/gocloud/+/master/spanner/transaction.go#740.
			return err
		}

		if txOpts == nil {
			// txOpts can only be nil if is the first retry of a read-write transaction.
			// (It is only set here and in the body of "if t.readOnly" above.)
			// Mention the transaction ID in BeginTransaction so the service
			// knows it is a retry.
			txOpts = &pb.TransactionOptions{
				Mode: &pb.TransactionOptions_ReadWrite_{
					&pb.TransactionOptions_ReadWrite{RetryTransaction: t.id},
				},
			}
		}
		// Use exponential backoff to avoid contention with other running
		// transactions.
		if cerr := sleep(c, backoff.Pause()); cerr != nil {
			err = cerr
			break
		}

		// Reset state for the next attempt.
		t.writes = nil
	}
	// If we run out of retries, return the last error we saw (which should
	// be the Aborted from Commit, or a context error).
	if err != nil {
		t.rollback()
	}
	if status.Code(err) == codes.Aborted {
		err = ErrConcurrentTransaction
	}
	return err
}

func (t *transaction) rollback() {
	_ = t.c.c.Rollback(t.ctx, &pb.RollbackRequest{
		Database:    t.c.databaseID,
		Transaction: t.id,
	})
	// Ignore the rollback error.
	// TODO(jba): Log it?
	// Note: Rollback is idempotent so it will be retried by the gapic layer.
}

// TransactionOptions are the options for running a transaction.
type TransactionOptions struct {
	// XG is ignored for Firestore
	XG bool
	// Attempts controls the number of retries to perform when commits fail
	// due to a conflicting transaction. If omitted, it defaults to 3.
	Attempts int
	// ReadOnly controls whether the transaction is a read only transaction.
	// Read only transactions are potentially more efficient.
	ReadOnly bool
}
