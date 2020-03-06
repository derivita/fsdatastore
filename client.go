// Based on https://github.com/googleapis/google-cloud-go/blob/4289a8c8e48e520ef69ddacec47283433eb7db30/firestore/client.go
package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	vkit "cloud.google.com/go/firestore/apiv1"
	gax "github.com/googleapis/gax-go/v2"
	"go.opencensus.io/trace"
	"google.golang.org/api/option"
	pb "google.golang.org/genproto/googleapis/firestore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var globalClient *client

type client struct {
	projectID  string
	databaseID string
	c          *vkit.Client
}

type logEntry struct {
	Severity string `json:"severity,omitempty"`
	TraceID  string `json:"logging.googleapis.com/trace"`
	SpanID   string `json:"logging.googleapis.com/spanId"`
	Message  string `json:"message"`
}

func Init(projectID string) error {
	databaseID := fmt.Sprintf("projects/%s/databases/(default)", projectID)
	var c *vkit.Client
	var err error
	if c, err = newClient(context.Background()); err != nil {
		return err
	}
	globalClient = &client{projectID, databaseID, c}
	return nil
}

func getClient() *client {
	if globalClient == nil {
		if err := Init(os.Getenv("GOOGLE_CLOUD_PROJECT")); err != nil {
			panic(err)
		}
	}
	return globalClient
}

func (c *client) root() string {
	return c.databaseID + "/documents"
}

func (c *client) startSpan(ctx context.Context, name string) context.Context {
	ctx, _ = trace.StartSpan(ctx, name)
	return ctx
}

func (c *client) endSpan(ctx context.Context, err error) {
	span := trace.FromContext(ctx)
	if err != nil {
		s := status.Convert(err)
		span.SetStatus(trace.Status{Code: int32(s.Code()), Message: s.Message()})
	}
}

func (c *client) log(ctx context.Context, severity, message string) {
	span := trace.FromContext(ctx).SpanContext()
	log := logEntry{
		TraceID:  fmt.Sprintf("projects/%s/traces/%s", c.projectID, span.TraceID.String()),
		SpanID:   span.SpanID.String(),
		Severity: severity,
		Message:  message,
	}
	json.NewEncoder(os.Stderr).Encode(log)
}

func newClient(ctx context.Context, opts ...option.ClientOption) (*vkit.Client, error) {
	var o []option.ClientOption
	// If this environment variable is defined, configure the client to talk to the emulator.
	if addr := os.Getenv("FIRESTORE_EMULATOR_HOST"); addr != "" {
		conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithPerRPCCredentials(emulatorCreds{}))
		if err != nil {
			return nil, fmt.Errorf("firestore: dialing address from env var FIRESTORE_EMULATOR_HOST: %s", err)
		}
		o = []option.ClientOption{option.WithGRPCConn(conn)}
	}
	o = append(o, opts...)

	return vkit.NewClient(ctx, o...)
}

func (c *client) getAll(ctx context.Context, docNames []string, tid []byte) ([]*pb.Document, error) {
	docIndices := map[string][]int{} // doc name to positions in docRefs
	for i, dr := range docNames {
		docIndices[dr] = append(docIndices[dr], i)
	}
	req := &pb.BatchGetDocumentsRequest{
		Database:  c.databaseID,
		Documents: docNames,
	}
	if tid != nil {
		req.ConsistencySelector = &pb.BatchGetDocumentsRequest_Transaction{tid}
	}
	streamClient, err := c.c.BatchGetDocuments(ctx, req)
	if err != nil {
		return nil, err
	}

	// Read and remember all results from the stream.
	var resps []*pb.BatchGetDocumentsResponse
	for {
		resp, err := streamClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		resps = append(resps, resp)
	}

	// Results may arrive out of order. Put each at the right indices.
	docs := make([]*pb.Document, len(docNames))
	for _, resp := range resps {
		var (
			indices []int
			doc     *pb.Document
		)
		switch r := resp.Result.(type) {
		case *pb.BatchGetDocumentsResponse_Found:
			indices = docIndices[r.Found.Name]
			doc = r.Found
		case *pb.BatchGetDocumentsResponse_Missing:
			indices = docIndices[r.Missing]
			doc = nil
		default:
			return nil, errors.New("firestore: unknown BatchGetDocumentsResponse result type")
		}
		for _, index := range indices {
			if docs[index] != nil {
				return nil, fmt.Errorf("firestore: %q seen twice", docNames[index])
			}
			docs[index] = doc
		}
	}
	return docs, nil
}

// commit calls the Commit RPC outside of a transaction, or appends the writes to the current transaction.
func (c *client) commit(ctx context.Context, ws []*pb.Write) ([]*pb.WriteResult, error) {
	tx, err := currentTransactionForWrite(ctx)
	if err != nil {
		return nil, err
	}
	if tx != nil {
		tx.writes = append(tx.writes, ws...)
		return nil, nil
	}
	req := &pb.CommitRequest{
		Database: c.databaseID,
		Writes:   ws,
	}
	res, err := c.c.Commit(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(res.WriteResults) == 0 {
		return nil, errors.New("firestore: missing WriteResult")
	}
	return res.WriteResults, nil
}

// emulatorCreds is an instance of grpc.PerRPCCredentials that will configure a
// client to act as an admin for the Firestore emulator. It always hardcodes
// the "authorization" metadata field to contain "Bearer owner", which the
// Firestore emulator accepts as valid admin credentials.
type emulatorCreds struct{}

func (ec emulatorCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer owner"}, nil
}
func (ec emulatorCreds) RequireTransportSecurity() bool {
	return false
}

func sleep(ctx context.Context, dur time.Duration) error {
	switch err := gax.Sleep(ctx, dur); err {
	case context.Canceled:
		return status.Error(codes.Canceled, "context canceled")
	case context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	default:
		return err
	}
}
