// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package datastore

import (
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/wrappers"

	"golang.org/x/net/context"

	pb "google.golang.org/genproto/googleapis/firestore/v1"
)

type operator int

const (
	lessThan operator = iota
	lessEq
	equal
	greaterEq
	greaterThan
	arrayContains
)

var operatorToProto = map[operator]pb.StructuredQuery_FieldFilter_Operator{
	lessThan:      pb.StructuredQuery_FieldFilter_LESS_THAN,
	lessEq:        pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
	equal:         pb.StructuredQuery_FieldFilter_EQUAL,
	greaterEq:     pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
	greaterThan:   pb.StructuredQuery_FieldFilter_GREATER_THAN,
	arrayContains: pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS,
}

// filter is a conditional filter on query results.
type filter struct {
	FieldName string
	Op        operator
	Value     interface{}
}

type sortDirection int

const (
	ascending sortDirection = iota
	descending
)

var sortDirectionToProto = map[sortDirection]pb.StructuredQuery_Direction{
	ascending:  pb.StructuredQuery_ASCENDING,
	descending: pb.StructuredQuery_DESCENDING,
}

// order is a sort order on query results.
type order struct {
	FieldName string
	Direction sortDirection
}

// NewQuery creates a new Query for a specific entity kind.
//
// An empty kind means to return all entities, including entities created and
// managed by other App Engine features, and is called a kindless query.
// Kindless queries cannot include filters or sort orders on property values.
func NewQuery(kind string) *Query {
	return &Query{
		kind:  kind,
		limit: -1,
	}
}

// Query represents a datastore query.
type Query struct {
	kind       string
	ancestor   *Key
	filter     []filter
	order      []order
	projection []string

	keysOnly bool
	eventual bool
	limit    int32
	offset   int32
	start    *pb.Cursor
	end      *pb.Cursor

	err error
}

func (q *Query) clone() *Query {
	x := *q
	// Copy the contents of the slice-typed fields to a new backing store.
	if len(q.filter) > 0 {
		x.filter = make([]filter, len(q.filter))
		copy(x.filter, q.filter)
	}
	if len(q.order) > 0 {
		x.order = make([]order, len(q.order))
		copy(x.order, q.order)
	}
	return &x
}

// Ancestor returns a derivative query with an ancestor filter.
// The ancestor should not be nil.
func (q *Query) Ancestor(ancestor *Key) *Query {
	q = q.clone()
	if ancestor == nil {
		q.err = errors.New("datastore: nil query ancestor")
		return q
	}
	q.ancestor = ancestor
	return q
}

// EventualConsistency returns a derivative query that returns eventually
// consistent results.
// It only has an effect on ancestor queries.
func (q *Query) EventualConsistency() *Query {
	q = q.clone()
	q.eventual = true
	return q
}

// Filter returns a derivative query with a field-based filter.
// The filterStr argument must be a field name followed by optional space,
// followed by an operator, one of ">", "<", ">=", "<=", or "=".
// Fields are compared against the provided value using the operator.
// Multiple filters are AND'ed together.
func (q *Query) Filter(filterStr string, value interface{}) *Query {
	q = q.clone()
	filterStr = strings.TrimSpace(filterStr)
	if len(filterStr) < 1 {
		q.err = errors.New("datastore: invalid filter: " + filterStr)
		return q
	}

	f := filter{
		FieldName: strings.TrimRight(filterStr, " ><=!"),
		Value:     value,
	}
	if strings.HasSuffix(f.FieldName, " array-contains") {
		f.FieldName = strings.TrimSpace(strings.TrimSuffix(f.FieldName, " array-contains"))
	}
	switch op := strings.TrimSpace(filterStr[len(f.FieldName):]); op {
	case "<=":
		f.Op = lessEq
	case ">=":
		f.Op = greaterEq
	case "<":
		f.Op = lessThan
	case ">":
		f.Op = greaterThan
	case "=":
		f.Op = equal
	case "array-contains":
		f.Op = arrayContains
	default:
		q.err = fmt.Errorf("datastore: invalid operator %q in filter %q", op, filterStr)
		return q
	}
	q.filter = append(q.filter, f)
	return q
}

// Order returns a derivative query with a field-based sort order. Orders are
// applied in the order they are added. The default order is ascending; to sort
// in descending order prefix the fieldName with a minus sign (-).
func (q *Query) Order(fieldName string) *Query {
	q = q.clone()
	fieldName = strings.TrimSpace(fieldName)
	o := order{
		Direction: ascending,
		FieldName: fieldName,
	}
	if strings.HasPrefix(fieldName, "-") {
		o.Direction = descending
		o.FieldName = strings.TrimSpace(fieldName[1:])
	} else if strings.HasPrefix(fieldName, "+") {
		q.err = fmt.Errorf("datastore: invalid order: %q", fieldName)
		return q
	}
	if len(o.FieldName) == 0 {
		q.err = errors.New("datastore: empty order")
		return q
	}
	q.order = append(q.order, o)
	return q
}

// Project returns a derivative query that yields only the given fields. It
// cannot be used with KeysOnly.
func (q *Query) Project(fieldNames ...string) *Query {
	q = q.clone()
	q.projection = append([]string(nil), fieldNames...)
	return q
}

// KeysOnly returns a derivative query that yields only keys, not keys and
// entities. It cannot be used with projection queries.
func (q *Query) KeysOnly() *Query {
	q = q.clone()
	q.keysOnly = true
	return q
}

// Limit returns a derivative query that has a limit on the number of results
// returned. A negative value means unlimited.
func (q *Query) Limit(limit int) *Query {
	q = q.clone()
	if limit < math.MinInt32 || limit > math.MaxInt32 {
		q.err = errors.New("datastore: query limit overflow")
		return q
	}
	q.limit = int32(limit)
	return q
}

// Offset returns a derivative query that has an offset of how many keys to
// skip over before returning results. A negative value is invalid.
func (q *Query) Offset(offset int) *Query {
	q = q.clone()
	if offset < 0 {
		q.err = errors.New("datastore: negative query offset")
		return q
	}
	if offset > math.MaxInt32 {
		q.err = errors.New("datastore: query offset overflow")
		return q
	}
	q.offset = int32(offset)
	return q
}

// toProto converts the query to a protocol buffer.
func (q *Query) toProto(ctx context.Context, req *pb.RunQueryRequest, client *client) error {
	if len(q.projection) != 0 && q.keysOnly {
		return errors.New("datastore: query cannot both project and be keys-only")
	}
	txid, err := currentTransactionForRead(ctx, nil, []string{q.kind})
	if err != nil {
		return err
	}
	req.Reset()
	req.Parent = client.root()
	req.QueryType = &pb.RunQueryRequest_StructuredQuery{&pb.StructuredQuery{}}
	if q.ancestor != nil {
		req.Parent = keyToReferenceValue(client.projectID, q.ancestor)
	}
	if txid != nil {
		req.ConsistencySelector = &pb.RunQueryRequest_Transaction{txid}
	}

	dst := req.GetStructuredQuery()
	dst.From = []*pb.StructuredQuery_CollectionSelector{{
		CollectionId:   q.kind,
		AllDescendants: true,
	}}
	if q.projection != nil {
		fields := make([]*pb.StructuredQuery_FieldReference, len(q.projection))
		for i := range fields {
			fields[i] = &pb.StructuredQuery_FieldReference{FieldPath: q.projection[i]}
		}
		dst.Select = &pb.StructuredQuery_Projection{Fields: fields}
	}
	if q.keysOnly {
		dst.Select = &pb.StructuredQuery_Projection{
			Fields: []*pb.StructuredQuery_FieldReference{{FieldPath: "__name__"}},
		}
	}
	var filters []*pb.StructuredQuery_Filter
	for _, qf := range q.filter {
		if qf.FieldName == "" {
			return errors.New("datastore: empty query filter field name")
		}
		p, errStr := valueToProto(client.projectID, reflect.ValueOf(qf.Value))
		if errStr != "" {
			return errors.New("datastore: bad query filter value type: " + errStr)
		}
		xf := &pb.StructuredQuery_FieldFilter{
			Field: &pb.StructuredQuery_FieldReference{FieldPath: qf.FieldName},
			Op:    operatorToProto[qf.Op],
			Value: p,
		}
		if xf.Op == pb.StructuredQuery_FieldFilter_OPERATOR_UNSPECIFIED {
			return errors.New("datastore: unknown query filter operator")
		}
		filters = append(filters, &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_FieldFilter{xf}})
	}
	if len(filters) == 1 {
		dst.Where = filters[0]
	} else if len(filters) > 1 {
		dst.Where = &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_CompositeFilter{
			&pb.StructuredQuery_CompositeFilter{
				Op:      pb.StructuredQuery_CompositeFilter_AND,
				Filters: filters,
			},
		},
		}
	}
	for _, qo := range q.order {
		if qo.FieldName == "" {
			return errors.New("datastore: empty query order field name")
		}
		xo := &pb.StructuredQuery_Order{
			Field:     &pb.StructuredQuery_FieldReference{FieldPath: qo.FieldName},
			Direction: sortDirectionToProto[qo.Direction],
		}
		if xo.Direction == pb.StructuredQuery_DIRECTION_UNSPECIFIED {
			return errors.New("datastore: unknown query order direction")
		}
		dst.OrderBy = append(dst.OrderBy, xo)
	}
	if q.limit >= 0 {
		dst.Limit = &wrappers.Int32Value{Value: q.limit}
	}
	if q.offset != 0 {
		dst.Offset = q.offset
	}

	dst.StartAt = q.start
	dst.EndAt = q.end
	return nil
}

// Count returns the number of results for the query.
//
// The running time and number of API calls made by Count scale linearly with
// the sum of the query's offset and limit. Unless the result count is
// expected to be small, it is best to specify a limit; otherwise Count will
// continue until it finishes counting or the provided context expires.
func (q *Query) Count(c context.Context) (int, error) {
	// Check that the query is well-formed.
	if q.err != nil {
		return 0, q.err
	}

	// Run a copy of the query, with keysOnly true (if we're not a projection,
	// since the two are incompatible), and an adjusted offset. We also set the
	// limit to zero, as we don't want any actual entity data, just the number
	// of skipped results.
	newQ := q.clone()
	newQ.keysOnly = len(newQ.projection) == 0
	newQ.limit = 0
	if q.limit < 0 {
		// If the original query was unlimited, set the new query's offset to maximum.
		newQ.offset = math.MaxInt32
	} else {
		newQ.offset = q.offset + q.limit
		if newQ.offset < 0 {
			// Do the best we can, in the presence of overflow.
			newQ.offset = math.MaxInt32
		}
	}
	client := getClient()
	req := &pb.RunQueryRequest{}
	if err := newQ.toProto(c, req, client); err != nil {
		return 0, err
	}
	stream, err := client.c.RunQuery(c, req)
	if err != nil {
		return 0, err
	}

	// n is the count we will return. For example, suppose that our original
	// query had an offset of 4 and a limit of 2008: the count will be 2008,
	// provided that there are at least 2012 matching entities. However, the
	// RPCs will only skip 1000 results at a time. The RPC sequence is:
	//   call RunQuery with (offset, limit) = (2012, 0)  // 2012 == newQ.offset
	//   response has (skippedResults, moreResults) = (1000, true)
	//   n += 1000  // n == 1000
	//   call Next     with (offset, limit) = (1012, 0)  // 1012 == newQ.offset - n
	//   response has (skippedResults, moreResults) = (1000, true)
	//   n += 1000  // n == 2000
	//   call Next     with (offset, limit) = (12, 0)    // 12 == newQ.offset - n
	//   response has (skippedResults, moreResults) = (12, false)
	//   n += 12    // n == 2012
	//   // exit the loop
	//   n -= 4     // n == 2008
	var n int32
	for res, err := stream.Recv(); err != io.EOF; res, err = stream.Recv() {
		if err != nil {
			return 0, err
		}
		// The QueryResult should have no actual entity data, just skipped results.
		if res.Document != nil {
			return 0, errors.New("datastore: internal error: Count request returned too much data")
		}
		n += res.GetSkippedResults()
	}
	n -= q.offset
	if n < 0 {
		// If the offset was greater than the number of matching entities,
		// return 0 instead of negative.
		n = 0
	}
	return int(n), nil
}

// callNext issues a datastore_v3/Next RPC to advance a cursor, such as that
// returned by a query with more results.
func (t *Iterator) callNext() error {
	t.res.Reset()
	res, err := t.stream.Recv()
	if err == io.EOF {
		err = Done
	} else if err == nil {
		proto.Merge(&t.res, res)
	}
	t.err = err
	return err
}

// GetAll runs the query in the given context and returns all keys that match
// that query, as well as appending the values to dst.
//
// dst must have type *[]S or *[]*S or *[]P, for some struct type S or some non-
// interface, non-pointer type P such that P or *P implements PropertyLoadSaver.
//
// As a special case, *PropertyList is an invalid type for dst, even though a
// PropertyList is a slice of structs. It is treated as invalid to avoid being
// mistakenly passed when *[]PropertyList was intended.
//
// The keys returned by GetAll will be in a 1-1 correspondence with the entities
// added to dst.
//
// If q is a ``keys-only'' query, GetAll ignores dst and only returns the keys.
//
// The running time and number of API calls made by GetAll scale linearly with
// the sum of the query's offset and limit. Unless the result count is
// expected to be small, it is best to specify a limit; otherwise GetAll will
// continue until it finishes collecting results or the provided context
// expires.
func (q *Query) GetAll(c context.Context, dst interface{}) ([]*Key, error) {
	var (
		dv               reflect.Value
		mat              multiArgType
		elemType         reflect.Type
		errFieldMismatch error
	)
	if !q.keysOnly {
		dv = reflect.ValueOf(dst)
		if dv.Kind() != reflect.Ptr || dv.IsNil() {
			return nil, ErrInvalidEntityType
		}
		dv = dv.Elem()
		mat, elemType = checkMultiArg(dv)
		if mat == multiArgTypeInvalid || mat == multiArgTypeInterface {
			return nil, ErrInvalidEntityType
		}
	}

	var keys []*Key
	for t := q.Run(c); ; {
		k, e, err := t.next()
		if err == Done {
			break
		}
		if err != nil {
			return keys, err
		}
		if !q.keysOnly {
			ev := reflect.New(elemType)
			if elemType.Kind() == reflect.Map {
				// This is a special case. The zero values of a map type are
				// not immediately useful; they have to be make'd.
				//
				// Funcs and channels are similar, in that a zero value is not useful,
				// but even a freshly make'd channel isn't useful: there's no fixed
				// channel buffer size that is always going to be large enough, and
				// there's no goroutine to drain the other end. Theoretically, these
				// types could be supported, for example by sniffing for a constructor
				// method or requiring prior registration, but for now it's not a
				// frequent enough concern to be worth it. Programmers can work around
				// it by explicitly using Iterator.Next instead of the Query.GetAll
				// convenience method.
				x := reflect.MakeMap(elemType)
				ev.Elem().Set(x)
			}
			if err = loadEntity(ev.Interface(), e); err != nil {
				if _, ok := err.(*ErrFieldMismatch); ok {
					// We continue loading entities even in the face of field mismatch errors.
					// If we encounter any other error, that other error is returned. Otherwise,
					// an ErrFieldMismatch is returned.
					errFieldMismatch = err
				} else {
					return keys, err
				}
			}
			if mat != multiArgTypeStructPtr {
				ev = ev.Elem()
			}
			dv.Set(reflect.Append(dv, ev))
		}
		keys = append(keys, k)
	}
	return keys, errFieldMismatch
}

// Run runs the query in the given context.
func (q *Query) Run(c context.Context) *Iterator {
	if q.err != nil {
		return &Iterator{err: q.err}
	}
	t := &Iterator{
		limit: q.limit,
		q:     q,
	}
	client := getClient()
	var req pb.RunQueryRequest
	if err := q.toProto(c, &req, client); err != nil {
		t.err = err
		return t
	}
	t.stream, t.err = client.c.RunQuery(c, &req)
	if t.err != nil {
		return t
	}
	offset := q.offset - t.res.GetSkippedResults()

	for offset > 0 && t.err == nil {
		if err := t.callNext(); err != nil {
			break
		}
		skip := t.res.GetSkippedResults()
		if skip < 0 {
			t.err = errors.New("datastore: internal error: negative number of skipped_results")
			break
		}
		offset -= skip
	}
	if offset < 0 {
		t.err = errors.New("datastore: internal error: query offset was overshot")
	}
	return t
}

// Iterator is the result of running a query.
type Iterator struct {
	err error
	// res is the result of the most recent RunQuery or Next API call.
	res pb.RunQueryResponse
	// limit is the limit on the number of results this iterator should return.
	// A negative value means unlimited.
	limit int32
	// q is the original query which yielded this iterator.
	q      *Query
	stream pb.Firestore_RunQueryClient
}

// Done is returned when a query iteration has completed.
var Done = errors.New("datastore: query has no more results")

// Next returns the key of the next result. When there are no more results,
// Done is returned as the error.
//
// If the query is not keys only and dst is non-nil, it also loads the entity
// stored for that key into the struct pointer or PropertyLoadSaver dst, with
// the same semantics and possible errors as for the Get function.
func (t *Iterator) Next(dst interface{}) (*Key, error) {
	k, e, err := t.next()
	if err != nil {
		return nil, err
	}
	if dst != nil && !t.q.keysOnly {
		err = loadEntity(dst, e)
	}
	return k, err
}

func (t *Iterator) next() (*Key, *pb.Document, error) {
	if t.err != nil {
		return nil, nil, t.err
	}

	// Read from the stream as necessary.
	for haveDoc := false; !haveDoc; {
		if t.callNext() != nil {
			return nil, nil, t.err
		}
		if t.res.GetSkippedResults() != 0 {
			t.err = errors.New("datastore: internal error: iterator has skipped results")
			return nil, nil, t.err
		}
		haveDoc = t.res.Document != nil
		if t.limit >= 0 && haveDoc {
			t.limit -= 1
			if t.limit < 0 {
				t.err = errors.New("datastore: internal error: query returned more results than the limit")
				return nil, nil, t.err
			}
		}
	}

	// Extract the key from t.res.Document.
	e := t.res.Document
	if e.Name == "" {
		return nil, nil, errors.New("datastore: internal error: server did not return a key")
	}
	k, err := referenceValueToKey(e.Name)
	if err != nil || k.Incomplete() {
		return nil, nil, errors.New("datastore: internal error: server returned an invalid key")
	}
	return k, e, nil
}
