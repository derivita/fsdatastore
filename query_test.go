package datastore

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	pb "google.golang.org/genproto/googleapis/firestore/v1"
)

func TestArrayContainsFilter(t *testing.T) {
	q := NewQuery("User").Filter("foo array-contains", 5)

	ctx := context.Background()
	var req pb.RunQueryRequest
	client := client{"test-project", "projects/test-project/databases/(default)", nil}
	err := q.toProto(ctx, &req, &client)
	if err != nil {
		t.Errorf("toProto failed: %v", err)
	}
	t.Logf(proto.MarshalTextString(&req))
	filter := req.GetStructuredQuery().Where.GetFieldFilter()
	if filter.Op != pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS {
		t.Errorf("Query protobuf filter operator was incorrect, got: %d, want: %d.", filter.Op, pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS)
	}
	if filter.Field.FieldPath != "foo" {
		t.Errorf("Query protobuf filter field path was incorrect, got: %s, want: %s.", filter.Field.FieldPath, "foo")
	}
}
