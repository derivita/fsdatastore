// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package datastore

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	dspb "github.com/derivita/fsdatastore/internal"
	"github.com/golang/protobuf/proto"
)

var (
	// ErrInvalidEntityType is returned when functions like Get or Next are
	// passed a dst or src argument of invalid type.
	ErrInvalidEntityType = errors.New("datastore: invalid entity type")
	// ErrInvalidKey is returned when an invalid key is presented.
	ErrInvalidKey = errors.New("datastore: invalid key")
	// ErrNoSuchEntity is returned when no entity was found for a given key.
	ErrNoSuchEntity = errors.New("datastore: no such entity")
)

// ErrFieldMismatch is returned when a field is to be loaded into a different
// type than the one it was stored from, or when a field is missing or
// unexported in the destination struct.
// StructType is the type of the struct pointed to by the destination argument
// passed to Get or to Iterator.Next.
type ErrFieldMismatch struct {
	StructType reflect.Type
	FieldName  string
	Reason     string
}

func (e *ErrFieldMismatch) Error() string {
	return fmt.Sprintf("datastore: cannot load field %q into a %q: %s",
		e.FieldName, e.StructType, e.Reason)
}

// protoToKey converts a Reference proto to a *Key. If the key is invalid,
// protoToKey will return the invalid key along with ErrInvalidKey.
func protoToKey(r *dspb.Reference) (k *Key, err error) {
	appID := r.GetApp()
	namespace := r.GetNameSpace()
	for _, e := range r.Path.Element {
		k = &Key{
			kind:      e.GetType(),
			stringID:  e.GetName(),
			intID:     e.GetId(),
			parent:    k,
			appID:     appID,
			namespace: namespace,
		}
		if !k.valid() {
			return k, ErrInvalidKey
		}
	}
	return
}

// keyToProto converts a *Key to a Reference proto.
func keyToProto(defaultAppID string, k *Key) *dspb.Reference {
	appID := k.appID
	if appID == "" {
		appID = defaultAppID
	}
	n := 0
	for i := k; i != nil; i = i.parent {
		n++
	}
	e := make([]*dspb.Path_Element, n)
	for i := k; i != nil; i = i.parent {
		n--
		e[n] = &dspb.Path_Element{
			Type: &i.kind,
		}
		// At most one of {Name,Id} should be set.
		// Neither will be set for incomplete keys.
		if i.stringID != "" {
			e[n].Name = &i.stringID
		} else if i.intID != 0 {
			e[n].Id = &i.intID
		}
	}
	var namespace *string
	if k.namespace != "" {
		namespace = proto.String(k.namespace)
	}
	return &dspb.Reference{
		App:       proto.String(appID),
		NameSpace: namespace,
		Path: &dspb.Path{
			Element: e,
		},
	}
}

// keyToReferenceValue converts a Key to a firestore style reference string:
// projects/{project_id}/databases/(default)/documents/{document_path}
// Integer ids are simply converted to strings
func keyToReferenceValue(defaultAppID string, k *Key) string {
	if k.Incomplete() {
		panic(ErrInvalidKey)
	}
	// Ignore the key's app id, always use the default.
	parts := []string{"projects", defaultAppID, "databases/(default)/documents"}
	var appendKey func(k *Key)
	appendKey = func(k *Key) {
		if k.Parent() != nil {
			appendKey(k.Parent())
		}
		parts = append(parts, k.kind)
		if k.stringID != "" {
			parts = append(parts, k.stringID)
		} else {
			parts = append(parts, strconv.FormatInt(k.intID, 10))
		}
	}
	appendKey(k)
	return strings.Join(parts, "/")
}
