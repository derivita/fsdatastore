// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package datastore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	dspb "github.com/derivita/fsdatastore/internal"
	"github.com/golang/protobuf/proto"
	"golang.org/x/xerrors"
	"google.golang.org/appengine"
	pb "google.golang.org/genproto/googleapis/firestore/v1"
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

// multiKeyToProto is a batch version of keyToReferenceValue.
func multiKeyToProto(appID string, key []*Key) (ret []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = ErrInvalidKey
		}
	}()
	ret = make([]string, len(key))
	for i, k := range key {
		ret[i] = keyToReferenceValue(appID, k)
	}
	return ret, nil
}

// multiValid is a batch version of Key.valid. It returns an error, not a
// []bool.
func multiValid(key []*Key) error {
	invalid := false
	for _, k := range key {
		if !k.valid() {
			invalid = true
			break
		}
	}
	if !invalid {
		return nil
	}
	err := make(appengine.MultiError, len(key))
	for i, k := range key {
		if !k.valid() {
			err[i] = ErrInvalidKey
		}
	}
	return err
}

// referenceValueToKey converts a firestore Document name to a Key.
func referenceValueToKey(r string) (k *Key, err error) {
	appID, _, elems, err := parseDocumentPath(r)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(elems); i += 2 {
		k = &Key{
			kind:     elems[i],
			stringID: elems[i+1],
			parent:   k,
			appID:    appID,
		}
		if !k.valid() {
			return nil, ErrInvalidKey
		}
	}
	return
}

// keyToReferenceValue converts a Key to a firestore style reference string:
// projects/{project_id}/databases/(default)/documents/{document_path}
// Integer ids are simply converted to strings
func keyToReferenceValue(defaultAppID string, k *Key) string {
	if k.Incomplete() {
		panic(ErrInvalidKey)
	}
	appID := k.AppID()
	if appID == "" {
		appID = defaultAppID
	}
	parts := []string{"projects", appID, "databases/(default)/documents"}
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

type multiArgType int

const (
	multiArgTypeInvalid multiArgType = iota
	multiArgTypePropertyLoadSaver
	multiArgTypeStruct
	multiArgTypeStructPtr
	multiArgTypeInterface
)

// checkMultiArg checks that v has type []S, []*S, []I, or []P, for some struct
// type S, for some interface type I, or some non-interface non-pointer type P
// such that P or *P implements PropertyLoadSaver.
//
// It returns what category the slice's elements are, and the reflect.Type
// that represents S, I or P.
//
// As a special case, PropertyList is an invalid type for v.
func checkMultiArg(v reflect.Value) (m multiArgType, elemType reflect.Type) {
	if v.Kind() != reflect.Slice {
		return multiArgTypeInvalid, nil
	}
	if v.Type() == typeOfPropertyList {
		return multiArgTypeInvalid, nil
	}
	elemType = v.Type().Elem()
	if reflect.PtrTo(elemType).Implements(typeOfPropertyLoadSaver) {
		return multiArgTypePropertyLoadSaver, elemType
	}
	switch elemType.Kind() {
	case reflect.Struct:
		return multiArgTypeStruct, elemType
	case reflect.Interface:
		return multiArgTypeInterface, elemType
	case reflect.Ptr:
		elemType = elemType.Elem()
		if elemType.Kind() == reflect.Struct {
			return multiArgTypeStructPtr, elemType
		}
	}
	return multiArgTypeInvalid, nil
}

// Get loads the entity stored for k into dst, which must be a struct pointer
// or implement PropertyLoadSaver. If there is no such entity for the key, Get
// returns ErrNoSuchEntity.
//
// The values of dst's unmatched struct fields are not modified, and matching
// slice-typed fields are not reset before appending to them. In particular, it
// is recommended to pass a pointer to a zero valued struct on each Get call.
//
// ErrFieldMismatch is returned when a field is to be loaded into a different
// type than the one it was stored from, or when a field is missing or
// unexported in the destination struct. ErrFieldMismatch is only returned if
// dst is a struct pointer.
func Get(c context.Context, key *Key, dst interface{}) error {
	if dst == nil { // GetMulti catches nil interface; we need to catch nil ptr here
		return ErrInvalidEntityType
	}
	err := GetMulti(c, []*Key{key}, []interface{}{dst})
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

// GetMulti is a batch version of Get.
//
// dst must be a []S, []*S, []I or []P, for some struct type S, some interface
// type I, or some non-interface non-pointer type P such that P or *P
// implements PropertyLoadSaver. If an []I, each element must be a valid dst
// for Get: it must be a struct pointer or implement PropertyLoadSaver.
//
// As a special case, PropertyList is an invalid type for dst, even though a
// PropertyList is a slice of structs. It is treated as invalid to avoid being
// mistakenly passed when []PropertyList was intended.
func GetMulti(c context.Context, key []*Key, dst interface{}) (err error) {
	client := getClient()
	c = client.startSpan(c, "fsdatastore.GetMulti")
	defer func() { client.endSpan(c, err) }()

	v := reflect.ValueOf(dst)
	multiArgType, _ := checkMultiArg(v)
	if multiArgType == multiArgTypeInvalid {
		return xerrors.New("datastore: dst has invalid type")
	}
	if len(key) != v.Len() {
		return xerrors.New("datastore: key and dst slices have different length")
	}
	if len(key) == 0 {
		return nil
	}
	if err := multiValid(key); err != nil {
		return xerrors.Errorf("datastore: %w", err)
	}
	keys, err := multiKeyToProto(client.projectID, key)
	if err != nil {
		return xerrors.Errorf("datastore: %w", err)
	}

	txid, err := currentTransactionForRead(c, keys, nil)
	if err != nil {
		return xerrors.Errorf("datastore: %w", err)
	}

	res, err := client.getAll(c, keys, txid)
	if err != nil {
		return xerrors.Errorf("datastore: %w", err)
	}
	if len(key) != len(res) {
		return xerrors.New("datastore: internal error: server returned the wrong number of entities")
	}
	multiErr, any := make(appengine.MultiError, len(key)), false
	for i, e := range res {
		if e == nil {
			multiErr[i] = ErrNoSuchEntity
		} else {
			elem := v.Index(i)
			if multiArgType == multiArgTypePropertyLoadSaver || multiArgType == multiArgTypeStruct {
				elem = elem.Addr()
			}
			if multiArgType == multiArgTypeStructPtr && elem.IsNil() {
				elem.Set(reflect.New(elem.Type().Elem()))
			}
			multiErr[i] = loadEntity(elem.Interface(), e)
		}
		if multiErr[i] != nil {
			any = true
		}
	}
	if any {
		return multiErr
	}
	return nil
}

// Put saves the entity src into the datastore with key k. src must be a struct
// pointer or implement PropertyLoadSaver; if a struct pointer then any
// unexported fields of that struct will be skipped. If k is an incomplete key,
// the returned key will be a unique key generated by the datastore.
func Put(c context.Context, key *Key, src interface{}) (*Key, error) {
	k, err := PutMulti(c, []*Key{key}, []interface{}{src})
	if err != nil {
		if me, ok := err.(appengine.MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return k[0], nil
}

// PutMulti is a batch version of Put.
//
// src must satisfy the same conditions as the dst argument to GetMulti.
func PutMulti(c context.Context, key []*Key, src interface{}) ([]*Key, error) {
	v := reflect.ValueOf(src)
	multiArgType, _ := checkMultiArg(v)
	if multiArgType == multiArgTypeInvalid {
		return nil, errors.New("datastore: src has invalid type")
	}
	if len(key) != v.Len() {
		return nil, errors.New("datastore: key and src slices have different length")
	}
	if len(key) == 0 {
		return nil, nil
	}
	client := getClient()
	appID := client.projectID
	if err := multiValid(key); err != nil {
		return nil, err
	}
	var writes []*pb.Write
	for i := range key {
		elem := v.Index(i)
		if multiArgType == multiArgTypePropertyLoadSaver || multiArgType == multiArgTypeStruct {
			elem = elem.Addr()
		}
		sProto, err := saveEntity(c, appID, key[i], elem.Interface())
		if err != nil {
			return nil, err
		}
		writes = append(writes, sProto)
	}

	_, err := client.commit(c, writes)
	if err != nil {
		return nil, err
	}
	ret := make([]*Key, len(key))
	for i := range ret {
		var err error
		docRef := writes[i].GetUpdate().Name
		ret[i], err = referenceValueToKey(docRef)
		if err != nil || ret[i].Incomplete() {
			return nil, errors.New("datastore: internal error: server returned an invalid key")
		}
	}
	return ret, nil
}

// Delete deletes the entity for the given key.
func Delete(c context.Context, key *Key) error {
	err := DeleteMulti(c, []*Key{key})
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

// DeleteMulti is a batch version of Delete.
func DeleteMulti(c context.Context, key []*Key) error {
	if len(key) == 0 {
		return nil
	}
	if err := multiValid(key); err != nil {
		return err
	}
	client := getClient()
	docNames, err := multiKeyToProto(client.projectID, key)
	if err != nil {
		return err
	}
	writes := make([]*pb.Write, len(docNames))
	for i := range writes {
		writes[i] = &pb.Write{
			Operation: &pb.Write_Delete{docNames[i]},
		}
	}
	_, err = client.commit(c, writes)
	return err
}

// from https://github.com/googleapis/google-cloud-go/blob/master/firestore/from_value.go
// A document path should be of the form "projects/P/databases/D/documents/coll1/doc1/coll2/doc2/...".
func parseDocumentPath(path string) (projectID, databaseID string, docPath []string, err error) {
	parts := strings.Split(path, "/")
	if len(parts) < 6 || parts[0] != "projects" || parts[2] != "databases" || parts[4] != "documents" {
		return "", "", nil, fmt.Errorf("firestore: malformed document path %q", path)
	}
	docp := parts[5:]
	if len(docp)%2 != 0 {
		return "", "", nil, fmt.Errorf("firestore: path %q refers to collection, not document", path)
	}
	return parts[1], parts[3], docp, nil
}
