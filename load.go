// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package datastore

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"google.golang.org/appengine"
	pb "google.golang.org/genproto/googleapis/firestore/v1"
)

var (
	typeOfBlobKey    = reflect.TypeOf(appengine.BlobKey(""))
	typeOfByteSlice  = reflect.TypeOf([]byte(nil))
	typeOfByteString = reflect.TypeOf(ByteString(nil))
	typeOfGeoPoint   = reflect.TypeOf(appengine.GeoPoint{})
	typeOfTime       = reflect.TypeOf(time.Time{})
	typeOfKeyPtr     = reflect.TypeOf(&Key{})
	typeOfEntityPtr  = reflect.TypeOf(&Entity{})
	typeOfString     = reflect.TypeOf("")
)

// typeMismatchReason returns a string explaining why the property p could not
// be stored in an entity field of type v.Type().
func typeMismatchReason(pValue interface{}, v reflect.Value) string {
	entityType := "empty"
	switch pValue.(type) {
	case int64:
		entityType = "int"
	case bool:
		entityType = "bool"
	case string:
		entityType = "string"
	case float64:
		entityType = "float"
	case *Key:
		entityType = "*datastore.Key"
	case time.Time:
		entityType = "time.Time"
	case appengine.BlobKey:
		entityType = "appengine.BlobKey"
	case appengine.GeoPoint:
		entityType = "appengine.GeoPoint"
	case ByteString:
		entityType = "datastore.ByteString"
	case []byte:
		entityType = "[]byte"
	}
	return fmt.Sprintf("type mismatch: %s versus %v", entityType, v.Type())
}

type propertyLoader struct {
	// m holds the number of times a substruct field like "Foo.Bar.Baz" has
	// been seen so far. The map is constructed lazily.
	m map[string]int
}

func (l *propertyLoader) load(codec *structCodec, structValue reflect.Value, p Property, requireSlice bool) string {
	var v reflect.Value
	var sliceIndex int

	name := p.Name

	// If name ends with a '.', the last field is anonymous.
	// In this case, strings.Split will give us "" as the
	// last element of our fields slice, which will match the ""
	// field name in the substruct codec.
	fields := strings.Split(name, ".")

	for len(fields) > 0 {
		var decoder fieldCodec
		var ok bool

		// Cut off the last field (delimited by ".") and find its parent
		// in the codec.
		// eg. for name "A.B.C.D", split off "A.B.C" and try to
		// find a field in the codec with this name.
		// Loop again with "A.B", etc.
		for i := len(fields); i > 0; i-- {
			parent := strings.Join(fields[:i], ".")
			decoder, ok = codec.fields[parent]
			if ok {
				fields = fields[i:]
				break
			}
		}

		// If we never found a matching field in the codec, return
		// error message.
		if !ok {
			return "no such struct field"
		}

		v = initField(structValue, decoder.path)
		if !v.IsValid() {
			return "no such struct field"
		}
		if !v.CanSet() {
			return "cannot set struct field"
		}

		if decoder.structCodec != nil {
			codec = decoder.structCodec
			structValue = v
		}

		if v.Kind() == reflect.Slice && v.Type() != typeOfByteSlice {
			if l.m == nil {
				l.m = make(map[string]int)
			}
			sliceIndex = l.m[p.Name]
			l.m[p.Name] = sliceIndex + 1
			for v.Len() <= sliceIndex {
				v.Set(reflect.Append(v, reflect.New(v.Type().Elem()).Elem()))
			}
			structValue = v.Index(sliceIndex)
			requireSlice = false
		}
	}

	var slice reflect.Value
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() != reflect.Uint8 {
		slice = v
		v = reflect.New(v.Type().Elem()).Elem()
	} else if requireSlice {
		return "multiple-valued property requires a slice field type"
	}

	if errReason := setVal(v, p.Value); errReason != "" {
		// Set the slice back to its zero value.
		if slice.IsValid() {
			slice.Set(reflect.Zero(slice.Type()))
		}
		return errReason
	}

	if slice.IsValid() {
		slice.Index(sliceIndex).Set(v)
	}

	return ""
}

// setVal sets v to the value pValue.
func setVal(v reflect.Value, pValue interface{}) string {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var x int64
		switch pv := pValue.(type) {
		case int64:
			x = pv
		case float64:
			x = int64(pv)
			if float64(x) != pv {
				return fmt.Sprintf("float %f does not fit into %s", pv, v.Type())
			}
		case nil:
			x = 0
		default:
			return typeMismatchReason(pValue, v)
		}
		if v.OverflowInt(x) {
			return fmt.Sprintf("value %v overflows struct field of type %v", x, v.Type())
		}
		v.SetInt(x)
	case reflect.Bool:
		x, ok := pValue.(bool)
		if !ok && pValue != nil {
			return typeMismatchReason(pValue, v)
		}
		v.SetBool(x)
	case reflect.String:
		switch x := pValue.(type) {
		case []byte:
			v.SetString(string(x))
		case string:
			v.SetString(x)
		default:
			if pValue != nil {
				return typeMismatchReason(pValue, v)
			}
		}
	case reflect.Float32, reflect.Float64:
		var f float64
		switch x := pValue.(type) {
		case float64:
			f = x
		case int64:
			f = float64(x)
			if int64(f) != x {
				return fmt.Sprintf("value %v overflows struct field of type %v", x, v.Type())
			}
		case nil:
			f = 0
		default:
			return typeMismatchReason(pValue, v)
		}
		if v.OverflowFloat(f) {
			return fmt.Sprintf("value %v overflows struct field of type %v", f, v.Type())
		}
		v.SetFloat(f)
	case reflect.Ptr:
		x, ok := pValue.(*Key)
		if !ok && pValue != nil {
			return typeMismatchReason(pValue, v)
		}
		if _, ok := v.Interface().(*Key); !ok {
			return typeMismatchReason(pValue, v)
		}
		v.Set(reflect.ValueOf(x))
	case reflect.Struct:
		switch v.Type() {
		case typeOfTime:
			x, ok := pValue.(time.Time)
			if !ok && pValue != nil {
				return typeMismatchReason(pValue, v)
			}
			v.Set(reflect.ValueOf(x))
		case typeOfGeoPoint:
			x, ok := pValue.(appengine.GeoPoint)
			if !ok && pValue != nil {
				return typeMismatchReason(pValue, v)
			}
			v.Set(reflect.ValueOf(x))
		default:
			ent, ok := pValue.(*Entity)
			if !ok {
				return typeMismatchReason(pValue, v)
			}

			// Recursively load nested struct
			pls, err := newStructPLS(v.Addr().Interface())
			if err != nil {
				return err.Error()
			}

			// if ent has a Key value and our struct has a Key field,
			// load the Entity's Key value into the Key field on the struct.
			if ent.Key != nil && pls.codec.keyField != -1 {

				pls.v.Field(pls.codec.keyField).Set(reflect.ValueOf(ent.Key))
			}

			err = pls.Load(ent.Properties)
			if err != nil {
				return err.Error()
			}
		}
	case reflect.Slice:
		x, ok := pValue.([]byte)
		if !ok {
			if y, yok := pValue.(ByteString); yok {
				x, ok = []byte(y), true
			}
		}
		if !ok && pValue != nil {
			return typeMismatchReason(pValue, v)
		}
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return typeMismatchReason(pValue, v)
		}
		v.SetBytes(x)
	default:
		return typeMismatchReason(pValue, v)
	}
	return ""
}

// initField is similar to reflect's Value.FieldByIndex, in that it
// returns the nested struct field corresponding to index, but it
// initialises any nil pointers encountered when traversing the structure.
func initField(val reflect.Value, index []int) reflect.Value {
	for _, i := range index[:len(index)-1] {
		val = val.Field(i)
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				val.Set(reflect.New(val.Type().Elem()))
			}
			val = val.Elem()
		}
	}
	return val.Field(index[len(index)-1])
}

// loadEntity loads an Document into PropertyLoadSaver or struct pointer.
func loadEntity(dst interface{}, src *pb.Document) (err error) {
	ent, err := protoToEntity(src)
	if err != nil {
		return err
	}
	if e, ok := dst.(PropertyLoadSaver); ok {
		return e.Load(ent.Properties)
	}
	return LoadStruct(dst, ent.Properties)
}

func (s structPLS) Load(props []Property) error {
	var fieldName, reason string
	var l propertyLoader
	for _, p := range props {
		if errStr := l.load(s.codec, s.v, p, p.Multiple); errStr != "" {
			// We don't return early, as we try to load as many properties as possible.
			// It is valid to load an entity into a struct that cannot fully represent it.
			// That case returns an error, but the caller is free to ignore it.
			fieldName, reason = p.Name, errStr
		}
	}
	if reason != "" {
		return &ErrFieldMismatch{
			StructType: s.v.Type(),
			FieldName:  fieldName,
			Reason:     reason,
		}
	}
	return nil
}

func protoToEntity(src *pb.Document) (*Entity, error) {
	outProps := make([]Property, 0, len(src.Fields))
	for name, fieldvalue := range src.Fields {
		values := []*pb.Value{fieldvalue}
		multiple := false
		if array, ok := fieldvalue.ValueType.(*pb.Value_ArrayValue); ok {
			values = array.ArrayValue.Values
			multiple = true
		}
		for _, x := range values {
			value, err := propValue(x)
			if err != nil {
				return nil, err
			}
			outProps = append(outProps, Property{
				Name:     name,
				Value:    value,
				Multiple: multiple,
			})
		}
	}

	var key *Key
	if src.Name != "" {
		// Ignore any error, since nested entity values
		// are allowed to have an invalid key.
		key, _ = referenceValueToKey(src.Name)
	}
	return &Entity{key, outProps}, nil
}

// propValue returns a Go value from a firestore Value.
func propValue(v *pb.Value) (interface{}, error) {
	switch vt := v.ValueType.(type) {
	case *pb.Value_IntegerValue:
		return vt.IntegerValue, nil
	case *pb.Value_TimestampValue:
		return ptypes.Timestamp(vt.TimestampValue)
	case *pb.Value_BooleanValue:
		return vt.BooleanValue, nil
	case *pb.Value_BytesValue:
		return vt.BytesValue, nil
	case *pb.Value_MapValue:
		return protoToEntity(&pb.Document{Fields: vt.MapValue.Fields})
	case *pb.Value_StringValue:
		return vt.StringValue, nil
	case *pb.Value_ArrayValue:
		return nil, fmt.Errorf("datastore: unhandled ArrayValue")
	case *pb.Value_DoubleValue:
		return vt.DoubleValue, nil
	case *pb.Value_ReferenceValue:
		key, err := referenceValueToKey(vt.ReferenceValue)
		if err != nil {
			return nil, err
		}
		return key, nil
	case *pb.Value_GeoPointValue:
		return appengine.GeoPoint{Lat: vt.GeoPointValue.Latitude, Lng: vt.GeoPointValue.Longitude}, nil
	}
	return nil, nil
}
