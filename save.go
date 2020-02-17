// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package datastore

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"

	"google.golang.org/appengine"
	pb "google.golang.org/genproto/googleapis/firestore/v1"
	"google.golang.org/genproto/googleapis/type/latlng"
)

func fromUnixMicro(t int64) time.Time {
	return time.Unix(t/1e6, (t%1e6)*1e3).UTC()
}

var (
	minTime = time.Unix(int64(math.MinInt64)/1e6, (int64(math.MinInt64)%1e6)*1e3)
	maxTime = time.Unix(int64(math.MaxInt64)/1e6, (int64(math.MaxInt64)%1e6)*1e3)
)

// valueToProto converts a value to a newly allocated Value.
// The returned error string is empty on success.
func valueToProto(defaultAppID string, v reflect.Value) (pv *pb.Value, errStr string) {
	var (
		unsupported bool
	)
	switch v.Kind() {
	case reflect.Invalid:
		// No-op.
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		pv.ValueType = &pb.Value_IntegerValue{v.Int()}
	case reflect.Bool:
		pv.ValueType = &pb.Value_BooleanValue{v.Bool()}
	case reflect.String:
		pv.ValueType = &pb.Value_StringValue{v.String()}
	case reflect.Float32, reflect.Float64:
		pv.ValueType = &pb.Value_DoubleValue{v.Float()}
	case reflect.Ptr:
		if k, ok := v.Interface().(*Key); ok {
			if k != nil {
				pv.ValueType = &pb.Value_ReferenceValue{keyToReferenceValue(defaultAppID, k)}
			}
		} else {
			unsupported = true
		}
	case reflect.Struct:
		switch t := v.Interface().(type) {
		case time.Time:
			if t.Before(minTime) || t.After(maxTime) {
				return nil, "time value out of range"
			}
			ts, _ := ptypes.TimestampProto(t)
			pv.ValueType = &pb.Value_TimestampValue{ts}
		case appengine.GeoPoint:
			if !t.Valid() {
				return nil, "invalid GeoPoint value"
			}
			pv.ValueType = &pb.Value_GeoPointValue{&latlng.LatLng{Latitude: t.Lat, Longitude: t.Lng}}
		default:
			unsupported = true
		}
	case reflect.Slice:
		if b, ok := v.Interface().([]byte); ok {
			pv.ValueType = &pb.Value_BytesValue{b}
		} else {
			// nvToProto should already catch slice values.
			// If we get here, we have a slice of slice values.
			unsupported = true
		}
	default:
		unsupported = true
	}
	if unsupported {
		return nil, "unsupported datastore value type: " + v.Type().String()
	}

	return pv, ""
}

type saveOpts struct {
	noIndex   bool
	multiple  bool
	omitEmpty bool
}

// saveEntity saves a PropertyLoadSaver or struct pointer into a Firestore Write.
func saveEntity(c context.Context, defaultAppID string, key *Key, src interface{}) (*pb.Write, error) {
	var err error
	var props []Property
	if e, ok := src.(PropertyLoadSaver); ok {
		props, err = e.Save()
	} else {
		props, err = SaveStruct(src)
	}
	if err != nil {
		return nil, err
	}
	return propertiesToProto(c, defaultAppID, key, props)
}

func saveStructProperty(props *[]Property, name string, opts saveOpts, v reflect.Value) error {
	if opts.omitEmpty && isEmptyValue(v) {
		return nil
	}
	p := Property{
		Name:     name,
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	}
	switch x := v.Interface().(type) {
	case *Key:
		p.Value = x
	case time.Time:
		p.Value = x
	case appengine.BlobKey:
		p.Value = x
	case appengine.GeoPoint:
		p.Value = x
	case ByteString:
		p.Value = x
	default:
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			p.Value = v.Int()
		case reflect.Bool:
			p.Value = v.Bool()
		case reflect.String:
			p.Value = v.String()
		case reflect.Float32, reflect.Float64:
			p.Value = v.Float()
		case reflect.Slice:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				p.NoIndex = true
				p.Value = v.Bytes()
			}
		case reflect.Struct:
			if !v.CanAddr() {
				return fmt.Errorf("datastore: unsupported struct field: value is unaddressable")
			}
			sub, err := newStructPLS(v.Addr().Interface())
			if err != nil {
				return fmt.Errorf("datastore: unsupported struct field: %v", err)
			}
			return sub.save(props, name+".", opts)
		}
	}
	if p.Value == nil {
		return fmt.Errorf("datastore: unsupported struct field type: %v", v.Type())
	}
	*props = append(*props, p)
	return nil
}

func (s structPLS) Save() ([]Property, error) {
	var props []Property
	if err := s.save(&props, "", saveOpts{}); err != nil {
		return nil, err
	}
	return props, nil
}

func (s structPLS) save(props *[]Property, prefix string, opts saveOpts) error {
	for name, f := range s.codec.fields {
		name = prefix + name
		v := s.v.FieldByIndex(f.path)
		if !v.IsValid() || !v.CanSet() {
			continue
		}
		var opts1 saveOpts
		opts1.noIndex = opts.noIndex || f.noIndex
		opts1.multiple = opts.multiple
		opts1.omitEmpty = f.omitEmpty // don't propagate
		// For slice fields that aren't []byte, save each element.
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() != reflect.Uint8 {
			opts1.multiple = true
			for j := 0; j < v.Len(); j++ {
				if err := saveStructProperty(props, name, opts1, v.Index(j)); err != nil {
					return err
				}
			}
			continue
		}
		// Otherwise, save the field itself.
		if err := saveStructProperty(props, name, opts1, v); err != nil {
			return err
		}
	}
	return nil
}

func propertiesToProto(c context.Context, defaultAppID string, key *Key, props []Property) (*pb.Write, error) {
	var precondition *pb.Precondition
	if key.Incomplete() {
		key = &Key{
			kind:     key.Kind(),
			stringID: uniqueID(),
			appID:    key.AppID(),
			parent:   key.Parent(),
		}
		precondition = &pb.Precondition{ConditionType: &pb.Precondition_Exists{false}}
	}
	e := &pb.Document{
		Name:   keyToReferenceValue(defaultAppID, key),
		Fields: make(map[string]*pb.Value),
	}

	prevMultiple := make(map[string]bool)

	for _, p := range props {
		if pm, ok := prevMultiple[p.Name]; ok {
			if !pm || !p.Multiple {
				return nil, fmt.Errorf("datastore: multiple Properties with Name %q, but Multiple is false", p.Name)
			}
		} else {
			prevMultiple[p.Name] = p.Multiple
		}

		x := &pb.Value{}
		switch v := p.Value.(type) {
		case int64:
			x.ValueType = &pb.Value_IntegerValue{v}
		case bool:
			x.ValueType = &pb.Value_BooleanValue{v}
		case string:
			x.ValueType = &pb.Value_StringValue{v}
		case float64:
			x.ValueType = &pb.Value_DoubleValue{v}
		case *Key:
			if v != nil {
				x.ValueType = &pb.Value_ReferenceValue{keyToReferenceValue(defaultAppID, v)}
			}
		case time.Time:
			if v.Before(minTime) || v.After(maxTime) {
				return nil, fmt.Errorf("datastore: time value out of range")
			}
			ts, _ := ptypes.TimestampProto(v)
			x.ValueType = &pb.Value_TimestampValue{ts}
		case appengine.BlobKey:
			x.ValueType = &pb.Value_StringValue{string(v)}
		case appengine.GeoPoint:
			if !v.Valid() {
				return nil, fmt.Errorf("datastore: invalid GeoPoint value")
			}
			x.ValueType = &pb.Value_GeoPointValue{&latlng.LatLng{Latitude: v.Lat, Longitude: v.Lng}}
		case []byte:
			x.ValueType = &pb.Value_BytesValue{v}
		case ByteString:
			x.ValueType = &pb.Value_BytesValue{v}
		default:
			if p.Value != nil {
				return nil, fmt.Errorf("datastore: invalid Value type for a Property with Name %q", p.Name)
			}
		}

		if p.Multiple {
			if existing, ok := e.Fields[p.Name]; ok {
				av := existing.ValueType.(*pb.Value_ArrayValue).ArrayValue
				av.Values = append(av.Values, x)
			} else {
				e.Fields[p.Name] = &pb.Value{ValueType: &pb.Value_ArrayValue{&pb.ArrayValue{Values: []*pb.Value{x}}}}
			}
		} else {
			e.Fields[p.Name] = x
		}
	}
	return &pb.Write{
		Operation:       &pb.Write_Update{e},
		CurrentDocument: precondition,
	}, nil
}

func setProperty(e *pb.Document, name string, value *pb.Value, multiple bool) error {
	// convert dotted names to MapValues.
	path := strings.Split(name, ".")
	fields := e.Fields
	for len(path) > 1 {
		var mapvalue *pb.Value_MapValue
		if existing, ok := fields[path[0]]; ok {
			if mapvalue, ok = existing.ValueType.(*pb.Value_MapValue); !ok {
				return fmt.Errorf("datastore: multiple nested Properties with name " + name)
			}
		} else {
			mapvalue = &pb.Value_MapValue{&pb.MapValue{Fields: make(map[string]*pb.Value)}}
			fields[path[0]] = &pb.Value{ValueType: mapvalue}
		}
		fields = mapvalue.MapValue.Fields
		path = path[1:]
	}

	if !multiple {
		fields[path[0]] = value
	}

	// Collect multiples into ArrayValues.
	var arrayvalue *pb.Value_ArrayValue
	if existing, ok := fields[path[0]]; ok {
		if arrayvalue, ok = existing.ValueType.(*pb.Value_ArrayValue); !ok {
			return fmt.Errorf("datastore: multiple and non-multiple properties with name " + name)
		}
	} else {
		arrayvalue = &pb.Value_ArrayValue{&pb.ArrayValue{}}
		fields[path[0]] = &pb.Value{ValueType: arrayvalue}
	}
	arrayvalue.ArrayValue.Values = append(arrayvalue.ArrayValue.Values, value)
	return nil
}

// isEmptyValue is taken from the encoding/json package in the standard library.
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		// TODO(perfomance): Only reflect.String needed, other property types are not supported (copy/paste from json package)
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		// TODO(perfomance): Uint* are unsupported property types - should be removed (copy/paste from json package)
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	case reflect.Struct:
		switch x := v.Interface().(type) {
		case time.Time:
			return x.IsZero()
		}
	}
	return false
}
