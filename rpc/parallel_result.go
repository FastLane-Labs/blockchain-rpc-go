package rpc

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// Decode each candidate into independent storage and publish only a success.
// Copy initialized values too: decoding into a zero value loses map entries and
// custom decoder configuration. Only the calling goroutine accesses result.
func decodeParallelResult(raw []byte, result any) error {
	v := reflect.ValueOf(result)
	if !v.IsValid() || v.Kind() != reflect.Pointer || v.IsNil() {
		return &json.InvalidUnmarshalError{Type: reflect.TypeOf(result)}
	}
	var seen map[resultVisit]reflect.Value
	value, err := copyResult(v.Elem(), &seen)
	if err != nil {
		return err
	}
	candidate := reflect.New(v.Elem().Type())
	candidate.Elem().Set(value)
	if err := json.Unmarshal(raw, candidate.Interface()); err != nil {
		return err
	}
	v.Elem().Set(candidate.Elem())
	return nil
}

type resultVisit struct {
	typ reflect.Type
	ptr uintptr
	len int
}

func copyResult(v reflect.Value, seen *map[resultVisit]reflect.Value) (reflect.Value, error) {
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if v.IsNil() {
			return v, nil
		}
		key := resultVisit{typ: v.Type(), ptr: v.Pointer()}
		if v.Kind() == reflect.Slice {
			key.len = v.Len()
		}
		if previous, ok := (*seen)[key]; ok {
			return previous, nil
		}
		if *seen == nil {
			*seen = make(map[resultVisit]reflect.Value)
		}
		var dst reflect.Value
		switch v.Kind() {
		case reflect.Pointer:
			dst = reflect.New(v.Type().Elem())
		case reflect.Map:
			dst = reflect.MakeMapWithSize(v.Type(), v.Len())
		case reflect.Slice:
			dst = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		}
		(*seen)[key] = dst
		switch v.Kind() {
		case reflect.Pointer:
			value, err := copyResult(v.Elem(), seen)
			if err != nil {
				return reflect.Value{}, err
			}
			dst.Elem().Set(value)
		case reflect.Map:
			iter := v.MapRange()
			for iter.Next() {
				key, err := copyResult(iter.Key(), seen)
				if err != nil {
					return reflect.Value{}, err
				}
				value, err := copyResult(iter.Value(), seen)
				if err != nil {
					return reflect.Value{}, err
				}
				dst.SetMapIndex(key, value)
			}
		case reflect.Slice:
			for i := 0; i < v.Len(); i++ {
				value, err := copyResult(v.Index(i), seen)
				if err != nil {
					return reflect.Value{}, err
				}
				dst.Index(i).Set(value)
			}
		}
		return dst, nil
	case reflect.Interface:
		if v.IsNil() {
			return v, nil
		}
		value, err := copyResult(v.Elem(), seen)
		if err != nil {
			return reflect.Value{}, err
		}
		dst := reflect.New(v.Type()).Elem()
		dst.Set(value)
		return dst, nil
	case reflect.UnsafePointer:
		if !v.IsNil() {
			return reflect.Value{}, fmt.Errorf("parallel RPC result cannot isolate %s", v.Type())
		}
		return v, nil
	case reflect.Struct, reflect.Array:
		dst := reflect.New(v.Type()).Elem()
		dst.Set(v)
		count := v.Len
		field, target := v.Index, dst.Index
		if v.Kind() == reflect.Struct {
			count, field, target = v.NumField, v.Field, dst.Field
		}
		for i := 0; i < count(); i++ {
			if !target(i).CanSet() {
				// Private reference state cannot be isolated safely without unsafe.
				// Refuse it instead of allowing a rejected decoder to mutate it.
				if hasResultReferences(field(i)) {
					return reflect.Value{}, fmt.Errorf("parallel RPC result %s has initialized private reference state in %s", v.Type(), v.Type().Field(i).Name)
				}
				continue
			}
			value, err := copyResult(field(i), seen)
			if err != nil {
				return reflect.Value{}, err
			}
			target(i).Set(value)
		}
		return dst, nil
	default:
		return v, nil
	}
}

func hasResultReferences(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.UnsafePointer:
		return !v.IsNil()
	case reflect.Interface:
		return !v.IsNil() && hasResultReferences(v.Elem())
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if hasResultReferences(v.Field(i)) {
				return true
			}
		}
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if hasResultReferences(v.Index(i)) {
				return true
			}
		}
	}
	return false
}
