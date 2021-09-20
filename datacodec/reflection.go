// Copyright 2021 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacodec

import (
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"math/big"
	"net"
	"reflect"
	"strings"
	"time"
)

var (
	typeOfString               = reflect.TypeOf("")
	typeOfInt64                = reflect.TypeOf(int64(0))
	typeOfByteSlice            = reflect.TypeOf([]byte{})
	typeOfBoolean              = reflect.TypeOf(false)
	typeOfTime                 = reflect.TypeOf(time.Time{})
	typeOfCqlDecimal           = reflect.TypeOf(CqlDecimal{})
	typeOfCqlDuration          = reflect.TypeOf(CqlDuration{})
	typeOfFloat64              = reflect.TypeOf(float64(0))
	typeOfFloat32              = reflect.TypeOf(float32(0))
	typeOfNetIP                = reflect.TypeOf((*net.IP)(nil)).Elem()
	typeOfInt                  = reflect.TypeOf(0)
	typeOfInt16                = reflect.TypeOf(int16(0))
	typeOfDuration             = reflect.TypeOf(time.Duration(0))
	typeOfUUID                 = reflect.TypeOf(primitive.UUID{})
	typeOfInt8                 = reflect.TypeOf(int8(0))
	typeOfInterfaceSlice       = reflect.TypeOf([]interface{}{})
	typeOfStringToInterfaceMap = reflect.TypeOf(map[string]interface{}{})
	typeOfBigIntPointer        = reflect.TypeOf(big.NewInt(0))
)

// reflectSource is used for collections, maps, tuples and udts. It analyzes the source and returns reflection data.
// If the source is a pointer, the pointer is de-referenced and the reflection data will refer to the pointer's target
// value.
func reflectSource(source interface{}) (sourceValue reflect.Value, sourceType reflect.Type, wasNil bool) {
	sourceType = reflect.TypeOf(source)
	if source == nil {
		wasNil = true
	} else {
		sourceValue = reflect.ValueOf(source)
		if sourceValue.Kind() == reflect.Ptr {
			sourceType = sourceType.Elem()
			wasNil = sourceValue.IsNil()
			sourceValue = reflect.Indirect(sourceValue)
		}
		if sourceValue.Kind() == reflect.Slice || sourceValue.Kind() == reflect.Map {
			wasNil = sourceValue.IsNil()
		}
	}
	return
}

// reflectDest is used for collections, maps, tuples and udts. It analyzes the destination and returns reflection data.
// If the destination is nil or is not a pointer, an error is returned; if the source was null, the destination is set
// to its zero value; otherwise, the pointer is de-referenced and the reflection data will refer to the pointer's target
// value.
func reflectDest(dest interface{}, wasNull bool) (destValue reflect.Value, err error) {
	if dest == nil {
		err = errNilDestination
	} else {
		destValue = reflect.ValueOf(dest)
		if destValue.Kind() != reflect.Ptr {
			err = errPointerTypeExpected
		} else if destValue.IsNil() {
			err = errNilDestination
		} else {
			if wasNull {
				zero := reflect.Zero(destValue.Elem().Type())
				destValue.Elem().Set(zero)
			}
			destValue = reflect.Indirect(destValue)
		}
	}
	return
}

// Adjusts the given slice so that its length is >= targetSize. Note: this method should only be called on an
// addressable slice, that is, its CanSet() method must return true, otherwise it will panic.
func adjustSliceLength(sliceValue reflect.Value, targetSize int) {
	if sliceValue.IsNil() || sliceValue.Cap() < targetSize {
		sliceValue.Set(reflect.MakeSlice(sliceValue.Type(), targetSize, targetSize))
	} else if sliceValue.Len() != targetSize {
		sliceValue.SetLen(targetSize)
	}
}

// Adjusts the given map so that its length is >= targetSize. Note: this method should only be called on an
// addressable map, that is, its CanSet() method must return true, otherwise it will panic.
func adjustMapSize(mapValue reflect.Value, targetSize int) {
	mapValue.Set(reflect.MakeMapWithSize(mapValue.Type(), targetSize))
}

// if the value is pointer, returns the value as is; otherwise, returns a pointer to it.
// This is done prior to decoding a value, since Codec.Decode requires a pointer value when decoding.
func ensurePointer(value reflect.Value) reflect.Value {
	if value.Kind() != reflect.Ptr {
		return pointerTo(value)
	}
	return value
}

// if the target type is not pointer but the value is pointer, returns the pointer's target, otherwise returns the
// value unchanged.
// This is done after decoding a value, since Codec.Decode requires a pointer value when decoding.
func maybeIndirect(targetType reflect.Type, value reflect.Value) reflect.Value {
	if value.Kind() == reflect.Ptr && targetType.Kind() != reflect.Ptr {
		return reflect.Indirect(value)
	}
	return value
}

// equivalent to reflect.Zero(), but with one exception: for pointers, instead of returning a nil pointer, returns a
// pointer to a zero value, effectively achieving the same effect as using reflect.New().
// Used in injectors when creating zero values for decoding purposes, since Codec.Decode does not accept nil pointers.
func nilSafeZero(targetType reflect.Type) reflect.Value {
	switch targetType.Kind() {
	case reflect.Ptr:
		return pointerTo(nilSafeZero(targetType.Elem()))
	default:
		return reflect.Zero(targetType)
	}
}

// functionally equivalent to Value.Addr() but doesn't panic if target is unaddressable.
func pointerTo(target reflect.Value) reflect.Value {
	ptr := reflect.New(target.Type())
	ptr.Elem().Set(target)
	return ptr
}

func handleInterface(targetType reflect.Type, dataType datatype.DataType) (reflect.Type, error) {
	if targetType.Kind() == reflect.Interface {
		if preferred, err := PreferredGoType(dataType); err != nil {
			return nil, err
		} else {
			return preferred, nil
		}
	}
	return targetType, nil
}

// Locates a struct field given a UDT field name. If no field can be located, this function returns a zero
// reflect.Value. If the struct field has a "cassandra" tag, then the tag must match the UDT field name exactly;
// otherwise, the UDT field name must match the struct field name, but case insensitively.
// The case-insensitive lookup is required because only exported struct fields can be located, and such fields are
// required to have a first upper-case letter.
func locateFieldByName(structValue reflect.Value, name string) (value reflect.Value) {
	structType := structValue.Type()
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if strings.EqualFold(name, field.Name) || field.Tag.Get("cassandra") == name {
			value = structValue.Field(i)
			break
		}
	}
	return
}

// Locates a struct field given its index. If the index is out of range, this function returns a zero reflect.Value.
func locateFieldByIndex(structValue reflect.Value, index int) (value reflect.Value) {
	structType := structValue.Type()
	if index >= 0 && index < structType.NumField() {
		value = structValue.Field(index)
	}
	return
}
