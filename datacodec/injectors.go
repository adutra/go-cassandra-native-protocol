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
	"reflect"
)

// A utility to inject elements in container types like slices, arrays, structs and maps, using a unified API.
type injector interface {

	// Prepares the container for receiving decoded elements by adjusting its internal size to meet the required size.
	// If the container cannot be resized (structs and arrays), calling this method should be a no-op.
	ensureSize(size int)

	// zeroElem returns a pointer to a zero value for the element at the given key.
	// Note that the returned value *must* be a pointer, even if the underlying element type is not, because the
	// returned value will be passed to Codec.Decode.
	// dataType is only useful when the element's type is interface{}: in this case the returned value should have
	// the concrete type returned by PreferredGoType.
	zeroElem(index int, key interface{}, dataType datatype.DataType) (value interface{}, err error)

	// setElem sets the element at the given key to the given decoded value.
	// Note that the decoded value will *always* be a pointer: if the underlying element type is not a pointer, the
	// value needs to be passed through reflect.Indirect before being set. wasNull indicates that the decoded element
	// was a CQL NULL.
	setElem(index int, key, value interface{}, keyWasNull, valueWasNull bool, keyDataType, valueDataType datatype.DataType) error
}

// A utility to inject keys and values in maps.
type keyValueInjector interface {
	injector

	// zeroKey returns a pointer to a zero value for the map key type.
	// Note that the returned value *must* be a pointer, even if the underlying key type is not, because the returned
	// value will be passed to Codec.Decode.
	// dataType is only useful when the key's type is interface{}: in this case the returned value should have
	// the concrete type returned by PreferredGoType.
	zeroKey(index int, dataType datatype.DataType) (value interface{}, err error)
}

type sliceInjector struct {
	dest          reflect.Value
	containerType string
}

type structInjector struct {
	dest          reflect.Value
	fieldsByIndex map[int]reflect.Value
	fieldsByName  map[string]reflect.Value
}

type mapInjector struct {
	dest reflect.Value
}

func newSliceInjector(dest reflect.Value) (injector, error) {
	if !dest.IsValid() {
		return nil, errDestinationTypeNotSupported
	} else if dest.Kind() != reflect.Slice && dest.Kind() != reflect.Array {
		return nil, errWrongContainerType("slice or array", dest.Type())
	} else if !dest.CanSet() {
		return nil, errDestinationUnaddressable(dest)
	}
	containerType := "slice"
	if dest.Kind() == reflect.Array {
		containerType = "array"
	}
	return &sliceInjector{dest, containerType}, nil

}
func newStructInjector(dest reflect.Value) (injector, error) {
	if !dest.IsValid() {
		return nil, errDestinationTypeNotSupported
	} else if dest.Kind() != reflect.Struct {
		return nil, errWrongContainerType("struct", dest.Type())
	} else if !dest.CanSet() {
		return nil, errDestinationUnaddressable(dest)
	}
	return &structInjector{dest: dest}, nil
}

func newMapInjector(dest reflect.Value) (keyValueInjector, error) {
	if !dest.IsValid() {
		return nil, errDestinationTypeNotSupported
	} else if dest.Kind() != reflect.Map {
		return nil, errWrongContainerType("map", dest.Type())
	} else if !dest.CanSet() {
		return nil, errDestinationUnaddressable(dest)
	}
	return &mapInjector{dest}, nil
}

func (i *sliceInjector) zeroElem(_ int, _ interface{}, dataType datatype.DataType) (value interface{}, err error) {
	if elementType, err := handleInterface(i.dest.Type().Elem(), dataType); err != nil {
		return nil, err
	} else {
		zero := ensurePointer(nilSafeZero(elementType))
		return zero.Interface(), nil
	}
}

func (i *sliceInjector) setElem(index int, _, value interface{}, _, wasNull bool, _, dataType datatype.DataType) error {
	if index < 0 || index >= i.dest.Len() {
		return errSliceIndexOutOfRange(i.containerType, index)
	}
	elementType := i.dest.Type().Elem()
	if wasNull {
		zero := reflect.Zero(elementType)
		i.dest.Index(index).Set(zero)
	} else {
		elementType, _ = handleInterface(elementType, dataType)
		newValue := maybeIndirect(elementType, reflect.ValueOf(value))
		if !newValue.Type().AssignableTo(elementType) {
			return errWrongElementType(i.containerType+" element", elementType, newValue.Interface())
		}
		i.dest.Index(index).Set(newValue)
	}
	return nil
}

func (i *structInjector) zeroElem(_ int, key interface{}, dataType datatype.DataType) (interface{}, error) {
	if field, err := i.locateAndStoreField(key); err != nil {
		return nil, err
	} else if field.IsValid() && field.CanSet() {
		if fieldType, err := handleInterface(field.Type(), dataType); err != nil {
			return nil, err
		} else {
			zero := ensurePointer(nilSafeZero(fieldType))
			return zero.Interface(), nil
		}
	} else {
		return nil, errStructFieldInvalid(i.dest, key)
	}
}

func (i *structInjector) setElem(_ int, key, value interface{}, _, wasNull bool, _, dataType datatype.DataType) error {
	field := i.retrieveStoredField(key)
	if field.IsValid() && field.CanSet() {
		fieldType := field.Type()
		if wasNull {
			zero := reflect.Zero(fieldType)
			field.Set(zero)
		} else {
			fieldType, _ = handleInterface(fieldType, dataType)
			newValue := maybeIndirect(fieldType, reflect.ValueOf(value))
			if !newValue.Type().AssignableTo(fieldType) {
				return errWrongElementType("struct field value", fieldType, newValue.Interface())
			}
			field.Set(newValue)
		}
		return nil
	} else {
		return errStructFieldInvalid(i.dest, key)
	}
}

func (i *structInjector) locateAndStoreField(key interface{}) (field reflect.Value, err error) {
	switch k := key.(type) {
	case string:
		field = locateFieldByName(i.dest, k)
		if i.fieldsByName == nil {
			i.fieldsByName = map[string]reflect.Value{}
		}
		i.fieldsByName[k] = field
	case int:
		field = locateFieldByIndex(i.dest, k)
		if i.fieldsByIndex == nil {
			i.fieldsByIndex = map[int]reflect.Value{}
		}
		i.fieldsByIndex[k] = field
	default:
		err = errWrongElementTypes("struct field key", typeOfInt, typeOfString, key)
	}
	return
}

func (i *structInjector) retrieveStoredField(key interface{}) reflect.Value {
	var field reflect.Value
	switch k := key.(type) {
	case string:
		field = i.fieldsByName[k]
	case int:
		field = i.fieldsByIndex[k]
	}
	return field
}

func (i *mapInjector) zeroKey(_ int, dataType datatype.DataType) (interface{}, error) {
	if keyType, err := handleInterface(i.dest.Type().Key(), dataType); err != nil {
		return nil, err
	} else {
		zero := ensurePointer(nilSafeZero(keyType))
		return zero.Interface(), nil
	}
}

func (i *mapInjector) zeroElem(_ int, _ interface{}, dataType datatype.DataType) (interface{}, error) {
	if valueType, err := handleInterface(i.dest.Type().Elem(), dataType); err != nil {
		return nil, err
	} else {
		zero := ensurePointer(nilSafeZero(valueType))
		return zero.Interface(), nil
	}
}

func (i *mapInjector) setElem(_ int, key, value interface{}, keyWasNull, valueWasNull bool, keyDataType, valueDataType datatype.DataType) error {
	keyType := i.dest.Type().Key()
	var newKey reflect.Value
	if keyWasNull {
		newKey = reflect.Zero(keyType)
	} else {
		keyType, _ = handleInterface(keyType, keyDataType)
		newKey = maybeIndirect(keyType, reflect.ValueOf(key))
		if !newKey.Type().AssignableTo(keyType) {
			return errWrongElementType("map key", keyType, newKey.Interface())
		}
	}
	valueType := i.dest.Type().Elem()
	var newValue reflect.Value
	if valueWasNull {
		newValue = reflect.Zero(valueType)
	} else {
		valueType, _ = handleInterface(valueType, valueDataType)
		newValue = maybeIndirect(valueType, reflect.ValueOf(value))
		if !newValue.Type().AssignableTo(valueType) {
			return errWrongElementType("map value", valueType, newValue.Interface())
		}
	}
	i.dest.SetMapIndex(newKey, newValue)
	return nil
}

func (i *sliceInjector) ensureSize(size int) {
	if i.dest.Kind() == reflect.Slice {
		adjustSliceLength(i.dest, size)
	}
}

func (i *mapInjector) ensureSize(size int) {
	adjustMapSize(i.dest, size)
}

func (i *structInjector) ensureSize(_ int) {
}
