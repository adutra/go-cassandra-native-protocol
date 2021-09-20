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
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"reflect"
)

var errNilDestination = errors.New("destination is nil")
var errNilDataType = errors.New("data type is nil")

var errConversionNotSupported = errors.New("conversion not supported")
var errSourceTypeNotSupported = errors.New("source type not supported")
var errDestinationTypeNotSupported = errors.New("destination type not supported")

var errPointerTypeExpected = errors.New("destination is not pointer")

func errCannotEncode(source interface{}, dataType datatype.DataType, version primitive.ProtocolVersion, err error) error {
	return fmt.Errorf("cannot encode %T as CQL %s with %v: %w", source, dataType, version, err)
}

func errCannotDecode(dest interface{}, dataType datatype.DataType, version primitive.ProtocolVersion, err error) error {
	return fmt.Errorf("cannot decode CQL %s as %T with %v: %w", dataType, dest, version, err)
}

func errDataTypeNotSupported(dataType datatype.DataType, version primitive.ProtocolVersion) error {
	return fmt.Errorf("data type %s not supported in %v", dataType, version)
}

func errSourceFetchFailed(from interface{}, to interface{}, err error) error {
	return fmt.Errorf("cannot assign %T to %T: %w", from, to, err)
}

func errDestStoreFailed(from interface{}, to interface{}, err error) error {
	return fmt.Errorf("cannot store %T into %T: %w", from, to, err)
}

func errCannotRead(val interface{}, err error) error {
	return fmt.Errorf("cannot read %T: %w", val, err)
}

func errCannotWrite(val interface{}, err error) error {
	return fmt.Errorf("cannot write %T: %w", val, err)
}

func errCannotParseString(s string, err error) error {
	return fmt.Errorf("cannot parse '%v': %w", s, err)
}

func errValueOutOfRange(val interface{}) error {
	return fmt.Errorf("value out of range: %v", val)
}

func errSliceIndexOutOfRange(desc string, index int) error {
	return fmt.Errorf("%s index out of range: %v", desc, index)
}

func errWrongFixedLength(expected, actual int) error {
	return fmt.Errorf("expected %v bytes but got: %v", expected, actual)
}

func errWrongMinimumLength(expected, actual int) error {
	return fmt.Errorf("expected at least %v bytes but got: %v", expected, actual)
}

func errWrongFixedLengths(expected1, expected2, actual int) error {
	return fmt.Errorf("expected %v or %v bytes but got: %v", expected1, expected2, actual)
}

func errWrongContainerType(expected string, actual reflect.Type) error {
	return fmt.Errorf("expected %s, got: %s", expected, actual)
}

func errWrongElementType(desc string, expected reflect.Type, actual interface{}) error {
	return fmt.Errorf("wrong %s, expected %s, got: '%v'", desc, expected, actual)
}

func errWrongElementTypes(desc string, expected1, expected2 reflect.Type, actual interface{}) error {
	return fmt.Errorf("wrong %s, expected %s or %s, got: '%v'", desc, expected1, expected2, actual)
}

func errBytesRemaining(total int, remaining int) error {
	return fmt.Errorf("source was not fully read: bytes total: %d, read: %d, remaining: %d", total, total-remaining, remaining)
}

func errCannotReadUdtField(i int, name string, err error) error {
	return fmt.Errorf("cannot read field %d (%s): %w", i, name, err)
}

func errCannotDecodeUdtField(i int, name string, err error) error {
	return fmt.Errorf("cannot decode field %d (%s): %w", i, name, err)
}

func errCannotEncodeUdtField(i int, name string, err error) error {
	return fmt.Errorf("cannot encode field %d (%s): %w", i, name, err)
}

func errNilElement(i int) error {
	return fmt.Errorf("element %d is nil", i)
}

func errNilMapKey(i int) error {
	return fmt.Errorf("entry %d key is nil", i)
}

func errNilMapValue(i int) error {
	return fmt.Errorf("entry %d value is nil", i)
}

func errCannotEncodeElement(i int, err error) error {
	return fmt.Errorf("cannot encode element %d: %w", i, err)
}

func errCannotEncodeMapKey(i int, err error) error {
	return fmt.Errorf("cannot encode entry %d key: %w", i, err)
}

func errCannotEncodeMapValue(i int, err error) error {
	return fmt.Errorf("cannot encode entry %d value: %w", i, err)
}

func errElementEncodedToNil(i int) error {
	return fmt.Errorf("element %d was encoded to nil", i)
}

func errMapKeyEncodedToNil(i int) error {
	return fmt.Errorf("entry %d key was encoded to nil", i)
}

func errMapValueEncodedToNil(i int) error {
	return fmt.Errorf("entry %d value was encoded to nil", i)
}

func errCannotDecodeElement(i int, err error) error {
	return fmt.Errorf("cannot decode element %d: %w", i, err)
}

func errCannotDecodeMapKey(i int, err error) error {
	return fmt.Errorf("cannot decode entry %d key: %w", i, err)
}

func errCannotDecodeMapValue(i int, err error) error {
	return fmt.Errorf("cannot decode entry %d value: %w", i, err)
}

func errCannotReadElement(i int, err error) error {
	return fmt.Errorf("cannot read element %d: %w", i, err)
}

func errCannotReadMapKey(i int, err error) error {
	return fmt.Errorf("cannot read entry %d key: %w", i, err)
}

func errCannotReadMapValue(i int, err error) error {
	return fmt.Errorf("cannot read entry %d value: %w", i, err)
}

func errCannotExtractElement(i int, err error) error {
	return fmt.Errorf("cannot extract element %d: %w", i, err)
}

func errCannotExtractMapValue(i int, err error) error {
	return fmt.Errorf("cannot extract entry %d value: %w", i, err)
}

func errCannotExtractUdtField(i int, name string, err error) error {
	return fmt.Errorf("cannot extract field %d (%s): %w", i, name, err)
}

func errCannotCreateElement(i int, err error) error {
	return fmt.Errorf("cannot create zero element %d: %w", i, err)
}

func errCannotCreateMapKey(i int, err error) error {
	return fmt.Errorf("cannot create zero entry %d key: %w", i, err)
}

func errCannotCreateMapValue(i int, err error) error {
	return fmt.Errorf("cannot create zero entry %d value: %w", i, err)
}

func errCannotCreateUdtField(i int, name string, err error) error {
	return fmt.Errorf("cannot create zero field %d (%s): %w", i, name, err)
}

func errCannotInjectElement(i int, err error) error {
	return fmt.Errorf("cannot inject element %d: %w", i, err)
}

func errCannotInjectMapEntry(i int, err error) error {
	return fmt.Errorf("cannot inject entry %d: %w", i, err)
}

func errCannotInjectUdtField(i int, name string, err error) error {
	return fmt.Errorf("cannot inject field %d (%s): %w", i, name, err)
}

func errDestinationUnaddressable(value reflect.Value) error {
	return fmt.Errorf("destination of type %s is not addressable", value.Type())
}

func errStructFieldInvalid(structValue reflect.Value, key interface{}) error {
	if i, ok := key.(int); ok {
		return fmt.Errorf("no accessible field with index %d found in struct %s", i, structValue.Type())
	} else {
		return fmt.Errorf("no accessible field with name '%v' found in struct %s", key, structValue.Type())
	}
}

func cannotWriteCollectionSize(err error) error {
	return fmt.Errorf("cannot write collection size: %w", err)
}

func collectionSizeTooLarge(size, max int) error {
	return fmt.Errorf("collection too large (%d elements, max is %d)", size, max)
}

func collectionSizeNegative(size int) error {
	return fmt.Errorf("expected collection size >= 0, got: %d", size)
}

func errCannotCreateCodec(dt datatype.DataType) error {
	return fmt.Errorf("cannot create data codec for CQL type %v", dt)
}

func errCannotFindGoType(dt datatype.DataType) error {
	return fmt.Errorf("could not find any suitable Go type for CQL type %v", dt)
}

func errDestinationInvalid(dest interface{}) error {
	if dest == nil {
		return errNilDestination
	} else if reflect.TypeOf(dest).Kind() == reflect.Ptr {
		return errConversionNotSupported
	} else {
		return errPointerTypeExpected
	}
}
