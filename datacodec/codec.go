// Copyright 2020 DataStax
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
	"reflect"
)

type Encoder interface {

	// Encode encodes the given source into dest. The parameter source must be a value of a supported Go type for the
	// CQL type being encoded, or a pointer thereto; a nil value is encoded as a CQL NULL.
	Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error)
}

type Decoder interface {

	// Decode decodes the given source into dest. The parameter dest must be a pointer to a supported Go type for
	// the CQL type being decoded; it cannot be nil. If return parameter wasNull is true, then the decoded value was a
	// NULL, in which case the actual value stored in dest will be set to its zero value.
	Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error)
}

// Codec is a codec for a specific CQL type.
type Codec interface {
	Encoder
	Decoder

	DataType() datatype.DataType
}

func NewCodec(dt datatype.DataType) (Codec, error) {
	switch dt.GetDataTypeCode() {
	case primitive.DataTypeCodeAscii:
		return Ascii, nil
	case primitive.DataTypeCodeBigint:
		return Bigint, nil
	case primitive.DataTypeCodeBlob:
		return Blob, nil
	case primitive.DataTypeCodeBoolean:
		return Boolean, nil
	case primitive.DataTypeCodeCounter:
		return Counter, nil
	case primitive.DataTypeCodeCustom:
		return NewCustom(dt.(datatype.CustomType)), nil
	case primitive.DataTypeCodeDate:
		return Date, nil
	case primitive.DataTypeCodeDecimal:
		return Decimal, nil
	case primitive.DataTypeCodeDouble:
		return Double, nil
	case primitive.DataTypeCodeDuration:
		return Duration, nil
	case primitive.DataTypeCodeFloat:
		return Float, nil
	case primitive.DataTypeCodeInet:
		return Inet, nil
	case primitive.DataTypeCodeInt:
		return Int, nil
	case primitive.DataTypeCodeSmallint:
		return Smallint, nil
	case primitive.DataTypeCodeTime:
		return Time, nil
	case primitive.DataTypeCodeTimestamp:
		return TimestampUTC, nil
	case primitive.DataTypeCodeTimeuuid:
		return Timeuuid, nil
	case primitive.DataTypeCodeTinyint:
		return Tinyint, nil
	case primitive.DataTypeCodeUuid:
		return Uuid, nil
	case primitive.DataTypeCodeVarchar:
		return Varchar, nil
	case primitive.DataTypeCodeVarint:
		return Varint, nil
	case primitive.DataTypeCodeList:
		return NewList(dt.(datatype.ListType))
	case primitive.DataTypeCodeSet:
		return NewSet(dt.(datatype.SetType))
	case primitive.DataTypeCodeMap:
		return NewMap(dt.(datatype.MapType))
	case primitive.DataTypeCodeTuple:
		return NewTuple(dt.(datatype.TupleType))
	case primitive.DataTypeCodeUdt:
		return NewUserDefined(dt.(datatype.UserDefinedType))
	}
	return nil, errCannotCreateCodec(dt)
}

func PreferredGoType(dt datatype.DataType) (reflect.Type, error) {
	switch dt.GetDataTypeCode() {
	case primitive.DataTypeCodeAscii:
		return typeOfString, nil
	case primitive.DataTypeCodeBigint:
		return typeOfInt64, nil
	case primitive.DataTypeCodeBlob:
		return typeOfByteSlice, nil
	case primitive.DataTypeCodeBoolean:
		return typeOfBoolean, nil
	case primitive.DataTypeCodeCounter:
		return typeOfInt64, nil
	case primitive.DataTypeCodeCustom:
		return typeOfByteSlice, nil
	case primitive.DataTypeCodeDate:
		return typeOfTime, nil
	case primitive.DataTypeCodeDecimal:
		return typeOfCqlDecimal, nil
	case primitive.DataTypeCodeDouble:
		return typeOfFloat64, nil
	case primitive.DataTypeCodeDuration:
		return typeOfCqlDuration, nil
	case primitive.DataTypeCodeFloat:
		return typeOfFloat32, nil
	case primitive.DataTypeCodeInet:
		return typeOfNetIP, nil
	case primitive.DataTypeCodeInt:
		return typeOfInt, nil
	case primitive.DataTypeCodeSmallint:
		return typeOfInt16, nil
	case primitive.DataTypeCodeTime:
		return typeOfDuration, nil
	case primitive.DataTypeCodeTimestamp:
		return typeOfTime, nil
	case primitive.DataTypeCodeTimeuuid:
		return typeOfUUID, nil
	case primitive.DataTypeCodeTinyint:
		return typeOfInt8, nil
	case primitive.DataTypeCodeTuple:
		return typeOfInterfaceSlice, nil
	case primitive.DataTypeCodeUdt:
		return typeOfStringToInterfaceMap, nil
	case primitive.DataTypeCodeUuid:
		return typeOfUUID, nil
	case primitive.DataTypeCodeVarchar:
		return typeOfString, nil
	case primitive.DataTypeCodeVarint:
		return typeOfBigIntPointer, nil
	case primitive.DataTypeCodeList:
		listType := dt.(datatype.ListType)
		elemType, err := PreferredGoType(listType.GetElementType())
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elemType), nil
	case primitive.DataTypeCodeSet:
		setType := dt.(datatype.SetType)
		elemType, err := PreferredGoType(setType.GetElementType())
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elemType), nil
	case primitive.DataTypeCodeMap:
		mapType := dt.(datatype.MapType)
		keyType, err := PreferredGoType(mapType.GetKeyType())
		if err != nil {
			return nil, err
		}
		valueType, err := PreferredGoType(mapType.GetValueType())
		if err != nil {
			return nil, err
		}
		return reflect.MapOf(keyType, valueType), nil
	}
	return nil, errCannotFindGoType(dt)
}
