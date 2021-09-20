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

package datatype

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type DataType interface {
	fmt.Stringer

	GetDataTypeCode() primitive.DataTypeCode
	Clone() DataType
}

func WriteDataType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if t == nil {
		return fmt.Errorf("DataType can not be nil")
	} else if err = primitive.CheckValidDataTypeCode(t.GetDataTypeCode(), version); err != nil {
		return err
	} else if err = primitive.WriteShort(uint16(t.GetDataTypeCode()), dest); err != nil {
		return fmt.Errorf("cannot write data type code %v: %w", t.GetDataTypeCode(), err)
	} else {
		switch t.GetDataTypeCode() {
		case primitive.DataTypeCodeCustom:
			return writeCustomType(t, dest, version)
		case primitive.DataTypeCodeList:
			return writeListType(t, dest, version)
		case primitive.DataTypeCodeMap:
			return writeMapType(t, dest, version)
		case primitive.DataTypeCodeSet:
			return writeSetType(t, dest, version)
		case primitive.DataTypeCodeUdt:
			return writeUserDefinedType(t, dest, version)
		case primitive.DataTypeCodeTuple:
			return writeTupleType(t, dest, version)
		}
		return
	}
}

func LengthOfDataType(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	length += primitive.LengthOfShort // type code
	dataTypeLength := 0
	switch t.GetDataTypeCode() {
	case primitive.DataTypeCodeCustom:
		dataTypeLength, err = lengthOfCustomType(t, version)
	case primitive.DataTypeCodeList:
		dataTypeLength, err = lengthOfListType(t, version)
	case primitive.DataTypeCodeMap:
		dataTypeLength, err = lengthOfMapType(t, version)
	case primitive.DataTypeCodeSet:
		dataTypeLength, err = lengthOfSetType(t, version)
	case primitive.DataTypeCodeUdt:
		dataTypeLength, err = lengthOfUserDefinedType(t, version)
	case primitive.DataTypeCodeTuple:
		dataTypeLength, err = lengthOfTupleType(t, version)
	}
	if err != nil {
		return -1, fmt.Errorf("cannot compute length of data type %v: %w", t, err)
	}
	return length + dataTypeLength, nil
}

func ReadDataType(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	var typeCode uint16
	if typeCode, err = primitive.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read data type code: %w", err)
	} else if err := primitive.CheckValidDataTypeCode(primitive.DataTypeCode(typeCode), version); err != nil {
		return nil, err
	} else {
		switch primitive.DataTypeCode(typeCode) {
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
		case primitive.DataTypeCodeDecimal:
			return Decimal, nil
		case primitive.DataTypeCodeDouble:
			return Double, nil
		case primitive.DataTypeCodeFloat:
			return Float, nil
		case primitive.DataTypeCodeInt:
			return Int, nil
		case primitive.DataTypeCodeTimestamp:
			return Timestamp, nil
		case primitive.DataTypeCodeUuid:
			return Uuid, nil
		case primitive.DataTypeCodeVarchar:
			return Varchar, nil
		case primitive.DataTypeCodeVarint:
			return Varint, nil
		case primitive.DataTypeCodeTimeuuid:
			return Timeuuid, nil
		case primitive.DataTypeCodeInet:
			return Inet, nil
		case primitive.DataTypeCodeDate:
			return Date, nil
		case primitive.DataTypeCodeTime:
			return Time, nil
		case primitive.DataTypeCodeSmallint:
			return Smallint, nil
		case primitive.DataTypeCodeTinyint:
			return Tinyint, nil
		case primitive.DataTypeCodeDuration:
			return Duration, nil
		case primitive.DataTypeCodeCustom:
			return readCustomType(source, version)
		case primitive.DataTypeCodeList:
			return readListType(source, version)
		case primitive.DataTypeCodeMap:
			return readMapType(source, version)
		case primitive.DataTypeCodeSet:
			return readSetType(source, version)
		case primitive.DataTypeCodeUdt:
			return readUserDefinedType(source, version)
		case primitive.DataTypeCodeTuple:
			return readTupleType(source, version)
		}
		return nil, fmt.Errorf("unknown type code: %w", err)
	}
}

func cloneDataTypeSlice(o []DataType) []DataType {
	var newFieldTypes []DataType
	for _, fieldType := range o {
		if fieldType == nil {
			newFieldTypes = append(newFieldTypes, nil)
		} else {
			newFieldTypes = append(newFieldTypes, fieldType.Clone())
		}
	}

	return newFieldTypes
}
