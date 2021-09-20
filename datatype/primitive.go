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
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

var (
	Ascii     PrimitiveType = &primitiveType{code: primitive.DataTypeCodeAscii}
	Bigint    PrimitiveType = &primitiveType{code: primitive.DataTypeCodeBigint}
	Blob      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeBlob}
	Boolean   PrimitiveType = &primitiveType{code: primitive.DataTypeCodeBoolean}
	Counter   PrimitiveType = &primitiveType{code: primitive.DataTypeCodeCounter}
	Date      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeDate}
	Decimal   PrimitiveType = &primitiveType{code: primitive.DataTypeCodeDecimal}
	Double    PrimitiveType = &primitiveType{code: primitive.DataTypeCodeDouble}
	Duration  PrimitiveType = &primitiveType{code: primitive.DataTypeCodeDuration}
	Float     PrimitiveType = &primitiveType{code: primitive.DataTypeCodeFloat}
	Inet      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeInet}
	Int       PrimitiveType = &primitiveType{code: primitive.DataTypeCodeInt}
	Smallint  PrimitiveType = &primitiveType{code: primitive.DataTypeCodeSmallint}
	Time      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTime}
	Timestamp PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTimestamp}
	Timeuuid  PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTimeuuid}
	Tinyint   PrimitiveType = &primitiveType{code: primitive.DataTypeCodeTinyint}
	Uuid      PrimitiveType = &primitiveType{code: primitive.DataTypeCodeUuid}
	Varchar   PrimitiveType = &primitiveType{code: primitive.DataTypeCodeVarchar}
	Varint    PrimitiveType = &primitiveType{code: primitive.DataTypeCodeVarint}
)

type PrimitiveType interface {
	DataType
}

type primitiveType struct {
	code primitive.DataTypeCode
}

func (t *primitiveType) GetDataTypeCode() primitive.DataTypeCode {
	return t.code
}

func (t *primitiveType) Clone() DataType {
	return &primitiveType{
		code: t.code,
	}
}

func (t *primitiveType) String() string {
	switch t.GetDataTypeCode() {
	case primitive.DataTypeCodeAscii:
		return "ascii"
	case primitive.DataTypeCodeBigint:
		return "bigint"
	case primitive.DataTypeCodeBlob:
		return "blob"
	case primitive.DataTypeCodeBoolean:
		return "boolean"
	case primitive.DataTypeCodeCounter:
		return "counter"
	case primitive.DataTypeCodeDecimal:
		return "decimal"
	case primitive.DataTypeCodeDouble:
		return "double"
	case primitive.DataTypeCodeFloat:
		return "float"
	case primitive.DataTypeCodeInt:
		return "int"
	case primitive.DataTypeCodeTimestamp:
		return "timestamp"
	case primitive.DataTypeCodeUuid:
		return "uuid"
	case primitive.DataTypeCodeVarchar:
		return "varchar"
	case primitive.DataTypeCodeVarint:
		return "varint"
	case primitive.DataTypeCodeTimeuuid:
		return "timeuuid"
	case primitive.DataTypeCodeInet:
		return "inet"
	case primitive.DataTypeCodeDate:
		return "date"
	case primitive.DataTypeCodeTime:
		return "time"
	case primitive.DataTypeCodeSmallint:
		return "smallint"
	case primitive.DataTypeCodeTinyint:
		return "tinyint"
	case primitive.DataTypeCodeDuration:
		return "duration"
	}
	return "?"
}

func (t *primitiveType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}
