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

type MapType interface {
	DataType
	GetKeyType() DataType
	GetValueType() DataType
}

type mapType struct {
	keyType   DataType
	valueType DataType
}

func (t *mapType) GetKeyType() DataType {
	return t.keyType
}

func (t *mapType) GetValueType() DataType {
	return t.valueType
}

func (t *mapType) GetDataTypeCode() primitive.DataTypeCode {
	return primitive.DataTypeCodeMap
}

func (t *mapType) Clone() DataType {
	return &mapType{
		keyType:   t.keyType.Clone(),
		valueType: t.valueType.Clone(),
	}
}

func (t *mapType) String() string {
	return fmt.Sprintf("map<%v,%v>", t.keyType, t.valueType)
}

func (t *mapType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

func NewMapType(keyType DataType, valueType DataType) MapType {
	return &mapType{keyType: keyType, valueType: valueType}
}

func writeMapType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	mapType, ok := t.(MapType)
	if !ok {
		return fmt.Errorf("expected MapType, got %T", t)
	} else if err = WriteDataType(mapType.GetKeyType(), dest, version); err != nil {
		return fmt.Errorf("cannot write map key type: %w", err)
	} else if err = WriteDataType(mapType.GetValueType(), dest, version); err != nil {
		return fmt.Errorf("cannot write map value type: %w", err)
	}
	return nil
}

func lengthOfMapType(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	mapType, ok := t.(MapType)
	if !ok {
		return -1, fmt.Errorf("expected MapType, got %T", t)
	}
	if keyLength, err := LengthOfDataType(mapType.GetKeyType(), version); err != nil {
		return -1, fmt.Errorf("cannot compute length of map key type: %w", err)
	} else {
		length += keyLength
	}
	if valueLength, err := LengthOfDataType(mapType.GetValueType(), version); err != nil {
		return -1, fmt.Errorf("cannot compute length of map value type: %w", err)
	} else {
		length += valueLength
	}
	return length, nil
}

func readMapType(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	mapType := &mapType{}
	if mapType.keyType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read map key type: %w", err)
	} else if mapType.valueType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read map value type: %w", err)
	}
	return mapType, nil
}
