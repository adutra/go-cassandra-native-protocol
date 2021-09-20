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

type SetType interface {
	DataType
	GetElementType() DataType
}

type setType struct {
	elementType DataType
}

func (t *setType) GetElementType() DataType {
	return t.elementType
}

func (t *setType) GetDataTypeCode() primitive.DataTypeCode {
	return primitive.DataTypeCodeSet
}

func (t *setType) Clone() DataType {
	return &setType{
		elementType: t.elementType.Clone(),
	}
}

func (t *setType) String() string {
	return fmt.Sprintf("set<%v>", t.elementType)
}

func (t *setType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

func NewSetType(elementType DataType) SetType {
	return &setType{elementType: elementType}
}

func writeSetType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if setType, ok := t.(SetType); !ok {
		return fmt.Errorf("expected SetType, got %T", t)
	} else if err = WriteDataType(setType.GetElementType(), dest, version); err != nil {
		return fmt.Errorf("cannot write set element type: %w", err)
	}
	return nil
}

func lengthOfSetType(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	if setType, ok := t.(SetType); !ok {
		return -1, fmt.Errorf("expected SetType, got %T", t)
	} else if elementLength, err := LengthOfDataType(setType.GetElementType(), version); err != nil {
		return -1, fmt.Errorf("cannot compute length of set element type: %w", err)
	} else {
		length += elementLength
	}
	return length, nil
}

func readSetType(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	setType := &setType{}
	if setType.elementType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read set element type: %w", err)
	}
	return setType, nil
}
