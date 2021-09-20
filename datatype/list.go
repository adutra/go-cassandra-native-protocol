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

type ListType interface {
	DataType
	GetElementType() DataType
}

type listType struct {
	elementType DataType
}

func (t *listType) GetElementType() DataType {
	return t.elementType
}

func NewListType(elementType DataType) ListType {
	return &listType{elementType: elementType}
}

func (t *listType) GetDataTypeCode() primitive.DataTypeCode {
	return primitive.DataTypeCodeList
}

func (t *listType) Clone() DataType {
	return &listType{
		elementType: t.elementType.Clone(),
	}
}

func (t *listType) String() string {
	return fmt.Sprintf("list<%v>", t.elementType)
}

func (t *listType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

func writeListType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if listType, ok := t.(ListType); !ok {
		return fmt.Errorf("expected ListType, got %T", t)
	} else if err = WriteDataType(listType.GetElementType(), dest, version); err != nil {
		return fmt.Errorf("cannot write list element type: %w", err)
	}
	return nil
}

func lengthOfListType(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	if listType, ok := t.(ListType); !ok {
		return -1, fmt.Errorf("expected ListType, got %T", t)
	} else if elementLength, err := LengthOfDataType(listType.GetElementType(), version); err != nil {
		return -1, fmt.Errorf("cannot compute length of list element type: %w", err)
	} else {
		length += elementLength
	}
	return length, nil
}

func readListType(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	listType := &listType{}
	if listType.elementType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read list element type: %w", err)
	}
	return listType, nil
}
