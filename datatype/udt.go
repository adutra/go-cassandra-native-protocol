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
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type UserDefinedType interface {
	DataType
	GetKeyspace() string
	GetName() string
	// names and types are not modeled as a map because iteration order matters
	GetFieldNames() []string
	GetFieldTypes() []DataType
}

type userDefinedType struct {
	keyspace   string
	name       string
	fieldNames []string
	fieldTypes []DataType
}

func (t *userDefinedType) GetKeyspace() string {
	return t.keyspace
}

func (t *userDefinedType) GetName() string {
	return t.name
}

func (t *userDefinedType) GetFieldNames() []string {
	return t.fieldNames
}

func (t *userDefinedType) GetFieldTypes() []DataType {
	return t.fieldTypes
}

func NewUserDefinedType(keyspace string, name string, fieldNames []string, fieldTypes []DataType) (UserDefinedType, error) {
	fieldNamesLength := len(fieldNames)
	fieldTypesLength := len(fieldTypes)
	if fieldNamesLength != fieldTypesLength {
		return nil, fmt.Errorf("field names and field types length mismatch: %d != %d", fieldNamesLength, fieldTypesLength)
	}
	return &userDefinedType{keyspace: keyspace, name: name, fieldNames: fieldNames, fieldTypes: fieldTypes}, nil
}

func (t *userDefinedType) GetDataTypeCode() primitive.DataTypeCode {
	return primitive.DataTypeCodeUdt
}

func (t *userDefinedType) Clone() DataType {
	return &userDefinedType{
		keyspace:   t.keyspace,
		name:       t.name,
		fieldNames: primitive.CloneStringSlice(t.fieldNames),
		fieldTypes: cloneDataTypeSlice(t.fieldTypes),
	}
}

func (t *userDefinedType) String() string {
	buf := &bytes.Buffer{}
	buf.WriteString(t.keyspace)
	buf.WriteString(".")
	buf.WriteString(t.name)
	buf.WriteString("<")
	for i, fieldType := range t.fieldTypes {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(t.fieldNames[i])
		buf.WriteString(":")
		buf.WriteString(fieldType.String())
	}
	buf.WriteString(">")
	return buf.String()
}

func (t *userDefinedType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

func writeUserDefinedType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	userDefinedType, ok := t.(UserDefinedType)
	if !ok {
		return fmt.Errorf("expected UserDefinedType, got %T", t)
	} else if err = primitive.WriteString(userDefinedType.GetKeyspace(), dest); err != nil {
		return fmt.Errorf("cannot write udt keyspace: %w", err)
	} else if err = primitive.WriteString(userDefinedType.GetName(), dest); err != nil {
		return fmt.Errorf("cannot write udt name: %w", err)
	} else if err = primitive.WriteShort(uint16(len(userDefinedType.GetFieldTypes())), dest); err != nil {
		return fmt.Errorf("cannot write udt field count: %w", err)
	}
	if len(userDefinedType.GetFieldNames()) != len(userDefinedType.GetFieldTypes()) {
		return fmt.Errorf("invalid user-defined type: length of field names is not equal to length of field types")
	}
	for i, fieldName := range userDefinedType.GetFieldNames() {
		fieldType := userDefinedType.GetFieldTypes()[i]
		if err = primitive.WriteString(fieldName, dest); err != nil {
			return fmt.Errorf("cannot write udt field %v name: %w", fieldName, err)
		} else if err = WriteDataType(fieldType, dest, version); err != nil {
			return fmt.Errorf("cannot write udt field %v: %w", fieldName, err)
		}
	}
	return nil
}

func lengthOfUserDefinedType(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	userDefinedType, ok := t.(UserDefinedType)
	if !ok {
		return -1, fmt.Errorf("expected UserDefinedType, got %T", t)
	}
	length += primitive.LengthOfString(userDefinedType.GetKeyspace())
	length += primitive.LengthOfString(userDefinedType.GetName())
	length += primitive.LengthOfShort // field count
	if len(userDefinedType.GetFieldNames()) != len(userDefinedType.GetFieldTypes()) {
		return -1, fmt.Errorf("invalid user-defined type: length of field names is not equal to length of field types")
	}
	for i, fieldName := range userDefinedType.GetFieldNames() {
		fieldType := userDefinedType.GetFieldTypes()[i]
		length += primitive.LengthOfString(fieldName)
		if fieldLength, err := LengthOfDataType(fieldType, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of udt field %v: %w", fieldName, err)
		} else {
			length += fieldLength
		}
	}
	return length, nil
}

func readUserDefinedType(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	userDefinedType := &userDefinedType{}
	if userDefinedType.keyspace, err = primitive.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read udt keyspace: %w", err)
	} else if userDefinedType.name, err = primitive.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read udt name: %w", err)
	} else if fieldCount, err := primitive.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read udt field count: %w", err)
	} else {
		userDefinedType.fieldNames = make([]string, fieldCount)
		userDefinedType.fieldTypes = make([]DataType, fieldCount)
		for i := 0; i < int(fieldCount); i++ {
			if userDefinedType.fieldNames[i], err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read udt field %d name: %w", i, err)
			} else if userDefinedType.fieldTypes[i], err = ReadDataType(source, version); err != nil {
				return nil, fmt.Errorf("cannot read udt field %d: %w", i, err)
			}
		}
		return userDefinedType, nil
	}
}
