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
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTupleType(t *testing.T) {
	tupleType := NewTupleType(Varchar, Int)
	assert.Equal(t, primitive.DataTypeCodeTuple, tupleType.GetDataTypeCode())
	assert.Equal(t, []DataType{Varchar, Int}, tupleType.GetFieldTypes())
}

func TestTupleTypeClone(t *testing.T) {
	tt := NewTupleType(Varchar, Int)
	cloned := tt.Clone().(*tupleType)
	assert.Equal(t, tt, cloned)
	cloned.fieldTypes = []DataType{Int, Uuid, Float}
	assert.NotEqual(t, tt, cloned)
	assert.Equal(t, primitive.DataTypeCodeTuple, tt.GetDataTypeCode())
	assert.Equal(t, []DataType{Varchar, Int}, tt.GetFieldTypes())
	assert.Equal(t, primitive.DataTypeCodeTuple, cloned.GetDataTypeCode())
	assert.Equal(t, []DataType{Int, Uuid, Float}, cloned.GetFieldTypes())
}

func TestTupleTypeClone_NilFieldTypesSlice(t *testing.T) {
	tt := NewTupleType(Varchar, Int).(*tupleType)
	tt.fieldTypes = nil
	cloned := tt.Clone().(*tupleType)
	assert.Equal(t, tt, cloned)
	cloned.fieldTypes = []DataType{Int, Uuid, Float}
	assert.NotEqual(t, tt, cloned)
	assert.Equal(t, primitive.DataTypeCodeTuple, tt.GetDataTypeCode())
	assert.Nil(t, tt.GetFieldTypes())
	assert.Equal(t, primitive.DataTypeCodeTuple, cloned.GetDataTypeCode())
	assert.Equal(t, []DataType{Int, Uuid, Float}, cloned.GetFieldTypes())
}

func TestTupleTypeClone_NilFieldType(t *testing.T) {
	tt := NewTupleType(nil, Int).(*tupleType)
	cloned := tt.Clone().(*tupleType)
	assert.Equal(t, tt, cloned)
	cloned.fieldTypes = []DataType{Int, Uuid, Float}
	assert.NotEqual(t, tt, cloned)
	assert.Equal(t, primitive.DataTypeCodeTuple, tt.GetDataTypeCode())
	assert.Equal(t, []DataType{nil, Int}, tt.GetFieldTypes())
	assert.Equal(t, primitive.DataTypeCodeTuple, cloned.GetDataTypeCode())
	assert.Equal(t, []DataType{Int, Uuid, Float}, cloned.GetFieldTypes())
}

func TestTupleTypeClone_ComplexFieldTypes(t *testing.T) {
	tt := NewTupleType(NewListType(NewTupleType(Varchar)), Int).(*tupleType)
	cloned := tt.Clone().(*tupleType)
	assert.Equal(t, tt, cloned)
	cloned.GetFieldTypes()[0].(*listType).elementType = NewTupleType(Int)
	assert.NotEqual(t, tt, cloned)
	assert.Equal(t, primitive.DataTypeCodeTuple, tt.GetDataTypeCode())
	assert.Equal(t, []DataType{NewListType(NewTupleType(Varchar)), Int}, tt.GetFieldTypes())
	assert.Equal(t, primitive.DataTypeCodeTuple, cloned.GetDataTypeCode())
	assert.Equal(t, []DataType{NewListType(NewTupleType(Int)), Int}, cloned.GetFieldTypes())
}

func TestWriteTupleType(t *testing.T) {
	tests := []struct {
		name     string
		input    TupleType
		expected []byte
		err      error
	}{
		{
			"simple tuple",
			NewTupleType(Varchar, Int),
			[]byte{
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
				0, 2, // field count
				0, byte(primitive.DataTypeCodeVarchar & 0xFF),
				0, byte(primitive.DataTypeCodeInt & 0xFF),
			},
			nil,
		},
		{
			"complex tuple",
			NewTupleType(NewTupleType(Varchar, Int), NewTupleType(Boolean, Float)),
			[]byte{
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
				0, 2, // field count
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
				0, 2, // field count
				0, byte(primitive.DataTypeCodeVarchar & 0xFF),
				0, byte(primitive.DataTypeCodeInt & 0xFF),
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
				0, 2, // field count
				0, byte(primitive.DataTypeCodeBoolean & 0xFF),
				0, byte(primitive.DataTypeCodeFloat & 0xFF),
			},
			nil,
		},
		{"nil tuple", nil, nil, errors.New("DataType can not be nil")},
	}

	t.Run("versions_with_tuple_support", func(t *testing.T) {
		for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
			t.Run(version.String(), func(t *testing.T) {
				for _, test := range tests {
					t.Run(test.name, func(t *testing.T) {
						var dest = &bytes.Buffer{}
						var err error
						err = WriteDataType(test.input, dest, version)
						actual := dest.Bytes()
						assert.Equal(t, test.err, err)
						assert.Equal(t, test.expected, actual)
					})
				}
			})
		}
	})

	t.Run("versions_without_tuple_support", func(t *testing.T) {
		for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
			t.Run(version.String(), func(t *testing.T) {
				for _, test := range tests {
					t.Run(test.name, func(t *testing.T) {
						var dest = &bytes.Buffer{}
						var err error
						err = WriteDataType(test.input, dest, version)
						actual := dest.Bytes()
						require.NotNil(t, err)
						if test.err != nil {
							assert.Equal(t, test.err, err)
						} else {
							assert.Contains(t, err.Error(),
								fmt.Sprintf("invalid data type code for %s: DataTypeCode Tuple", version))
						}
						assert.Equal(t, 0, len(actual))
					})
				}
			})
		}
	})
}

func TestLengthOfTupleType(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    TupleType
				expected int
				err      error
			}{
				{
					"simple tuple",
					NewTupleType(Varchar, Int),
					primitive.LengthOfShort * 3,
					nil,
				},
				{
					"complex tuple",
					NewTupleType(NewTupleType(Varchar, Int), NewTupleType(Boolean, Float)),
					primitive.LengthOfShort * 9,
					nil,
				},
				{"nil tuple", nil, -1, errors.New("expected TupleType, got <nil>")},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var actual int
					var err error
					actual, err = lengthOfTupleType(test.input, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestReadTupleType(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected TupleType
		err      error
	}{
		{
			"simple tuple",
			[]byte{
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
				0, 2, // field count
				0, byte(primitive.DataTypeCodeVarchar & 0xFF),
				0, byte(primitive.DataTypeCodeInt & 0xFF),
			},
			NewTupleType(Varchar, Int),
			nil,
		},
		{
			"complex tuple",
			[]byte{
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
				0, 2, // field count
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
				0, 2, // field count
				0, byte(primitive.DataTypeCodeVarchar & 0xFF),
				0, byte(primitive.DataTypeCodeInt & 0xFF),
				0, byte(primitive.DataTypeCodeTuple & 0xFF),
				0, 2, // field count
				0, byte(primitive.DataTypeCodeBoolean & 0xFF),
				0, byte(primitive.DataTypeCodeFloat & 0xFF),
			},
			NewTupleType(NewTupleType(Varchar, Int), NewTupleType(Boolean, Float)),
			nil,
		},
		{
			"cannot read tuple",
			[]byte{
				0, byte(primitive.DataTypeCodeTuple & 0xFF)},
			nil,
			fmt.Errorf("cannot read tuple field count: %w",
				fmt.Errorf("cannot read [short]: %w",
					errors.New("EOF"))),
		},
	}

	t.Run("versions_with_tuple_support", func(t *testing.T) {
		for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
			t.Run(version.String(), func(t *testing.T) {

				for _, test := range tests {
					t.Run(test.name, func(t *testing.T) {
						var source = bytes.NewBuffer(test.input)
						var actual DataType
						var err error
						actual, err = ReadDataType(source, version)
						assert.Equal(t, test.expected, actual)
						assert.Equal(t, test.err, err)
					})
				}
			})
		}
	})

	t.Run("versions_without_tuple_support", func(t *testing.T) {
		for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
			t.Run(version.String(), func(t *testing.T) {
				for _, test := range tests {
					t.Run(test.name, func(t *testing.T) {
						var source = bytes.NewBuffer(test.input)
						var actual DataType
						var err error
						actual, err = ReadDataType(source, version)
						require.NotNil(t, err)
						assert.Contains(t, err.Error(),
							fmt.Sprintf("invalid data type code for %s: DataTypeCode Tuple", version))
						assert.Nil(t, actual)
					})
				}
			})
		}
	})
}

func Test_tupleType_String(t1 *testing.T) {
	tests := []struct {
		name       string
		fieldTypes []DataType
		want       string
	}{
		{"empty", []DataType{}, "tuple<>"},
		{"simple", []DataType{Int, Varchar, Boolean}, "tuple<int,varchar,boolean>"},
		{"complex", []DataType{Int, NewTupleType(Varchar, Boolean)}, "tuple<int,tuple<varchar,boolean>>"},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t *testing.T) {
			tuple := NewTupleType(tt.fieldTypes...)
			got := tuple.String()
			assert.Equal(t, tt.want, got)
		})
	}
}
