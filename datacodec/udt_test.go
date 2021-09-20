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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

var (
	udtTypeSimple, _ = datatype.NewUserDefinedType(
		"ks1",
		"type1",
		[]string{"f1", "f2", "f3"},
		[]datatype.DataType{datatype.Int, datatype.Boolean, datatype.Varchar},
	)
	udtTypeComplex, _ = datatype.NewUserDefinedType(
		"ks1",
		"type2",
		[]string{"f1", "f2"},
		[]datatype.DataType{udtTypeSimple, udtTypeSimple},
	)
	udtTypeEmpty, _ = datatype.NewUserDefinedType("ks1", "type3", []string{}, []datatype.DataType{})
	udtTypeWrong, _ = datatype.NewUserDefinedType("ks1", "type4", []string{"f1"}, []datatype.DataType{wrongDataType{}})
)

var (
	udtCodecSimple, _  = NewUserDefined(udtTypeSimple)
	udtCodecComplex, _ = NewUserDefined(udtTypeComplex)
	udtCodecEmpty, _   = NewUserDefined(udtTypeEmpty)
)

type (
	SimpleUdt struct {
		F1 int
		F2 bool
		F3 *string
	}
	partialUdt struct {
		F1 int
		F2 bool
	}
	excessUdt struct {
		F1 int
		F2 bool
		F3 *string
		F4 float64
	}
	complexUdt struct {
		F1 SimpleUdt
		F2 *excessUdt
	}
)

func TestNewUserDefinedCodec(t *testing.T) {
	tests := []struct {
		name     string
		dataType datatype.UserDefinedType
		expected Codec
		err      string
	}{
		{
			"simple",
			udtTypeSimple,
			&udtCodec{dataType: udtTypeSimple, fieldCodecs: []Codec{Int, Boolean, Varchar}},
			"",
		},
		{
			"complex",
			udtTypeComplex,
			&udtCodec{
				dataType: udtTypeComplex,
				fieldCodecs: []Codec{
					&udtCodec{dataType: udtTypeSimple, fieldCodecs: []Codec{Int, Boolean, Varchar}},
					&udtCodec{dataType: udtTypeSimple, fieldCodecs: []Codec{Int, Boolean, Varchar}},
				},
			},
			"",
		},
		{
			"empty",
			udtTypeEmpty,
			&udtCodec{dataType: udtTypeEmpty, fieldCodecs: []Codec{}},
			"",
		},
		{
			"wrong child",
			udtTypeWrong,
			nil,
			"cannot create codec for user-defined type field 0 (f1): cannot create data codec for CQL type 666",
		},
		{
			"nil",
			nil,
			nil,
			"data type is nil",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := NewUserDefined(tt.dataType)
			assert.Equal(t, tt.expected, actual)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_udtCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			t.Run("[]interface{}", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    *[]interface{}
					expected []byte
					err      string
				}{
					{"nil", udtCodecEmpty, nil, nil, ""},
					{"empty", udtCodecSimple, &[]interface{}{nil, nil, nil}, []byte{
						255, 255, 255, 255, // nil int
						255, 255, 255, 255, // nil boolean
						255, 255, 255, 255, // nil string
					}, ""},
					{"simple", udtCodecSimple, &[]interface{}{123, true, "abc"}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						1,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
					}, ""},
					{"simple with pointers", udtCodecSimple, &[]interface{}{intPtr(123), boolPtr(true), stringPtr("abc")}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						1,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
					}, ""},
					{"nil element", udtCodecSimple, &[]interface{}{123, false, nil}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						0,                  // boolean
						255, 255, 255, 255, // nil string
					}, ""},
					{"not enough elements", udtCodecSimple, &[]interface{}{123}, nil, "slice index out of range: 1"},
					{"too many elements", udtCodecSimple, &[]interface{}{123, false, "abc", "extra"}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						0,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
					}, ""},
					{"complex", udtCodecComplex, &[]interface{}{[]interface{}{12, false, "abc"}, []interface{}{34, true, "def"}}, []byte{
						0, 0, 0, 20, // length of element 1
						// element 1
						0, 0, 0, 4, // length of int
						0, 0, 0, 12, // int
						0, 0, 0, 1, // length of boolean
						0,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
						0, 0, 0, 20, // length of element 2
						// element 2
						0, 0, 0, 4, // length of int
						0, 0, 0, 34, // int
						0, 0, 0, 1, // length of boolean
						1,          // boolean
						0, 0, 0, 3, // length of string
						d, e, f, // string
					}, ""},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						if tt.input != nil {
							t.Run("value", func(t *testing.T) {
								dest, err := tt.codec.Encode(*tt.input, version)
								assert.Equal(t, tt.expected, dest)
								assertErrorMessage(t, tt.err, err)
							})
						}
						t.Run("pointer", func(t *testing.T) {
							dest, err := tt.codec.Encode(tt.input, version)
							assert.Equal(t, tt.expected, dest)
							assertErrorMessage(t, tt.err, err)
						})
					})
				}
			})
			t.Run("struct simple", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    *SimpleUdt
					expected []byte
				}{
					{"nil", udtCodecEmpty, nil, nil},
					{"empty", udtCodecSimple, &SimpleUdt{}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 0, // int
						0, 0, 0, 1, // length of boolean
						0,                  // boolean
						255, 255, 255, 255, // nil string
					}},
					{"simple", udtCodecSimple, &SimpleUdt{123, false, stringPtr("abc")}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						0,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
					}},
					{"nil element", udtCodecSimple, &SimpleUdt{123, false, nil}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						0,                  // boolean
						255, 255, 255, 255, // nil string
					}},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						if tt.input != nil {
							t.Run("value", func(t *testing.T) {
								dest, err := tt.codec.Encode(*tt.input, version)
								assert.Equal(t, tt.expected, dest)
								assert.NoError(t, err)

							})
						}
						t.Run("pointer", func(t *testing.T) {
							dest, err := tt.codec.Encode(tt.input, version)
							assert.Equal(t, tt.expected, dest)
							assert.NoError(t, err)
						})
					})
				}
			})
			t.Run("struct partial", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    *partialUdt
					expected []byte
					err      string
				}{
					{"simple", udtCodecSimple, &partialUdt{123, false}, nil, "no accessible field with name 'f3' found"},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						if tt.input != nil {
							t.Run("value", func(t *testing.T) {
								dest, err := tt.codec.Encode(*tt.input, version)
								assert.Equal(t, tt.expected, dest)
								assertErrorMessage(t, tt.err, err)
							})
						}
						t.Run("pointer", func(t *testing.T) {
							dest, err := tt.codec.Encode(tt.input, version)
							assert.Equal(t, tt.expected, dest)
							assertErrorMessage(t, tt.err, err)
						})
					})
				}
			})
			t.Run("struct excess", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    *excessUdt
					expected []byte
				}{
					{"nil", udtCodecEmpty, nil, nil},
					{"empty", udtCodecSimple, &excessUdt{}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 0, // int
						0, 0, 0, 1, // length of boolean
						0,                  // boolean
						255, 255, 255, 255, // nil string
					}},
					{"simple", udtCodecSimple, &excessUdt{123, false, stringPtr("abc"), 42.0}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						0,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
					}},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						if tt.input != nil {
							t.Run("value", func(t *testing.T) {
								dest, err := tt.codec.Encode(*tt.input, version)
								assert.Equal(t, tt.expected, dest)
								assert.NoError(t, err)
							})
						}
						t.Run("pointer", func(t *testing.T) {
							dest, err := tt.codec.Encode(tt.input, version)
							assert.Equal(t, tt.expected, dest)
							assert.NoError(t, err)
						})
					})
				}
			})
			t.Run("struct complex", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    *complexUdt
					expected []byte
				}{
					{"nil", udtCodecEmpty, nil, nil},
					{"empty", udtCodecEmpty, &complexUdt{}, nil},
					{"complex", udtCodecComplex, &complexUdt{
						SimpleUdt{12, false, stringPtr("abc")},
						&excessUdt{34, true, nil, 0.0},
					}, []byte{
						0, 0, 0, 20, // length of element 1
						// element 1
						0, 0, 0, 4, // length of int
						0, 0, 0, 12, // int
						0, 0, 0, 1, // length of boolean
						0,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
						0, 0, 0, 17, // length of element 2
						// element 2
						0, 0, 0, 4, // length of int
						0, 0, 0, 34, // int
						0, 0, 0, 1, // length of boolean
						1,                  // boolean
						255, 255, 255, 255, // nil string
					}},
					{"nil element", udtCodecComplex, &complexUdt{
						SimpleUdt{12, false, stringPtr("abc")},
						nil,
					}, []byte{
						0, 0, 0, 20, // length of element 1
						// element 1
						0, 0, 0, 4, // length of int
						0, 0, 0, 12, // int
						0, 0, 0, 1, // length of boolean
						0,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
						255, 255, 255, 255, // nil element 2
					}},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						if tt.input != nil {
							t.Run("value", func(t *testing.T) {
								dest, err := tt.codec.Encode(*tt.input, version)
								assert.Equal(t, tt.expected, dest)
								assert.NoError(t, err)
							})
						}
						t.Run("pointer", func(t *testing.T) {
							dest, err := tt.codec.Encode(tt.input, version)
							assert.Equal(t, tt.expected, dest)
							assert.NoError(t, err)
						})
					})
				}
			})
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			dest, err := udtCodecSimple.Encode(nil, version)
			assert.Nil(t, dest)
			expectedMessage := fmt.Sprintf("data type %s not supported in %v", udtTypeSimple, version)
			assertErrorMessage(t, expectedMessage, err)
		})
	}
	t.Run("invalid types", func(t *testing.T) {
		dest, err := udtCodecSimple.Encode(123, primitive.ProtocolVersion5)
		assert.Nil(t, dest)
		assert.EqualError(t, err, "cannot encode int as CQL ks1.type1<f1:int,f2:boolean,f3:varchar> with ProtocolVersion OSS 5: source type not supported")
		dest, err = udtCodecSimple.Encode(map[int]string{123: "abc"}, primitive.ProtocolVersion5)
		assert.Nil(t, dest)
		assert.EqualError(t, err, "cannot encode map[int]string as CQL ks1.type1<f1:int,f2:boolean,f3:varchar> with ProtocolVersion OSS 5: wrong map key, expected string, got: 'int'")
	})
}

func Test_udtCodec_Decode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			t.Run("*[]interface{}", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    []byte
					dest     *[]interface{}
					expected *[]interface{}
					err      string
					wasNull  bool
				}{
					{
						"nil input",
						udtCodecSimple,
						nil,
						new([]interface{}),
						new([]interface{}),
						"",
						true,
					},
					{
						"nil elements map to zero values",
						udtCodecSimple,
						[]byte{
							255, 255, 255, 255, // nil int
							255, 255, 255, 255, // nil boolean
							255, 255, 255, 255, // nil string
						},
						new([]interface{}),
						&[]interface{}{nil, nil, nil},
						"",
						false,
					},
					{
						"simple",
						udtCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
						},
						new([]interface{}),
						&[]interface{}{123, true, "abc"},
						"",
						false,
					},
					{
						"complex",
						udtCodecComplex,
						[]byte{
							0, 0, 0, 20, // length of element 1
							// element 1
							0, 0, 0, 4, // length of int
							0, 0, 0, 12, // int
							0, 0, 0, 1, // length of boolean
							0,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
							0, 0, 0, 20, // length of element 2
							// element 2
							0, 0, 0, 4, // length of int
							0, 0, 0, 34, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							d, e, f, // string
						},
						new([]interface{}),
						&[]interface{}{
							map[string]interface{}{"f1": 12, "f2": false, "f3": "abc"},
							map[string]interface{}{"f1": 34, "f2": true, "f3": "def"},
						},
						"",
						false,
					},
					{
						"nil dest",
						udtCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
						},
						nil,
						nil,
						"destination is nil",
						false,
					},
					{
						"not enough bytes",
						udtCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							// missing string
						},
						new([]interface{}),
						&[]interface{}{123, true, nil},
						"cannot read field 2 (f3)",
						false,
					},
					{
						"slice length too large",
						udtCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
						},
						&[]interface{}{nil, nil, nil, 42.0},
						&[]interface{}{123, true, "abc"},
						"",
						false,
					},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						wasNull, err := tt.codec.Decode(tt.input, tt.dest, version)
						if tt.expected != nil && tt.dest != nil {
							assertSlicesEqual(t, *tt.expected, *tt.dest)
						}
						assert.Equal(t, tt.wasNull, wasNull)
						assertErrorMessage(t, tt.err, err)
					})
				}
			})
			t.Run("struct simple", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    []byte
					dest     *SimpleUdt
					expected *SimpleUdt
					err      string
					wasNull  bool
				}{
					{
						"nil input",
						udtCodecSimple,
						nil,
						&SimpleUdt{},
						&SimpleUdt{},
						"",
						true,
					},
					{
						"empty input",
						udtCodecSimple,
						[]byte{},
						&SimpleUdt{},
						&SimpleUdt{},
						"",
						true,
					},
					{
						"nil elements",
						udtCodecSimple,
						[]byte{
							255, 255, 255, 255, // nil int
							255, 255, 255, 255, // nil boolean
							255, 255, 255, 255, // nil string
						},
						&SimpleUdt{},
						&SimpleUdt{
							F1: 0,
							F2: false,
							F3: nil,
						},
						"",
						false,
					},
					{
						"simple",
						udtCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
						},
						&SimpleUdt{},
						&SimpleUdt{
							F1: 123,
							F2: true,
							F3: stringPtr("abc"),
						},
						"",
						false,
					},
					{
						"nil dest",
						udtCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							// missing string
						},
						nil,
						nil,
						"destination is nil",
						false,
					},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						wasNull, err := tt.codec.Decode(tt.input, tt.dest, version)
						if tt.expected != nil && tt.dest != nil {
							assert.Equal(t, *tt.expected, *tt.dest)
						}
						assert.Equal(t, tt.wasNull, wasNull)
						assertErrorMessage(t, tt.err, err)
					})
				}
			})
			t.Run("struct partial", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    []byte
					dest     *partialUdt
					expected *partialUdt
					err      string
					wasNull  bool
				}{
					{
						"simple",
						udtCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
						},
						&partialUdt{},
						&partialUdt{
							F1: 123,
							F2: true,
						},
						"no accessible field with name 'f3' found",
						false,
					},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						wasNull, err := tt.codec.Decode(tt.input, tt.dest, version)
						if tt.expected != nil && tt.dest != nil {
							assert.Equal(t, *tt.expected, *tt.dest)
						}
						assert.Equal(t, tt.wasNull, wasNull)
						assertErrorMessage(t, tt.err, err)
					})
				}
			})
			t.Run("struct excess", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    []byte
					dest     *excessUdt
					expected *excessUdt
					err      string
					wasNull  bool
				}{
					{
						"nil input",
						udtCodecSimple,
						nil,
						&excessUdt{},
						&excessUdt{},
						"",
						true,
					},
					{
						"empty input",
						udtCodecSimple,
						[]byte{},
						&excessUdt{},
						&excessUdt{},
						"",
						true,
					},
					{
						"nil elements",
						udtCodecSimple,
						[]byte{
							255, 255, 255, 255, // nil int
							255, 255, 255, 255, // nil boolean
							255, 255, 255, 255, // nil string
						},
						&excessUdt{},
						&excessUdt{},
						"",
						false,
					},
					{
						"simple",
						udtCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
						},
						&excessUdt{},
						&excessUdt{
							F1: 123,
							F2: true,
							F3: stringPtr("abc"),
						},
						"",
						false,
					},
					{
						"nil dest",
						udtCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							// missing string
						},
						nil,
						nil,
						"destination is nil",
						false,
					},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						wasNull, err := tt.codec.Decode(tt.input, tt.dest, version)
						if tt.expected != nil && tt.dest != nil {
							assert.Equal(t, *tt.expected, *tt.dest)
						}
						assert.Equal(t, tt.wasNull, wasNull)
						assertErrorMessage(t, tt.err, err)
					})
				}
			})
			t.Run("struct complex", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    []byte
					dest     *complexUdt
					expected *complexUdt
					err      string
					wasNull  bool
				}{
					{
						"nil",
						udtCodecComplex,
						nil,
						&complexUdt{},
						&complexUdt{},
						"",
						true,
					},
					{
						"empty",
						udtCodecComplex,
						[]byte{},
						&complexUdt{},
						&complexUdt{},
						"",
						true,
					},
					{
						"complex",
						udtCodecComplex,
						[]byte{
							0, 0, 0, 20, // length of element 1
							// element 1
							0, 0, 0, 4, // length of int
							0, 0, 0, 12, // int
							0, 0, 0, 1, // length of boolean
							0,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
							0, 0, 0, 17, // length of element 2
							// element 2
							0, 0, 0, 4, // length of int
							0, 0, 0, 34, // int
							0, 0, 0, 1, // length of boolean
							1,                  // boolean
							255, 255, 255, 255, // nil string
						}, &complexUdt{},
						&complexUdt{
							SimpleUdt{12, false, stringPtr("abc")},
							&excessUdt{34, true, nil, 0.0},
						},
						"",
						false,
					},
					{
						"nil element",
						udtCodecComplex,
						[]byte{
							0, 0, 0, 20, // length of element 1
							// element 1
							0, 0, 0, 4, // length of int
							0, 0, 0, 12, // int
							0, 0, 0, 1, // length of boolean
							0,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
							255, 255, 255, 255, // nil element 2
						},
						&complexUdt{},
						&complexUdt{
							SimpleUdt{12, false, stringPtr("abc")},
							nil,
						},
						"",
						false,
					},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						wasNull, err := tt.codec.Decode(tt.input, tt.dest, version)
						assert.Equal(t, *tt.expected, *tt.dest)
						assert.Equal(t, tt.wasNull, wasNull)
						assertErrorMessage(t, tt.err, err)
					})
				}
			})
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			_, err := udtCodecSimple.Decode(nil, nil, version)
			expectedMessage := fmt.Sprintf("data type %s not supported in %v", udtTypeSimple, version)
			assertErrorMessage(t, expectedMessage, err)
		})
	}
	t.Run("invalid types", func(t *testing.T) {
		wasNull, err := udtCodecSimple.Decode([]byte{1, 2, 3}, new(int), primitive.ProtocolVersion5)
		assert.False(t, wasNull)
		assert.EqualError(t, err, "cannot decode CQL ks1.type1<f1:int,f2:boolean,f3:varchar> as *int with ProtocolVersion OSS 5: destination type not supported")
		wasNull, err = udtCodecSimple.Decode([]byte{1, 2, 3}, new(map[int]string), primitive.ProtocolVersion5)
		assert.False(t, wasNull)
		assert.EqualError(t, err, "cannot decode CQL ks1.type1<f1:int,f2:boolean,f3:varchar> as *map[int]string with ProtocolVersion OSS 5: wrong map key, expected string, got: 'int'")
	})
}

func Test_writeUdt(t *testing.T) {
	type args struct {
		ext         extractor
		fieldNames  []string
		fieldCodecs []Codec
		version     primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr string
	}{
		{
			"cannot extract elem",
			args{
				func() extractor {
					ext := &mockExtractor{}
					ext.On("getElem", 0, "f1").Return(nil, errors.New("wrong type"))
					return ext
				}(),
				[]string{"f1"},
				[]Codec{nil},
				primitive.ProtocolVersion5,
			},
			nil,
			"cannot extract field 0 (f1): wrong type",
		},
		{
			"cannot encode",
			args{
				func() extractor {
					ext := &mockExtractor{}
					ext.On("getElem", 0, "f1").Return(123, nil)
					return ext
				}(),
				[]string{"f1"},
				func() []Codec {
					codec := &mockCodec{}
					codec.On("Encode", 123, primitive.ProtocolVersion5).Return(nil, errors.New("write failed"))
					return []Codec{codec}
				}(),
				primitive.ProtocolVersion5,
			},
			nil,
			"cannot encode field 0 (f1): write failed",
		},
		{"success", args{
			func() extractor {
				ext := &mockExtractor{}
				ext.On("getElem", 0, "f1").Return(123, nil)
				ext.On("getElem", 1, "f2").Return("abc", nil)
				ext.On("getElem", 2, "f3").Return(true, nil)
				return ext
			}(),
			[]string{"f1", "f2", "f3"},
			func() []Codec {
				codec1 := &mockCodec{}
				codec1.On("Encode", 123, primitive.ProtocolVersion5).Return([]byte{1}, nil)
				codec2 := &mockCodec{}
				codec2.On("Encode", "abc", primitive.ProtocolVersion5).Return([]byte{2}, nil)
				codec3 := &mockCodec{}
				codec3.On("Encode", true, primitive.ProtocolVersion5).Return(nil, nil)
				return []Codec{codec1, codec2, codec3}
			}(),
			primitive.ProtocolVersion5,
		}, []byte{
			0, 0, 0, 1, // field 1
			1,
			0, 0, 0, 1, // field 2
			2,
			255, 255, 255, 255, // field 3 (nil)
		}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := writeUdt(tt.args.ext, tt.args.fieldNames, tt.args.fieldCodecs, tt.args.version)
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_readUdt(t *testing.T) {
	type args struct {
		source      []byte
		inj         injector
		fieldNames  []string
		fieldCodecs []Codec
		version     primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		args    args
		wantErr string
	}{
		{
			"cannot read element",
			args{
				[]byte{
					0, // wrong [bytes]
				},
				nil,
				[]string{"f1"},
				[]Codec{nil},
				primitive.ProtocolVersion5,
			},
			"cannot read field 0 (f1): cannot read [bytes] length: cannot read [int]: unexpected EOF",
		},
		{
			"cannot create element",
			args{
				[]byte{
					0, 0, 0, 1, 123, // [bytes]
				},
				func() injector {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, "f1", datatype.Int).Return(nil, errors.New("wrong data type"))
					return inj
				}(),
				[]string{"f1"},
				func() []Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					return []Codec{codec}
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot create zero field 0 (f1): wrong data type",
		},
		{
			"cannot decode element",
			args{
				[]byte{
					0, 0, 0, 1, 123, // [bytes]
				},
				func() injector {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, "f1", datatype.Int).Return(new(int), nil)
					return inj
				}(),
				[]string{"f1"},
				func() []Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{123}, new(int), primitive.ProtocolVersion5).Return(false, errors.New("decode failed"))
					return []Codec{codec}
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot decode field 0 (f1): decode failed",
		},
		{
			"cannot set element",
			args{
				[]byte{
					0, 0, 0, 1, 123, // [bytes]
				},
				func() injector {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, "f1", datatype.Int).Return(new(int), nil)
					inj.On("setElem", 0, "f1", intPtr(123), false, false, nil, datatype.Int).Return(errors.New("cannot set elem"))
					return inj
				}(),
				[]string{"f1"},
				func() []Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{123}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 123
					}).Return(false, nil)
					return []Codec{codec}
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot inject field 0 (f1): cannot set elem",
		},
		{
			"bytes remaining",
			args{
				[]byte{
					0, 0, 0, 1, 123, // [bytes]
					1, // trailing bytes
				},
				func() injector {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, "f1", datatype.Int).Return(new(int), nil)
					inj.On("setElem", 0, "f1", intPtr(123), false, false, nil, datatype.Int).Return(nil)
					return inj
				}(),
				[]string{"f1"},
				func() []Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{123}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 123
					}).Return(false, nil)
					return []Codec{codec}
				}(),
				primitive.ProtocolVersion5,
			},
			"source was not fully read: bytes total: 6, read: 5, remaining: 1",
		},
		{
			"success",
			args{
				[]byte{
					0, 0, 0, 1, 123, // 1st elem
					0, 0, 0, 3, a, b, c, // 2nd elem
					255, 255, 255, 255, // 3rd elem (nil)
				},
				func() injector {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, "f1", datatype.Int).Return(new(int), nil)
					inj.On("zeroElem", 1, "f2", datatype.Varchar).Return(new(string), nil)
					inj.On("zeroElem", 2, "f3", datatype.Boolean).Return(new(bool), nil)
					inj.On("setElem", 0, "f1", intPtr(123), false, false, nil, datatype.Int).Return(nil)
					inj.On("setElem", 1, "f2", stringPtr("abc"), false, false, nil, datatype.Varchar).Return(nil)
					inj.On("setElem", 2, "f3", new(bool), false, true, nil, datatype.Boolean).Return(nil)
					return inj
				}(),
				[]string{"f1", "f2", "f3"},
				func() []Codec {
					codec1 := &mockCodec{}
					codec1.On("DataType").Return(datatype.Int)
					codec1.On("Decode", []byte{123}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 123
					}).Return(false, nil)
					codec2 := &mockCodec{}
					codec2.On("DataType").Return(datatype.Varchar)
					codec2.On("Decode", []byte{a, b, c}, new(string), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*string)
						*decodedElement = "abc"
					}).Return(false, nil)
					codec3 := &mockCodec{}
					codec3.On("DataType").Return(datatype.Boolean)
					codec3.On("Decode", []byte(nil), new(bool), primitive.ProtocolVersion5).Return(true, nil)
					return []Codec{codec1, codec2, codec3}
				}(),
				primitive.ProtocolVersion5,
			},
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := readUdt(tt.args.source, tt.args.inj, tt.args.fieldNames, tt.args.fieldCodecs, tt.args.version)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}
