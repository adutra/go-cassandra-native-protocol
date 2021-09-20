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
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type (
	SimpleTuple struct {
		Int    int
		Bool   bool
		String *string
	}
	partialTuple struct {
		Int  int
		Bool bool
	}
	excessTuple struct {
		Int    int
		Bool   bool
		String *string
		Float  float64
	}
	complexTuple struct {
		SimpleTuple
		Element2 *excessTuple
	}
)

var (
	tupleCodecEmpty, _   = NewTuple(datatype.NewTupleType())
	tupleCodecSimple, _  = NewTuple(datatype.NewTupleType(datatype.Int, datatype.Boolean, datatype.Varchar))
	tupleCodecInts, _    = NewTuple(datatype.NewTupleType(datatype.Int, datatype.Int, datatype.Int)) // can be mapped to []int
	tupleCodecComplex, _ = NewTuple(datatype.NewTupleType(tupleCodecSimple.DataType(), tupleCodecSimple.DataType()))
)

func TestNewTupleCodec(t *testing.T) {
	tests := []struct {
		name     string
		dataType datatype.TupleType
		expected Codec
		err      string
	}{
		{
			"simple",
			datatype.NewTupleType(datatype.Int, datatype.Varchar),
			&tupleCodec{
				dataType:      datatype.NewTupleType(datatype.Int, datatype.Varchar),
				elementCodecs: []Codec{Int, Varchar},
			},
			"",
		},
		{
			"complex",
			datatype.NewTupleType(datatype.Int, datatype.NewTupleType(datatype.Int, datatype.Varchar)),
			&tupleCodec{
				dataType: datatype.NewTupleType(datatype.Int, datatype.NewTupleType(datatype.Int, datatype.Varchar)),
				elementCodecs: []Codec{Int, &tupleCodec{
					dataType:      datatype.NewTupleType(datatype.Int, datatype.Varchar),
					elementCodecs: []Codec{Int, Varchar},
				}},
			},
			"",
		},
		{
			"empty",
			datatype.NewTupleType(),
			&tupleCodec{
				dataType:      datatype.NewTupleType(),
				elementCodecs: []Codec{},
			},
			"",
		},
		{
			"wrong child",
			datatype.NewTupleType(wrongDataType{}),
			nil,
			"cannot create codec for tuple element 0: cannot create data codec for CQL type 666",
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
			actual, err := NewTuple(tt.dataType)
			assert.Equal(t, tt.expected, actual)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_tupleCodec_Encode(t *testing.T) {
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
					{"nil", tupleCodecEmpty, nil, nil, ""},
					{"empty", tupleCodecSimple, &[]interface{}{nil, nil, nil}, []byte{
						255, 255, 255, 255, // nil int
						255, 255, 255, 255, // nil boolean
						255, 255, 255, 255, // nil string
					}, ""},
					{"simple", tupleCodecSimple, &[]interface{}{123, true, "abc"}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						1,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
					}, ""},
					{"simple with pointers", tupleCodecSimple, &[]interface{}{intPtr(123), boolPtr(true), stringPtr("abc")}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						1,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
					}, ""},
					{"nil element", tupleCodecSimple, &[]interface{}{123, false, nil}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						0,                  // boolean
						255, 255, 255, 255, // nil string
					}, ""},
					{"not enough elements", tupleCodecSimple, &[]interface{}{123}, nil, "slice index out of range: 1"},
					{"too many elements", tupleCodecSimple, &[]interface{}{123, false, "abc", "extra"}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						0,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
					}, ""},
					{"complex", tupleCodecComplex, &[]interface{}{[]interface{}{12, false, "abc"}, []interface{}{34, true, "def"}}, []byte{
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
					input    *SimpleTuple
					expected []byte
				}{
					{"nil", tupleCodecEmpty, nil, nil},
					{"empty", tupleCodecSimple, &SimpleTuple{}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 0, // int
						0, 0, 0, 1, // length of boolean
						0,                  // boolean
						255, 255, 255, 255, // nil string
					}},
					{"simple", tupleCodecSimple, &SimpleTuple{123, false, stringPtr("abc")}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 123, // int
						0, 0, 0, 1, // length of boolean
						0,          // boolean
						0, 0, 0, 3, // length of string
						a, b, c, // string
					}},
					{"nil element", tupleCodecSimple, &SimpleTuple{123, false, nil}, []byte{
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
					input    *partialTuple
					expected []byte
					err      string
				}{
					{"simple", tupleCodecSimple, &partialTuple{123, false}, nil, "no accessible field with index 2 found"},
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
					input    *excessTuple
					expected []byte
				}{
					{"nil", tupleCodecEmpty, nil, nil},
					{"empty", tupleCodecSimple, &excessTuple{}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 0, // int
						0, 0, 0, 1, // length of boolean
						0,                  // boolean
						255, 255, 255, 255, // nil string
					}},
					{"simple", tupleCodecSimple, &excessTuple{123, false, stringPtr("abc"), 42.0}, []byte{
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
					input    *complexTuple
					expected []byte
				}{
					{"nil", tupleCodecEmpty, nil, nil},
					{"empty", tupleCodecEmpty, &complexTuple{}, nil},
					{"complex", tupleCodecComplex, &complexTuple{
						SimpleTuple{12, false, stringPtr("abc")},
						&excessTuple{34, true, nil, 0.0},
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
					{"nil element", tupleCodecComplex, &complexTuple{
						SimpleTuple{12, false, stringPtr("abc")},
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
			t.Run("[3]int", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    *[3]int
					expected []byte
				}{
					{"nil", tupleCodecInts, nil, nil},
					{"non nil", tupleCodecInts, &[3]int{1, 2, 3}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 1, // int
						0, 0, 0, 4, // length of int
						0, 0, 0, 2, // int
						0, 0, 0, 4, // length of int
						0, 0, 0, 3, // int
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
			t.Run("[3]*int", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    *[3]*int
					expected []byte
				}{
					{"nil", tupleCodecInts, nil, nil},
					{"nil element", tupleCodecInts, &[3]*int{intPtr(1), nil, nil}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 1, // int
						255, 255, 255, 255, // nil int
						255, 255, 255, 255, // nil int
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
			t.Run("[]int", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    *[]int
					expected []byte
					err      string
				}{
					{"nil", tupleCodecInts, nil, nil, ""},
					{"non nil", tupleCodecInts, &[]int{1, 2, 3}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 1, // int
						0, 0, 0, 4, // length of int
						0, 0, 0, 2, // int
						0, 0, 0, 4, // length of int
						0, 0, 0, 3, // int
					}, ""},
					{"not enough elements", tupleCodecInts, &[]int{1}, nil, "slice index out of range: 1"},
					{"too many elements", tupleCodecInts, &[]int{1, 2, 3, 4, 5}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 1, // int
						0, 0, 0, 4, // length of int
						0, 0, 0, 2, // int
						0, 0, 0, 4, // length of int
						0, 0, 0, 3, // int
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
			t.Run("[]*int", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    *[]*int
					expected []byte
					err      string
				}{
					{"nil", tupleCodecInts, nil, nil, ""},
					{"nil element", tupleCodecInts, &[]*int{intPtr(1), intPtr(2), nil}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 1, // int
						0, 0, 0, 4, // length of int
						0, 0, 0, 2, // int
						255, 255, 255, 255, // nil int
					}, ""},
					{"not enough elements", tupleCodecInts, &[]*int{intPtr(1)}, nil, "slice index out of range: 1"},
					{"too many elements", tupleCodecInts, &[]*int{intPtr(1), nil, intPtr(3), intPtr(4)}, []byte{
						0, 0, 0, 4, // length of int
						0, 0, 0, 1, // int
						255, 255, 255, 255, // nil int
						0, 0, 0, 4, // length of int
						0, 0, 0, 3, // int
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
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			codec, _ := NewTuple(datatype.NewTupleType(datatype.Int))
			dest, err := codec.Encode(nil, version)
			assert.Nil(t, dest)
			assertErrorMessage(t, "data type tuple<int> not supported in "+version.String(), err)
		})
	}
	t.Run("invalid types", func(t *testing.T) {
		dest, err := tupleCodecSimple.Encode(123, primitive.ProtocolVersion5)
		assert.Nil(t, dest)
		assert.EqualError(t, err, "cannot encode int as CQL tuple<int,boolean,varchar> with ProtocolVersion OSS 5: source type not supported")
	})
}

func Test_tupleCodec_Decode(t *testing.T) {
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
						tupleCodecSimple,
						nil,
						new([]interface{}),
						new([]interface{}),
						"",
						true,
					},
					{
						"nil elements map to zero values",
						tupleCodecSimple,
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
						tupleCodecSimple,
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
						tupleCodecComplex,
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
							[]interface{}{12, false, "abc"},
							[]interface{}{34, true, "def"},
						},
						"",
						false,
					},
					{
						"nil dest",
						tupleCodecSimple,
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
						tupleCodecSimple,
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
						"cannot read element 2",
						false,
					},
					{
						"slice length too large",
						tupleCodecSimple,
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
					dest     *SimpleTuple
					expected *SimpleTuple
					err      string
					wasNull  bool
				}{
					{
						"nil input",
						tupleCodecSimple,
						nil,
						&SimpleTuple{},
						&SimpleTuple{},
						"",
						true,
					},
					{
						"empty input",
						tupleCodecSimple,
						[]byte{},
						&SimpleTuple{},
						&SimpleTuple{},
						"",
						true,
					},
					{
						"nil elements",
						tupleCodecSimple,
						[]byte{
							255, 255, 255, 255, // nil int
							255, 255, 255, 255, // nil boolean
							255, 255, 255, 255, // nil string
						},
						&SimpleTuple{},
						&SimpleTuple{
							Int:    0,
							Bool:   false,
							String: nil,
						},
						"",
						false,
					},
					{
						"simple",
						tupleCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
						},
						&SimpleTuple{},
						&SimpleTuple{
							Int:    123,
							Bool:   true,
							String: stringPtr("abc"),
						},
						"",
						false,
					},
					{
						"nil dest",
						tupleCodecSimple,
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
					dest     *partialTuple
					expected *partialTuple
					err      string
					wasNull  bool
				}{
					{
						"simple",
						tupleCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
						},
						&partialTuple{},
						&partialTuple{
							Int:  123,
							Bool: true,
						},
						"no accessible field with index 2 found",
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
					dest     *excessTuple
					expected *excessTuple
					err      string
					wasNull  bool
				}{
					{
						"nil input",
						tupleCodecSimple,
						nil,
						&excessTuple{},
						&excessTuple{},
						"",
						true,
					},
					{
						"empty input",
						tupleCodecSimple,
						[]byte{},
						&excessTuple{},
						&excessTuple{},
						"",
						true,
					},
					{
						"nil elements",
						tupleCodecSimple,
						[]byte{
							255, 255, 255, 255, // nil int
							255, 255, 255, 255, // nil boolean
							255, 255, 255, 255, // nil string
						},
						&excessTuple{},
						&excessTuple{},
						"",
						false,
					},
					{
						"simple",
						tupleCodecSimple,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 123, // int
							0, 0, 0, 1, // length of boolean
							1,          // boolean
							0, 0, 0, 3, // length of string
							a, b, c, // string
						},
						&excessTuple{},
						&excessTuple{
							Int:    123,
							Bool:   true,
							String: stringPtr("abc"),
						},
						"",
						false,
					},
					{
						"nil dest",
						tupleCodecSimple,
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
					dest     *complexTuple
					expected *complexTuple
					err      string
					wasNull  bool
				}{
					{
						"nil",
						tupleCodecComplex,
						nil,
						&complexTuple{},
						&complexTuple{},
						"",
						true,
					},
					{
						"empty",
						tupleCodecComplex,
						[]byte{},
						&complexTuple{},
						&complexTuple{},
						"",
						true,
					},
					{
						"complex",
						tupleCodecComplex,
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
						}, &complexTuple{},
						&complexTuple{
							SimpleTuple{12, false, stringPtr("abc")},
							&excessTuple{34, true, nil, 0.0},
						},
						"",
						false,
					},
					{
						"nil element",
						tupleCodecComplex,
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
						&complexTuple{},
						&complexTuple{
							SimpleTuple{12, false, stringPtr("abc")},
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
			t.Run("[3]int", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    []byte
					expected *[3]int
					dest     *[3]int
					err      string
					wasNull  bool
				}{
					{"nil", tupleCodecInts, nil, new([3]int), new([3]int), "", true},
					{
						"non nil",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
						},
						&[3]int{1, 2, 3},
						new([3]int),
						"",
						false,
					},
					{
						"nil dest",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
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
						assert.Equal(t, tt.expected, tt.dest)
						assert.Equal(t, tt.wasNull, wasNull)
						assertErrorMessage(t, tt.err, err)
					})
				}
			})
			t.Run("[3]*int", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    []byte
					expected *[3]*int
					dest     *[3]*int
					err      string
					wasNull  bool
				}{
					{"nil", tupleCodecInts, nil, new([3]*int), new([3]*int), "", true},
					{
						"non nil",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
						},
						&[3]*int{intPtr(1), intPtr(2), intPtr(3)},
						new([3]*int),
						"",
						false,
					},
					{
						"nil dest",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
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
						assert.Equal(t, tt.expected, tt.dest)
						assert.Equal(t, tt.wasNull, wasNull)
						assertErrorMessage(t, tt.err, err)
					})
				}
			})
			t.Run("[4]int", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    []byte
					expected *[4]int
					dest     *[4]int
					err      string
					wasNull  bool
				}{
					{"nil", tupleCodecInts, nil, new([4]int), new([4]int), "", true},
					{
						"non nil",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
						},
						&[4]int{1, 2, 3, 0},
						new([4]int),
						"",
						false,
					},
					{
						"nil dest",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
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
						assert.Equal(t, tt.expected, tt.dest)
						assert.Equal(t, tt.wasNull, wasNull)
						assertErrorMessage(t, tt.err, err)
					})
				}
			})
			t.Run("[2]int", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    []byte
					expected *[2]int
					dest     *[2]int
					err      string
					wasNull  bool
				}{
					{
						"non nil",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
						},
						&[2]int{1, 2},
						new([2]int),
						"array index out of range: 2",
						false,
					},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						wasNull, err := tt.codec.Decode(tt.input, tt.dest, version)
						assert.Equal(t, tt.expected, tt.dest)
						assert.Equal(t, tt.wasNull, wasNull)
						assertErrorMessage(t, tt.err, err)
					})
				}
			})
			t.Run("[]int", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    []byte
					expected *[]int
					dest     *[]int
					err      string
					wasNull  bool
				}{
					{"nil", tupleCodecInts, nil, new([]int), new([]int), "", true},
					{
						"non nil",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
						},
						&[]int{1, 2, 3},
						new([]int),
						"",
						false,
					},
					{
						"not enough elements",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							// missing int
						},
						&[]int{1, 2, 0},
						new([]int),
						"cannot read element 2",
						false,
					},
					{
						"slice length too small",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
						},
						&[]int{1, 2, 3},
						new([]int),
						"",
						false,
					},
					{
						"slice length too large",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
						},
						&[]int{1, 2, 3},
						&[]int{0, 0, 0, 0},
						"",
						false,
					},
					{
						"nil dest",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
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
						assert.Equal(t, tt.expected, tt.dest)
						assert.Equal(t, tt.wasNull, wasNull)
						assertErrorMessage(t, tt.err, err)
					})
				}
			})
			t.Run("[]*int", func(t *testing.T) {
				tests := []struct {
					name     string
					codec    Codec
					input    []byte
					expected *[]*int
					dest     *[]*int
					err      string
					wasNull  bool
				}{
					{"nil", tupleCodecInts, nil, new([]*int), new([]*int), "", true},
					{
						"non nil",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
						},
						&[]*int{intPtr(1), intPtr(2), intPtr(3)},
						new([]*int),
						"",
						false,
					},
					{
						"nil dest",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
						},
						nil,
						nil,
						"destination is nil",
						false,
					},
					{
						"slice length too small",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
						},
						&[]*int{intPtr(1), intPtr(2), intPtr(3)},
						&[]*int{intPtr(1), nil},
						"",
						false,
					},
					{
						"slice length too large",
						tupleCodecInts,
						[]byte{
							0, 0, 0, 4, // length of int
							0, 0, 0, 1, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 2, // int
							0, 0, 0, 4, // length of int
							0, 0, 0, 3, // int
						},
						&[]*int{intPtr(1), intPtr(2), intPtr(3)},
						&[]*int{intPtr(0), intPtr(0), intPtr(0), intPtr(0)},
						"",
						false,
					},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						wasNull, err := tt.codec.Decode(tt.input, tt.dest, version)
						assert.Equal(t, tt.expected, tt.dest)
						assert.Equal(t, tt.wasNull, wasNull)
						assertErrorMessage(t, tt.err, err)
					})
				}
			})
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			codec, _ := NewTuple(datatype.NewTupleType(datatype.Int))
			_, err := codec.Decode(nil, nil, version)
			assertErrorMessage(t, "data type tuple<int> not supported in "+version.String(), err)
		})
	}
	t.Run("invalid types", func(t *testing.T) {
		wasNull, err := tupleCodecSimple.Decode([]byte{1, 2, 3}, new(int), primitive.ProtocolVersion5)
		assert.False(t, wasNull)
		assert.EqualError(t, err, "cannot decode CQL tuple<int,boolean,varchar> as *int with ProtocolVersion OSS 5: destination type not supported")
	})
}

func Test_writeTuple(t *testing.T) {
	type args struct {
		ext           extractor
		elementCodecs []Codec
		version       primitive.ProtocolVersion
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
					ext.On("getElem", 0, 0).Return(nil, errSliceIndexOutOfRange("slice", 0))
					return ext
				}(),
				[]Codec{nil},
				primitive.ProtocolVersion5,
			},
			nil,
			"cannot extract element 0: slice index out of range: 0",
		},
		{
			"cannot encode",
			args{
				func() extractor {
					ext := &mockExtractor{}
					ext.On("getElem", 0, 0).Return(123, nil)
					return ext
				}(),
				func() []Codec {
					codec := &mockCodec{}
					codec.On("Encode", 123, primitive.ProtocolVersion5).Return(nil, errors.New("write failed"))
					return []Codec{codec}
				}(),
				primitive.ProtocolVersion5,
			},
			nil,
			"cannot encode element 0: write failed",
		},
		{"success", args{
			func() extractor {
				ext := &mockExtractor{}
				ext.On("getElem", 0, 0).Return(123, nil)
				ext.On("getElem", 1, 1).Return("abc", nil)
				ext.On("getElem", 2, 2).Return(true, nil)
				return ext
			}(),
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
			0, 0, 0, 1, // elem 1
			1,
			0, 0, 0, 1, // elem 2
			2,
			255, 255, 255, 255, // elem 3 (nil)
		}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := writeTuple(tt.args.ext, tt.args.elementCodecs, tt.args.version)
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_readTuple(t *testing.T) {
	type args struct {
		source        []byte
		inj           injector
		elementCodecs []Codec
		version       primitive.ProtocolVersion
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
				[]Codec{nil},
				primitive.ProtocolVersion5,
			},
			"cannot read element 0: cannot read [bytes] length: cannot read [int]: unexpected EOF",
		},
		{
			"cannot create element",
			args{
				[]byte{
					0, 0, 0, 1, 123, // [bytes]
				},
				func() injector {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, 0, datatype.Int).Return(nil, errors.New("wrong data type"))
					return inj
				}(),
				func() []Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					return []Codec{codec}
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot create zero element 0: wrong data type",
		},
		{
			"cannot decode element",
			args{
				[]byte{
					0, 0, 0, 1, 123, // [bytes]
				},
				func() injector {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, 0, datatype.Int).Return(new(int), nil)
					return inj
				}(),
				func() []Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{123}, new(int), primitive.ProtocolVersion5).Return(false, errors.New("decode failed"))
					return []Codec{codec}
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot decode element 0: decode failed",
		},
		{
			"cannot set element",
			args{
				[]byte{
					0, 0, 0, 1, 123, // [bytes]
				},
				func() injector {
					inj := &mockInjector{}
					inj.On("zeroElem", 0, 0, datatype.Int).Return(new(int), nil)
					inj.On("setElem", 0, 0, intPtr(123), false, false, nil, datatype.Int).Return(errors.New("cannot set elem"))
					return inj
				}(),
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
			"cannot inject element 0: cannot set elem",
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
					inj.On("zeroElem", 0, 0, datatype.Int).Return(new(int), nil)
					inj.On("setElem", 0, 0, intPtr(123), false, false, nil, datatype.Int).Return(nil)
					return inj
				}(),
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
					inj.On("zeroElem", 0, 0, datatype.Int).Return(new(int), nil)
					inj.On("zeroElem", 1, 1, datatype.Varchar).Return(new(string), nil)
					inj.On("zeroElem", 2, 2, datatype.Boolean).Return(new(bool), nil)
					inj.On("setElem", 0, 0, intPtr(123), false, false, nil, datatype.Int).Return(nil)
					inj.On("setElem", 1, 1, stringPtr("abc"), false, false, nil, datatype.Varchar).Return(nil)
					inj.On("setElem", 2, 2, new(bool), false, true, nil, datatype.Boolean).Return(nil)
					return inj
				}(),
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
			gotErr := readTuple(tt.args.source, tt.args.inj, tt.args.elementCodecs, tt.args.version)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}
