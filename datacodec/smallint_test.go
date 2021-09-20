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
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"math"
	"strconv"
	"testing"
)

var (
	smallintZero     = encodeUint16(0x0000)
	smallintOne      = encodeUint16(0x0001)
	smallintMinusOne = encodeUint16(0xffff)
	smallintMaxInt16 = encodeUint16(0x7fff)
	smallintMinInt16 = encodeUint16(0x8000)
)

func Test_smallintCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", int16NilPtr(), nil, ""},
				{"non nil", 1, smallintOne, ""},
				{"conversion failed", uint16(math.MaxUint16), nil, fmt.Sprintf("cannot encode uint16 as CQL smallint with %v: cannot assign uint16 to int16: value out of range: 65535", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Smallint.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", int16NilPtr(), nil, "data type smallint not supported"},
				{"non nil", 1, nil, "data type smallint not supported"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Smallint.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_smallintCodec_Decode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				dest     interface{}
				expected interface{}
				wasNull  bool
				err      string
			}{
				{"null", nil, new(int16), new(int16), true, ""},
				{"non null", smallintOne, new(int16), int16Ptr(1), false, ""},
				{"read failed", []byte{1}, new(int16), new(int16), false, fmt.Sprintf("cannot decode CQL smallint as *int16 with %v: cannot read int16: expected 2 bytes but got: 1", version)},
				{"conversion failed", smallintOne, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL smallint as *float64 with %v: cannot store int16 into *float64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Smallint.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion4) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				dest     interface{}
				expected interface{}
				wasNull  bool
				err      string
			}{
				{"null", nil, new(int16), new(int16), true, "data type smallint not supported"},
				{"non null", smallintOne, new(int16), new(int16), false, "data type smallint not supported"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Smallint.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_convertToInt16(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int16
		wasNil   bool
		err      string
	}{
		{"from int", int(1), 1, false, ""},
		{"from int out of range pos", int(math.MaxInt16 + 1), 0, false, "cannot assign int to int16: value out of range: 32768"},
		{"from int out of range neg", int(math.MinInt16 - 1), 0, false, "cannot assign int to int16: value out of range: -32769"},
		{"from *int non nil", intPtr(1), 1, false, ""},
		{"from *int nil", intNilPtr(), 0, true, ""},
		{"from *int out of range pos", intPtr(math.MaxInt16 + 1), 0, false, "cannot assign *int to int16: value out of range: 32768"},
		{"from *int out of range neg", intPtr(math.MinInt16 - 1), 0, false, "cannot assign *int to int16: value out of range: -32769"},
		{"from int64", int64(1), 1, false, ""},
		{"from int64 out of range pos", int64(math.MaxInt16 + 1), 0, false, "cannot assign int64 to int16: value out of range: 32768"},
		{"from int64 out of range neg", int64(math.MinInt16 - 1), 0, false, "cannot assign int64 to int16: value out of range: -32769"},
		{"from *int64 non nil", int64Ptr(1), 1, false, ""},
		{"from *int64 nil", int64NilPtr(), 0, true, ""},
		{"from *int64 out of range pos", int64Ptr(math.MaxInt16 + 1), 0, false, "cannot assign *int64 to int16: value out of range: 32768"},
		{"from *int64 out of range neg", int64Ptr(math.MinInt16 - 1), 0, false, "cannot assign *int64 to int16: value out of range: -32769"},
		{"from int32", int32(1), 1, false, ""},
		{"from int32 out of range pos", int32(math.MaxInt16 + 1), 0, false, "cannot assign int32 to int16: value out of range: 32768"},
		{"from int32 out of range neg", int32(math.MinInt16 - 1), 0, false, "cannot assign int32 to int16: value out of range: -32769"},
		{"from *int32 non nil", int32Ptr(1), 1, false, ""},
		{"from *int32 nil", int32NilPtr(), 0, true, ""},
		{"from *int32 out of range pos", int32Ptr(math.MaxInt16 + 1), 0, false, "cannot assign *int32 to int16: value out of range: 32768"},
		{"from *int32 out of range neg", int32Ptr(math.MinInt16 - 1), 0, false, "cannot assign *int32 to int16: value out of range: -32769"},
		{"from int16", int16(1), 1, false, ""},
		{"from *int16 non nil", int16Ptr(1), 1, false, ""},
		{"from *int16 nil", int16NilPtr(), 0, true, ""},
		{"from int8", int8(1), 1, false, ""},
		{"from *int8 non nil", int8Ptr(1), 1, false, ""},
		{"from *int8 nil", int8NilPtr(), 0, true, ""},
		{"from uint", uint(1), 1, false, ""},
		{"from uint out of range", uint(math.MaxInt16 + 1), 0, false, "cannot assign uint to int16: value out of range: 32768"},
		{"from *uint non nil", uintPtr(1), 1, false, ""},
		{"from *uint nil", uintNilPtr(), 0, true, ""},
		{"from *uint out of range", uintPtr(math.MaxInt16 + 1), 0, false, "cannot assign *uint to int16: value out of range: 32768"},
		{"from uint64", uint64(1), 1, false, ""},
		{"from uint64 out of range", uint64(math.MaxInt16 + 1), 0, false, "cannot assign uint64 to int16: value out of range: 32768"},
		{"from *uint64 non nil", uint64Ptr(1), 1, false, ""},
		{"from *uint64 nil", uint64NilPtr(), 0, true, ""},
		{"from *uint64 out of range", uint64Ptr(math.MaxInt16 + 1), 0, false, "cannot assign *uint64 to int16: value out of range: 32768"},
		{"from uint32", uint32(1), 1, false, ""},
		{"from uint32 out of range", uint32(math.MaxInt16 + 1), 0, false, "cannot assign uint32 to int16: value out of range: 32768"},
		{"from *uint32 non nil", uint32Ptr(1), 1, false, ""},
		{"from *uint32 nil", uint32NilPtr(), 0, true, ""},
		{"from *uint32 out of range", uint32Ptr(math.MaxInt16 + 1), 0, false, "cannot assign *uint32 to int16: value out of range: 32768"},
		{"from uint16", uint16(1), 1, false, ""},
		{"from uint16 out of range", uint16(math.MaxInt16 + 1), 0, false, "cannot assign uint16 to int16: value out of range: 32768"},
		{"from *uint16 non nil", uint16Ptr(1), 1, false, ""},
		{"from *uint16 nil", uint16NilPtr(), 0, true, ""},
		{"from *uint16 out of range", uint16Ptr(math.MaxInt16 + 1), 0, false, "cannot assign *uint16 to int16: value out of range: 32768"},
		{"from uint8", uint8(1), 1, false, ""},
		{"from *uint8 non nil", uint8Ptr(1), 1, false, ""},
		{"from *uint8 nil", uint8NilPtr(), 0, true, ""},
		{"from string", "1", 1, false, ""},
		{"from string malformed", "not a number", 0, false, "cannot assign string to int16: cannot parse 'not a number'"},
		{"from string out of range", strconv.Itoa(math.MaxInt16 + 1), 0, false, "cannot assign string to int16: cannot parse '32768'"},
		{"from *string non nil", stringPtr("1"), 1, false, ""},
		{"from *string malformed", stringPtr("not a number"), 0, false, "cannot assign *string to int16: cannot parse 'not a number'"},
		{"from *string out of range", stringPtr(strconv.Itoa(math.MaxInt16 + 1)), 0, false, "cannot assign *string to int16: cannot parse '32768'"},
		{"from *string nil", stringNilPtr(), 0, true, ""},
		{"from untyped nil", nil, 0, true, ""},
		{"from unsupported value type", 42.0, 0, false, "cannot assign float64 to int16: conversion not supported"},
		{"from unsupported pointer type", float64Ptr(42.0), 0, false, "cannot assign *float64 to int16: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest, wasNil, err := convertToInt16(tt.input)
			assert.Equal(t, tt.expected, dest)
			assert.Equal(t, tt.wasNil, wasNil)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_convertFromInt16(t *testing.T) {
	tests := []struct {
		name     string
		val      int16
		wasNull  bool
		dest     interface{}
		expected interface{}
		err      string
	}{
		{"to *int nil dest", 1, false, intNilPtr(), intNilPtr(), "cannot store int16 into *int: destination is nil"},
		{"to *int nil source", 0, true, new(int), intPtr(0), ""},
		{"to *int non nil", 1, false, new(int), intPtr(1), ""},
		{"to *int64 nil dest", 1, false, int64NilPtr(), int64NilPtr(), "cannot store int16 into *int64: destination is nil"},
		{"to *int64 nil source", 0, true, new(int64), int64Ptr(0), ""},
		{"to *int64 non nil", 1, false, new(int64), int64Ptr(1), ""},
		{"to *int32 nil dest", 1, false, int32NilPtr(), int32NilPtr(), "cannot store int16 into *int32: destination is nil"},
		{"to *int32 nil source", 0, true, new(int32), int32Ptr(0), ""},
		{"to *int32 non nil", 1, false, new(int32), int32Ptr(1), ""},
		{"to *int16 nil dest", 1, false, int16NilPtr(), int16NilPtr(), "cannot store int16 into *int16: destination is nil"},
		{"to *int16 nil source", 0, true, new(int16), int16Ptr(0), ""},
		{"to *int16 non nil", 1, false, new(int16), int16Ptr(1), ""},
		{"to *int8 nil dest", 1, false, int8NilPtr(), int8NilPtr(), "cannot store int16 into *int8: destination is nil"},
		{"to *int8 nil source", 0, true, new(int8), int8Ptr(0), ""},
		{"to *int8 non nil", 1, false, new(int8), int8Ptr(1), ""},
		{"to *int8 out of range pos", math.MaxInt8 + 1, false, new(int8), int8Ptr(0), "cannot store int16 into *int8: value out of range: 128"},
		{"to *int8 out of range neg", math.MinInt8 - 1, false, new(int8), int8Ptr(0), "cannot store int16 into *int8: value out of range: -129"},
		{"to *uint nil dest", 1, false, uintNilPtr(), uintNilPtr(), "cannot store int16 into *uint: destination is nil"},
		{"to *uint nil source", 0, true, new(uint), uintPtr(0), ""},
		{"to *uint non nil", 1, false, new(uint), uintPtr(1), ""},
		{"to *uint out of range neg", -1, false, new(uint), new(uint), "cannot store int16 into *uint: value out of range: -1"},
		{"to *uint64 nil dest", 1, false, uint64NilPtr(), uint64NilPtr(), "cannot store int16 into *uint64: destination is nil"},
		{"to *uint64 nil source", 0, true, new(uint64), uint64Ptr(0), ""},
		{"to *uint64 non nil", 1, false, new(uint64), uint64Ptr(1), ""},
		{"to *uint64 out of range neg", -1, false, new(uint64), uint64Ptr(0), "cannot store int16 into *uint64: value out of range: -1"},
		{"to *uint32 nil dest", 1, false, uint32NilPtr(), uint32NilPtr(), "cannot store int16 into *uint32: destination is nil"},
		{"to *uint32 nil source", 0, true, new(uint32), uint32Ptr(0), ""},
		{"to *uint32 non nil", 1, false, new(uint32), uint32Ptr(1), ""},
		{"to *uint32 out of range neg", -1, false, new(uint32), uint32Ptr(0), "cannot store int16 into *uint32: value out of range: -1"},
		{"to *uint16 nil dest", 1, false, uint16NilPtr(), uint16NilPtr(), "cannot store int16 into *uint16: destination is nil"},
		{"to *uint16 nil source", 0, true, new(uint16), uint16Ptr(0), ""},
		{"to *uint16 non nil", 1, false, new(uint16), uint16Ptr(1), ""},
		{"to *uint16 out of range neg", -1, false, new(uint16), uint16Ptr(0), "cannot store int16 into *uint16: value out of range: -1"},
		{"to *uint8 nil dest", 1, false, uint8NilPtr(), uint8NilPtr(), "cannot store int16 into *uint8: destination is nil"},
		{"to *uint8 nil source", 0, true, new(uint8), uint8Ptr(0), ""},
		{"to *uint8 non nil", 1, false, new(uint8), uint8Ptr(1), ""},
		{"to *uint8 out of range pos", math.MaxUint8 + 1, false, new(uint8), uint8Ptr(0), "cannot store int16 into *uint8: value out of range: 256"},
		{"to *uint8 out of range neg", -1, false, new(uint8), uint8Ptr(0), "cannot store int16 into *uint8: value out of range: -1"},
		{"to *string nil dest", 1, false, stringNilPtr(), stringNilPtr(), "cannot store int16 into *string: destination is nil"},
		{"to *string nil source", 0, true, new(string), new(string), ""},
		{"to *string non nil", 1, false, new(string), stringPtr("1"), ""},
		{"to untyped nil", 1, false, nil, nil, "cannot store int16 into <nil>: destination is nil"},
		{"to non pointer", 1, false, int16(0), int16(0), "cannot store int16 into int16: destination is not pointer"},
		{"to unsupported pointer type", 1, false, new(float64), new(float64), "cannot store int16 into *float64: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := convertFromInt16(tt.val, tt.wasNull, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_writeInt16(t *testing.T) {
	tests := []struct {
		name     string
		val      int16
		expected []byte
	}{
		{"zero", 0, smallintZero},
		{"positive", 1, smallintOne},
		{"negative", -1, smallintMinusOne},
		{"max", math.MaxInt16, smallintMaxInt16},
		{"min", math.MinInt16, smallintMinInt16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := writeInt16(tt.val)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func Test_readInt16(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected int16
		wasNull  bool
		err      string
	}{
		{"nil", nil, 0, true, ""},
		{"empty", []byte{}, 0, true, ""},
		{"wrong length", []byte{1}, 0, false, "cannot read int16: expected 2 bytes but got: 1"},
		{"zero", smallintZero, 0, false, ""},
		{"positive", smallintOne, 1, false, ""},
		{"negative", smallintMinusOne, -1, false, ""},
		{"max", smallintMaxInt16, math.MaxInt16, false, ""},
		{"min", smallintMinInt16, math.MinInt16, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, wasNull, err := readInt16(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.wasNull, wasNull)
			assertErrorMessage(t, tt.err, err)
		})
	}
}
