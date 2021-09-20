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
	tinyintZero     = []byte{0x00}
	tinyintOne      = []byte{0x01}
	tinyintMinusOne = []byte{0xff}
	tinyintMaxInt8  = []byte{0x7f}
	tinyintMinInt8  = []byte{0x80}
)

func Test_tinyintCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", int8NilPtr(), nil, ""},
				{"non nil", 1, tinyintOne, ""},
				{"conversion failed", uint8(math.MaxUint8), nil, fmt.Sprintf("cannot encode uint8 as CQL tinyint with %v: cannot assign uint8 to int8: value out of range: 255", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Tinyint.Encode(tt.source, version)
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
				{"nil", int8NilPtr(), nil, "data type tinyint not supported"},
				{"non nil", 1, nil, "data type tinyint not supported"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Tinyint.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_tinyintCodec_Decode(t *testing.T) {
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
				{"null", nil, new(int8), new(int8), true, ""},
				{"non null", tinyintOne, new(int8), int8Ptr(1), false, ""},
				{"read failed", []byte{1, 2}, new(int8), new(int8), false, fmt.Sprintf("cannot decode CQL tinyint as *int8 with %v: cannot read int8: expected 1 bytes but got: 2", version)},
				{"conversion failed", tinyintOne, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL tinyint as *float64 with %v: cannot store int8 into *float64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Tinyint.Decode(tt.source, tt.dest, version)
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
				{"null", nil, new(int8), new(int8), true, "data type tinyint not supported"},
				{"non null", tinyintOne, new(int8), new(int8), false, "data type tinyint not supported"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Tinyint.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_convertToInt8(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int8
		wasNil   bool
		err      string
	}{
		{"from int", int(1), 1, false, ""},
		{"from int out of range pos", int(math.MaxInt8 + 1), 0, false, "cannot assign int to int8: value out of range: 128"},
		{"from int out of range neg", int(math.MinInt8 - 1), 0, false, "cannot assign int to int8: value out of range: -129"},
		{"from *int non nil", intPtr(1), 1, false, ""},
		{"from *int nil", intNilPtr(), 0, true, ""},
		{"from *int out of range pos", intPtr(math.MaxInt8 + 1), 0, false, "cannot assign *int to int8: value out of range: 128"},
		{"from *int out of range neg", intPtr(math.MinInt8 - 1), 0, false, "cannot assign *int to int8: value out of range: -129"},
		{"from int64", int64(1), 1, false, ""},
		{"from int64 out of range pos", int64(math.MaxInt8 + 1), 0, false, "cannot assign int64 to int8: value out of range: 128"},
		{"from int64 out of range neg", int64(math.MinInt8 - 1), 0, false, "cannot assign int64 to int8: value out of range: -129"},
		{"from *int64 non nil", int64Ptr(1), 1, false, ""},
		{"from *int64 nil", int64NilPtr(), 0, true, ""},
		{"from *int64 out of range pos", int64Ptr(math.MaxInt8 + 1), 0, false, "cannot assign *int64 to int8: value out of range: 128"},
		{"from *int64 out of range neg", int64Ptr(math.MinInt8 - 1), 0, false, "cannot assign *int64 to int8: value out of range: -129"},
		{"from int32", int32(1), 1, false, ""},
		{"from int32 out of range pos", int32(math.MaxInt8 + 1), 0, false, "cannot assign int32 to int8: value out of range: 128"},
		{"from int32 out of range neg", int32(math.MinInt8 - 1), 0, false, "cannot assign int32 to int8: value out of range: -129"},
		{"from *int32 non nil", int32Ptr(1), 1, false, ""},
		{"from *int32 nil", int32NilPtr(), 0, true, ""},
		{"from *int32 out of range pos", int32Ptr(math.MaxInt8 + 1), 0, false, "cannot assign *int32 to int8: value out of range: 128"},
		{"from *int32 out of range neg", int32Ptr(math.MinInt8 - 1), 0, false, "cannot assign *int32 to int8: value out of range: -129"},
		{"from int16", int16(1), 1, false, ""},
		{"from *int16 non nil", int16Ptr(1), 1, false, ""},
		{"from *int16 out of range pos", int16Ptr(math.MaxInt8 + 1), 0, false, "cannot assign *int16 to int8: value out of range: 128"},
		{"from *int16 out of range neg", int16Ptr(math.MinInt8 - 1), 0, false, "cannot assign *int16 to int8: value out of range: -129"},
		{"from *int16 nil", int16NilPtr(), 0, true, ""},
		{"from int8", int8(1), 1, false, ""},
		{"from *int8 non nil", int8Ptr(1), 1, false, ""},
		{"from *int8 nil", int8NilPtr(), 0, true, ""},
		{"from uint", uint(1), 1, false, ""},
		{"from uint out of range", uint(math.MaxInt8 + 1), 0, false, "cannot assign uint to int8: value out of range: 128"},
		{"from *uint non nil", uintPtr(1), 1, false, ""},
		{"from *uint nil", uintNilPtr(), 0, true, ""},
		{"from *uint out of range", uintPtr(math.MaxInt8 + 1), 0, false, "cannot assign *uint to int8: value out of range: 128"},
		{"from uint64", uint64(1), 1, false, ""},
		{"from uint64 out of range", uint64(math.MaxInt8 + 1), 0, false, "cannot assign uint64 to int8: value out of range: 128"},
		{"from *uint64 non nil", uint64Ptr(1), 1, false, ""},
		{"from *uint64 nil", uint64NilPtr(), 0, true, ""},
		{"from *uint64 out of range", uint64Ptr(math.MaxInt8 + 1), 0, false, "cannot assign *uint64 to int8: value out of range: 128"},
		{"from uint32", uint32(1), 1, false, ""},
		{"from uint32 out of range", uint32(math.MaxInt8 + 1), 0, false, "cannot assign uint32 to int8: value out of range: 128"},
		{"from *uint32 non nil", uint32Ptr(1), 1, false, ""},
		{"from *uint32 nil", uint32NilPtr(), 0, true, ""},
		{"from *uint32 out of range", uint32Ptr(math.MaxInt8 + 1), 0, false, "cannot assign *uint32 to int8: value out of range: 128"},
		{"from uint16", uint16(1), 1, false, ""},
		{"from uint16 out of range", uint16(math.MaxInt8 + 1), 0, false, "cannot assign uint16 to int8: value out of range: 128"},
		{"from *uint16 non nil", uint16Ptr(1), 1, false, ""},
		{"from *uint16 nil", uint16NilPtr(), 0, true, ""},
		{"from *uint16 out of range", uint16Ptr(math.MaxInt8 + 1), 0, false, "cannot assign *uint16 to int8: value out of range: 128"},
		{"from uint8", uint8(1), 1, false, ""},
		{"from *uint8 non nil", uint8Ptr(1), 1, false, ""},
		{"from *uint8 nil", uint8NilPtr(), 0, true, ""},
		{"from string", "1", 1, false, ""},
		{"from string malformed", "not a number", 0, false, "cannot assign string to int8: cannot parse 'not a number'"},
		{"from string out of range", strconv.Itoa(math.MaxInt8 + 1), 0, false, "cannot assign string to int8: cannot parse '128'"},
		{"from *string non nil", stringPtr("1"), 1, false, ""},
		{"from *string malformed", stringPtr("not a number"), 0, false, "cannot assign *string to int8: cannot parse 'not a number'"},
		{"from *string out of range", stringPtr(strconv.Itoa(math.MaxInt8 + 1)), 0, false, "cannot assign *string to int8: cannot parse '128'"},
		{"from *string nil", stringNilPtr(), 0, true, ""},
		{"from untyped nil", nil, 0, true, ""},
		{"from unsupported value type", 42.0, 0, false, "cannot assign float64 to int8: conversion not supported"},
		{"from unsupported pointer type", float64Ptr(42.0), 0, false, "cannot assign *float64 to int8: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest, wasNil, err := convertToInt8(tt.input)
			assert.Equal(t, tt.expected, dest)
			assert.Equal(t, tt.wasNil, wasNil)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_convertFromInt8(t *testing.T) {
	tests := []struct {
		name     string
		val      int8
		wasNull  bool
		dest     interface{}
		expected interface{}
		err      string
	}{
		{"to *int nil dest", 1, false, intNilPtr(), intNilPtr(), "cannot store int8 into *int: destination is nil"},
		{"to *int nil source", 0, true, new(int), intPtr(0), ""},
		{"to *int non nil", 1, false, new(int), intPtr(1), ""},
		{"to *int64 nil dest", 1, false, int64NilPtr(), int64NilPtr(), "cannot store int8 into *int64: destination is nil"},
		{"to *int64 nil source", 0, true, new(int64), int64Ptr(0), ""},
		{"to *int64 non nil", 1, false, new(int64), int64Ptr(1), ""},
		{"to *int32 nil dest", 1, false, int32NilPtr(), int32NilPtr(), "cannot store int8 into *int32: destination is nil"},
		{"to *int32 nil source", 0, true, new(int32), int32Ptr(0), ""},
		{"to *int32 non nil", 1, false, new(int32), int32Ptr(1), ""},
		{"to *int16 nil dest", 1, false, int16NilPtr(), int16NilPtr(), "cannot store int8 into *int16: destination is nil"},
		{"to *int16 nil source", 0, true, new(int16), int16Ptr(0), ""},
		{"to *int16 non nil", 1, false, new(int16), int16Ptr(1), ""},
		{"to *int8 nil dest", 1, false, int8NilPtr(), int8NilPtr(), "cannot store int8 into *int8: destination is nil"},
		{"to *int8 nil source", 0, true, new(int8), int8Ptr(0), ""},
		{"to *int8 non nil", 1, false, new(int8), int8Ptr(1), ""},
		{"to *uint nil dest", 1, false, uintNilPtr(), uintNilPtr(), "cannot store int8 into *uint: destination is nil"},
		{"to *uint nil source", 0, true, new(uint), uintPtr(0), ""},
		{"to *uint non nil", 1, false, new(uint), uintPtr(1), ""},
		{"to *uint out of range neg", -1, false, new(uint), new(uint), "cannot store int8 into *uint: value out of range: -1"},
		{"to *uint64 nil dest", 1, false, uint64NilPtr(), uint64NilPtr(), "cannot store int8 into *uint64: destination is nil"},
		{"to *uint64 nil source", 0, true, new(uint64), uint64Ptr(0), ""},
		{"to *uint64 non nil", 1, false, new(uint64), uint64Ptr(1), ""},
		{"to *uint64 out of range neg", -1, false, new(uint64), uint64Ptr(0), "cannot store int8 into *uint64: value out of range: -1"},
		{"to *uint32 nil dest", 1, false, uint32NilPtr(), uint32NilPtr(), "cannot store int8 into *uint32: destination is nil"},
		{"to *uint32 nil source", 0, true, new(uint32), uint32Ptr(0), ""},
		{"to *uint32 non nil", 1, false, new(uint32), uint32Ptr(1), ""},
		{"to *uint32 out of range neg", -1, false, new(uint32), uint32Ptr(0), "cannot store int8 into *uint32: value out of range: -1"},
		{"to *uint16 nil dest", 1, false, uint16NilPtr(), uint16NilPtr(), "cannot store int8 into *uint16: destination is nil"},
		{"to *uint16 nil source", 0, true, new(uint16), uint16Ptr(0), ""},
		{"to *uint16 non nil", 1, false, new(uint16), uint16Ptr(1), ""},
		{"to *uint16 out of range neg", -1, false, new(uint16), uint16Ptr(0), "cannot store int8 into *uint16: value out of range: -1"},
		{"to *uint8 nil dest", 1, false, uint8NilPtr(), uint8NilPtr(), "cannot store int8 into *uint8: destination is nil"},
		{"to *uint8 nil source", 0, true, new(uint8), uint8Ptr(0), ""},
		{"to *uint8 non nil", 1, false, new(uint8), uint8Ptr(1), ""},
		{"to *uint8 out of range neg", -1, false, new(uint8), uint8Ptr(0), "cannot store int8 into *uint8: value out of range: -1"},
		{"to *string nil dest", 1, false, stringNilPtr(), stringNilPtr(), "cannot store int8 into *string: destination is nil"},
		{"to *string nil source", 0, true, new(string), new(string), ""},
		{"to *string non nil", 1, false, new(string), stringPtr("1"), ""},
		{"to untyped nil", 1, false, nil, nil, "cannot store int8 into <nil>: destination is nil"},
		{"to non pointer", 1, false, int8(0), int8(0), "cannot store int8 into int8: destination is not pointer"},
		{"to unsupported pointer type", 1, false, new(float64), new(float64), "cannot store int8 into *float64: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := convertFromInt8(tt.val, tt.wasNull, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_writeInt8(t *testing.T) {
	tests := []struct {
		name     string
		val      int8
		expected []byte
	}{
		{"zero", 0, tinyintZero},
		{"positive", 1, tinyintOne},
		{"negative", -1, tinyintMinusOne},
		{"max", math.MaxInt8, tinyintMaxInt8},
		{"min", math.MinInt8, tinyintMinInt8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := writeInt8(tt.val)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func Test_readInt8(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected int8
		wasNull  bool
		err      string
	}{
		{"nil", nil, 0, true, ""},
		{"empty", []byte{}, 0, true, ""},
		{"wrong length", []byte{1, 2}, 0, false, "cannot read int8: expected 1 bytes but got: 2"},
		{"zero", tinyintZero, 0, false, ""},
		{"positive", tinyintOne, 1, false, ""},
		{"negative", tinyintMinusOne, -1, false, ""},
		{"max", tinyintMaxInt8, math.MaxInt8, false, ""},
		{"min", tinyintMinInt8, math.MinInt8, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, wasNull, err := readInt8(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.wasNull, wasNull)
			assertErrorMessage(t, tt.err, err)
		})
	}
}
