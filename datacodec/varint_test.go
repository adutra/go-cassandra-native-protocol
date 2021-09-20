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
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"math"
	"math/big"
	"testing"
)

var hugeBigIntPos = new(big.Int).Add(new(big.Int).SetUint64(math.MaxUint64), oneBigInt)
var hugeBigIntNeg = new(big.Int).Sub(big.NewInt(math.MinInt64), oneBigInt)

func Test_varintCodec_DataType(t *testing.T) {
	assert.Equal(t, datatype.Varint, Varint.DataType())
}

func Test_varintCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", bigIntNilPtr(), nil, ""},
				{"non nil", oneBigInt, []byte{1}, ""},
				{"conversion failed", float64(0), nil, fmt.Sprintf("cannot encode float64 as CQL varint with %v: cannot assign float64 to *big.Int: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Varint.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_varintCodec_Decode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				dest     interface{}
				expected interface{}
				wasNull  bool
				err      string
			}{
				{"null", nil, new(big.Int), new(big.Int), true, ""},
				{"non null", []byte{1}, new(big.Int), oneBigInt, false, ""},
				{"conversion failed", []byte{1}, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL varint as *float64 with %v: cannot store *big.Int into *float64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Varint.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_convertToBigInt(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected *big.Int
		err      string
	}{
		{"from *big.Int non nil", oneBigInt, oneBigInt, ""},
		{"from *big.Int nil", bigIntNilPtr(), nil, ""},
		{"from int", int(1), oneBigInt, ""},
		{"from *int non nil", intPtr(1), oneBigInt, ""},
		{"from *int nil", intNilPtr(), nil, ""},
		{"from int64", int64(1), oneBigInt, ""},
		{"from *int64 non nil", int64Ptr(1), oneBigInt, ""},
		{"from *int64 nil", int64NilPtr(), nil, ""},
		{"from int32", int32(1), oneBigInt, ""},
		{"from *int32 non nil", int32Ptr(1), oneBigInt, ""},
		{"from *int32 nil", int32NilPtr(), nil, ""},
		{"from int16", int16(1), oneBigInt, ""},
		{"from *int16 non nil", int16Ptr(1), oneBigInt, ""},
		{"from *int16 nil", int16NilPtr(), nil, ""},
		{"from int8", int8(1), oneBigInt, ""},
		{"from *int8 non nil", int8Ptr(1), oneBigInt, ""},
		{"from *int8 nil", int8NilPtr(), nil, ""},
		{"from uint", uint(1), oneBigInt, ""},
		{"from *uint non nil", uintPtr(1), oneBigInt, ""},
		{"from *uint nil", uintNilPtr(), nil, ""},
		{"from uint64", uint64(1), oneBigInt, ""},
		{"from *uint64 non nil", uint64Ptr(1), oneBigInt, ""},
		{"from *uint64 nil", uint64NilPtr(), nil, ""},
		{"from uint32", uint32(1), oneBigInt, ""},
		{"from *uint32 non nil", uint32Ptr(1), oneBigInt, ""},
		{"from *uint32 nil", uint32NilPtr(), nil, ""},
		{"from uint16", uint16(1), oneBigInt, ""},
		{"from *uint16 non nil", uint16Ptr(1), oneBigInt, ""},
		{"from *uint16 nil", uint16NilPtr(), nil, ""},
		{"from uint8", uint8(1), oneBigInt, ""},
		{"from *uint8 non nil", uint8Ptr(1), oneBigInt, ""},
		{"from *uint8 nil", uint8NilPtr(), nil, ""},
		{"from string", "1", oneBigInt, ""},
		{"from string malformed", "not a number", nil, "cannot assign string to *big.Int: cannot parse 'not a number'"},
		{"from *string non nil", stringPtr("1"), oneBigInt, ""},
		{"from *string malformed", stringPtr("not a number"), nil, "cannot assign *string to *big.Int: cannot parse 'not a number'"},
		{"from *string nil", stringNilPtr(), nil, ""},
		{"from untyped nil", nil, nil, ""},
		{"from unsupported value type", 42.0, nil, "cannot assign float64 to *big.Int: conversion not supported"},
		{"from unsupported pointer type", float64Ptr(42.0), nil, "cannot assign *float64 to *big.Int: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest, err := convertToBigInt(tt.input)
			assert.Equal(t, tt.expected, dest)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_convertFromBigInt(t *testing.T) {
	tests := []struct {
		name     string
		val      *big.Int
		wasNull  bool
		dest     interface{}
		expected interface{}
		err      string
	}{
		{"to *big.Int nil dest", oneBigInt, false, bigIntNilPtr(), bigIntNilPtr(), "cannot store *big.Int into *big.Int: destination is nil"},
		{"to *big.Int nil source", zeroBigInt, true, new(big.Int), new(big.Int), ""},
		{"to *big.Int non nil", oneBigInt, false, big.NewInt(1), big.NewInt(1), ""},
		{"to *int nil dest", oneBigInt, false, intNilPtr(), intNilPtr(), "cannot store *big.Int into *int: destination is nil"},
		{"to *int nil source", zeroBigInt, true, new(int), intPtr(0), ""},
		{"to *int non nil", oneBigInt, false, new(int), intPtr(1), ""},
		{"to *int out of range pos", hugeBigIntPos, false, new(int), new(int), "cannot store *big.Int into *int: value out of range: 18446744073709551616"},
		{"to *int out of range neg", hugeBigIntNeg, false, new(int), new(int), "cannot store *big.Int into *int: value out of range: -9223372036854775809"},
		{"to *int64 nil dest", oneBigInt, false, int64NilPtr(), int64NilPtr(), "cannot store *big.Int into *int64: destination is nil"},
		{"to *int64 nil source", zeroBigInt, true, new(int64), int64Ptr(0), ""},
		{"to *int64 non nil", oneBigInt, false, new(int64), int64Ptr(1), ""},
		{"to *int64 out of range pos", hugeBigIntPos, false, new(int64), new(int64), "cannot store *big.Int into *int64: value out of range: 18446744073709551616"},
		{"to *int64 out of range neg", hugeBigIntNeg, false, new(int64), new(int64), "cannot store *big.Int into *int64: value out of range: -9223372036854775809"},
		{"to *int32 nil dest", oneBigInt, false, int32NilPtr(), int32NilPtr(), "cannot store *big.Int into *int32: destination is nil"},
		{"to *int32 nil source", zeroBigInt, true, new(int32), int32Ptr(0), ""},
		{"to *int32 non nil", oneBigInt, false, new(int32), int32Ptr(1), ""},
		{"to *int32 out of range pos", hugeBigIntPos, false, new(int32), new(int32), "cannot store *big.Int into *int32: value out of range: 18446744073709551616"},
		{"to *int32 out of range neg", hugeBigIntNeg, false, new(int32), new(int32), "cannot store *big.Int into *int32: value out of range: -9223372036854775809"},
		{"to *int16 nil dest", oneBigInt, false, int16NilPtr(), int16NilPtr(), "cannot store *big.Int into *int16: destination is nil"},
		{"to *int16 nil source", zeroBigInt, true, new(int16), int16Ptr(0), ""},
		{"to *int16 non nil", oneBigInt, false, new(int16), int16Ptr(1), ""},
		{"to *int16 out of range pos", hugeBigIntPos, false, new(int16), new(int16), "cannot store *big.Int into *int16: value out of range: 18446744073709551616"},
		{"to *int16 out of range neg", hugeBigIntNeg, false, new(int16), new(int16), "cannot store *big.Int into *int16: value out of range: -9223372036854775809"},
		{"to *int8 nil dest", oneBigInt, false, int8NilPtr(), int8NilPtr(), "cannot store *big.Int into *int8: destination is nil"},
		{"to *int8 nil source", zeroBigInt, true, new(int8), int8Ptr(0), ""},
		{"to *int8 non nil", oneBigInt, false, new(int8), int8Ptr(1), ""},
		{"to *int8 out of range pos", hugeBigIntPos, false, new(int8), new(int8), "cannot store *big.Int into *int8: value out of range: 18446744073709551616"},
		{"to *int8 out of range neg", hugeBigIntNeg, false, new(int8), new(int8), "cannot store *big.Int into *int8: value out of range: -9223372036854775809"},
		{"to *uint nil dest", oneBigInt, false, uintNilPtr(), uintNilPtr(), "cannot store *big.Int into *uint: destination is nil"},
		{"to *uint nil source", zeroBigInt, true, new(uint), uintPtr(0), ""},
		{"to *uint non nil", oneBigInt, false, new(uint), uintPtr(1), ""},
		{"to *uint out of range pos", hugeBigIntPos, false, new(uint), new(uint), "cannot store *big.Int into *uint: value out of range: 18446744073709551616"},
		{"to *uint out of range neg", hugeBigIntNeg, false, new(uint), new(uint), "cannot store *big.Int into *uint: value out of range: -9223372036854775809"},
		{"to *uint64 nil dest", oneBigInt, false, uint64NilPtr(), uint64NilPtr(), "cannot store *big.Int into *uint64: destination is nil"},
		{"to *uint64 nil source", zeroBigInt, true, new(uint64), uint64Ptr(0), ""},
		{"to *uint64 non nil", oneBigInt, false, new(uint64), uint64Ptr(1), ""},
		{"to *uint64 out of range pos", hugeBigIntPos, false, new(uint64), new(uint64), "cannot store *big.Int into *uint64: value out of range: 18446744073709551616"},
		{"to *uint64 out of range neg", hugeBigIntNeg, false, new(uint64), new(uint64), "cannot store *big.Int into *uint64: value out of range: -9223372036854775809"},
		{"to *uint32 nil dest", oneBigInt, false, uint32NilPtr(), uint32NilPtr(), "cannot store *big.Int into *uint32: destination is nil"},
		{"to *uint32 nil source", zeroBigInt, true, new(uint32), uint32Ptr(0), ""},
		{"to *uint32 non nil", oneBigInt, false, new(uint32), uint32Ptr(1), ""},
		{"to *uint32 out of range pos", hugeBigIntPos, false, new(uint32), new(uint32), "cannot store *big.Int into *uint32: value out of range: 18446744073709551616"},
		{"to *uint32 out of range neg", hugeBigIntNeg, false, new(uint32), new(uint32), "cannot store *big.Int into *uint32: value out of range: -9223372036854775809"},
		{"to *uint16 nil dest", oneBigInt, false, uint16NilPtr(), uint16NilPtr(), "cannot store *big.Int into *uint16: destination is nil"},
		{"to *uint16 nil source", zeroBigInt, true, new(uint16), uint16Ptr(0), ""},
		{"to *uint16 non nil", oneBigInt, false, new(uint16), uint16Ptr(1), ""},
		{"to *uint16 out of range pos", hugeBigIntPos, false, new(uint16), new(uint16), "cannot store *big.Int into *uint16: value out of range: 18446744073709551616"},
		{"to *uint16 out of range neg", hugeBigIntNeg, false, new(uint16), new(uint16), "cannot store *big.Int into *uint16: value out of range: -9223372036854775809"},
		{"to *uint8 nil dest", oneBigInt, false, uint8NilPtr(), uint8NilPtr(), "cannot store *big.Int into *uint8: destination is nil"},
		{"to *uint8 nil source", zeroBigInt, true, new(uint8), uint8Ptr(0), ""},
		{"to *uint8 non nil", oneBigInt, false, new(uint8), uint8Ptr(1), ""},
		{"to *uint8 out of range pos", hugeBigIntPos, false, new(uint8), new(uint8), "cannot store *big.Int into *uint8: value out of range: 18446744073709551616"},
		{"to *uint8 out of range neg", hugeBigIntNeg, false, new(uint8), new(uint8), "cannot store *big.Int into *uint8: value out of range: -9223372036854775809"},
		{"to *string nil dest", oneBigInt, false, stringNilPtr(), stringNilPtr(), "cannot store *big.Int into *string: destination is nil"},
		{"to *string nil source", zeroBigInt, true, new(string), new(string), ""},
		{"to *string non nil", oneBigInt, false, new(string), stringPtr("1"), ""},
		{"to untyped nil", oneBigInt, false, nil, nil, "cannot store *big.Int into <nil>: destination is nil"},
		{"to non pointer", oneBigInt, false, int64(0), int64(0), "cannot store *big.Int into int64: destination is not pointer"},
		{"to unsupported pointer type", oneBigInt, false, new(float64), new(float64), "cannot store *big.Int into *float64: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := convertFromBigInt(tt.val, tt.wasNull, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_writeBigInt(t *testing.T) {
	hugeNeg, ok := new(big.Int).SetString("-1042342234234123423435647768234", 10)
	assert.True(t, ok)
	tests := []struct {
		name     string
		val      *big.Int
		expected []byte
	}{
		{"nil", nil, nil},
		{"zero", zeroBigInt, []byte{0}},
		{"1", oneBigInt, []byte{1}},
		{"-1", big.NewInt(-1), []byte{0xff}},
		{"100", big.NewInt(100), []byte{0x64}},
		{"-100", big.NewInt(-100), []byte{0x9c}},
		{"128", big.NewInt(128), []byte{0x00, 0x80}},
		{"255", big.NewInt(255), []byte{0x00, 0xff}},
		{"MinInt32", big.NewInt(math.MinInt32), []byte{0x80, 0x00, 0x00, 0x00}},
		{"MaxInt32", big.NewInt(math.MaxInt32), []byte{0x7f, 0xff, 0xff, 0xff}},
		{"huge neg", hugeNeg, []byte{0xf2, 0xd8, 0x02, 0xb6, 0x52, 0x7f, 0x99, 0xee, 0x98, 0x23, 0x99, 0xa9, 0x56}},
		{"huge pos", new(big.Int).SetUint64(math.MaxUint64), []byte{0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := writeBigInt(tt.val)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func Test_readBigInt(t *testing.T) {
	hugeNeg, ok := new(big.Int).SetString("-1042342234234123423435647768234", 10)
	assert.True(t, ok)
	tests := []struct {
		name     string
		source   []byte
		expected *big.Int
	}{
		{"nil", nil, nil},
		{"empty", []byte{}, nil},
		{"zero", []byte{0}, zeroBigInt},
		{"-1", []byte{0xff}, big.NewInt(-1)},
		{"100", []byte{0x64}, big.NewInt(100)},
		{"-100", []byte{0x9c}, big.NewInt(-100)},
		{"128", []byte{0x00, 0x80}, big.NewInt(128)},
		{"255", []byte{0x00, 0xff}, big.NewInt(255)},
		{"huge neg", []byte{0xf2, 0xd8, 0x02, 0xb6, 0x52, 0x7f, 0x99, 0xee, 0x98, 0x23, 0x99, 0xa9, 0x56}, hugeNeg},
		{"huge pos", []byte{0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, new(big.Int).SetUint64(math.MaxUint64)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := readBigInt(tt.source)
			assert.Zero(t, tt.expected.Cmp(actual))
		})
	}
}
