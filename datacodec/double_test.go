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
	"testing"
)

var (
	doubleZero       = encodeUint64(0x0000000000000000)
	doubleOne        = encodeUint64(0x3ff0000000000000)
	doubleMinusOne   = encodeUint64(0xbff0000000000000)
	doubleMaxFloat64 = encodeUint64(0x7fefffffffffffff)
)

func Test_doubleCodec_DataType(t *testing.T) {
	assert.Equal(t, datatype.Double, Double.DataType())
}

func Test_doubleCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", float64NilPtr(), nil, ""},
				{"non nil", 1.0, doubleOne, ""},
				{"conversion failed", int32(42), nil, fmt.Sprintf("cannot encode int32 as CQL double with %v: cannot assign int32 to float64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Double.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_doubleCodec_Decode(t *testing.T) {
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
				{"null", nil, new(float64), new(float64), true, ""},
				{"non null", doubleOne, new(float64), float64Ptr(1), false, ""},
				{"read failed", []byte{1}, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL double as *float64 with %v: cannot read float64: expected 8 bytes but got: 1", version)},
				{"conversion failed", doubleOne, new(int64), new(int64), false, fmt.Sprintf("cannot decode CQL double as *int64 with %v: cannot store float64 into *int64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Double.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_convertToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
		wasNil   bool
		err      string
	}{
		{"from float64", float64(1), 1, false, ""},
		{"from *float64 non nil", float64Ptr(1), 1, false, ""},
		{"from *float64 nil", float64NilPtr(), 0, true, ""},
		{"from float32", float32(1), 1, false, ""},
		{"from *float32 non nil", float32Ptr(1), 1, false, ""},
		{"from *float32 nil", float32NilPtr(), 0, true, ""},
		{"from untyped nil", nil, 0, true, ""},
		{"from unsupported value type", 42, 0, false, "cannot assign int to float64: conversion not supported"},
		{"from unsupported pointer type", int32Ptr(42), 0, false, "cannot assign *int32 to float64: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest, wasNil, err := convertToFloat64(tt.input)
			assert.Equal(t, tt.expected, dest)
			assert.Equal(t, tt.wasNil, wasNil)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_convertFromFloat64(t *testing.T) {
	tests := []struct {
		name     string
		val      float64
		wasNull  bool
		dest     interface{}
		expected interface{}
		err      string
	}{
		{"to *float64 nil dest", 1, false, float64NilPtr(), float64NilPtr(), "cannot store float64 into *float64: destination is nil"},
		{"to *float64 nil source", 0, true, new(float64), float64Ptr(0), ""},
		{"to *float64 non nil", 1, false, new(float64), float64Ptr(1), ""},
		{"to *float32 nil dest", 1, false, float32NilPtr(), float32NilPtr(), "cannot store float64 into *float32: destination is nil"},
		{"to *float32 nil source", 0, true, new(float32), float32Ptr(0), ""},
		{"to *float32 non nil", 1, false, new(float32), float32Ptr(1), ""},
		{"to *float32 out of range pos", math.MaxFloat64, false, new(float32), new(float32), "cannot store float64 into *float32: value out of range: 1.7976931348623157e+308"},
		{"to untyped nil", 1, false, nil, nil, "cannot store float64 into <nil>: destination is nil"},
		{"to non pointer", 1, false, int64(0), int64(0), "cannot store float64 into int64: destination is not pointer"},
		{"to unsupported pointer type", 1, false, new(int64), new(int64), "cannot store float64 into *int64: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := convertFromFloat64(tt.val, tt.wasNull, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_writeFloat64(t *testing.T) {
	tests := []struct {
		name     string
		val      float64
		expected []byte
	}{
		{"zero", 0, doubleZero},
		{"positive", 1, doubleOne},
		{"negative", -1, doubleMinusOne},
		{"max", math.MaxFloat64, doubleMaxFloat64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := writeFloat64(tt.val)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func Test_readFloat64(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected float64
		wasNull  bool
		err      string
	}{
		{"nil", nil, 0, true, ""},
		{"empty", []byte{}, 0, true, ""},
		{"wrong length", []byte{1}, 0, false, "cannot read float64: expected 8 bytes but got: 1"},
		{"zero", doubleZero, 0, false, ""},
		{"positive", doubleOne, 1, false, ""},
		{"negative", doubleMinusOne, -1, false, ""},
		{"max", doubleMaxFloat64, math.MaxFloat64, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, wasNull, err := readFloat64(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.wasNull, wasNull)
			assertErrorMessage(t, tt.err, err)
		})
	}
}
