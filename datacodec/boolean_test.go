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
	"testing"
	"time"
)

func Test_booleanCodec_DataType(t *testing.T) {
	assert.Equal(t, datatype.Boolean, Boolean.DataType())
}

func Test_booleanCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", nil, nil, ""},
				{"true", true, []byte{1}, ""},
				{"true", true, []byte{1}, ""},
				{"false", false, []byte{0}, ""},
				{"conversion failed", time.Nanosecond, nil, fmt.Sprintf("cannot encode time.Duration as CQL boolean with %v: cannot assign time.Duration to bool: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Boolean.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_booleanCodec_Decode(t *testing.T) {
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
				{"null", nil, new(bool), new(bool), true, ""},
				{"true", []byte{1}, new(bool), boolPtr(true), false, ""},
				{"true 255", []byte{255}, new(bool), boolPtr(true), false, ""},
				{"false", []byte{0}, new(bool), boolPtr(false), false, ""},
				{"read failed", []byte{1, 2, 3}, new(bool), new(bool), false, fmt.Sprintf("cannot decode CQL boolean as *bool with %v: cannot read bool: expected 1 bytes but got: 3", version)},
				{"conversion failed", []byte{1}, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL boolean as *float64 with %v: cannot store bool into *float64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Boolean.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_convertToBoolean(t *testing.T) {
	tests := []struct {
		name       string
		source     interface{}
		wantDest   bool
		wantWasNil bool
		wantErr    string
	}{
		{"from bool", true, true, false, ""},
		{"from *bool non nil", boolPtr(true), true, false, ""},
		{"from *bool nil", boolNilPtr(), false, true, ""},
		{"from int", int(1), true, false, ""},
		{"from *int non nil", intPtr(1), true, false, ""},
		{"from *int nil", intNilPtr(), false, true, ""},
		{"from int64", int64(1), true, false, ""},
		{"from *int64 non nil", int64Ptr(1), true, false, ""},
		{"from *int64 nil", int64NilPtr(), false, true, ""},
		{"from int32", int32(1), true, false, ""},
		{"from *int32 non nil", int32Ptr(1), true, false, ""},
		{"from *int32 nil", int32NilPtr(), false, true, ""},
		{"from int16", int16(1), true, false, ""},
		{"from *int16 non nil", int16Ptr(1), true, false, ""},
		{"from *int16 nil", int16NilPtr(), false, true, ""},
		{"from int8", int8(1), true, false, ""},
		{"from *int8 non nil", int8Ptr(1), true, false, ""},
		{"from *int8 nil", int8NilPtr(), false, true, ""},
		{"from uint", uint(1), true, false, ""},
		{"from *uint non nil", uintPtr(1), true, false, ""},
		{"from *uint nil", uintNilPtr(), false, true, ""},
		{"from uint64", uint64(1), true, false, ""},
		{"from *uint64 non nil", uint64Ptr(1), true, false, ""},
		{"from *uint64 nil", uint64NilPtr(), false, true, ""},
		{"from uint32", uint32(1), true, false, ""},
		{"from *uint32 non nil", uint32Ptr(1), true, false, ""},
		{"from *uint32 nil", uint32NilPtr(), false, true, ""},
		{"from uint16", uint16(1), true, false, ""},
		{"from *uint16 non nil", uint16Ptr(1), true, false, ""},
		{"from *uint16 nil", uint16NilPtr(), false, true, ""},
		{"from uint8", uint8(1), true, false, ""},
		{"from *uint8 non nil", uint8Ptr(1), true, false, ""},
		{"from *uint8 nil", uint8NilPtr(), false, true, ""},
		{"from untyped nil", nil, false, true, ""},
		{"from unsupported pointer type", float64Ptr(42.0), false, false, "cannot assign *float64 to bool: conversion not supported"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDest, gotWasNil, gotErr := convertToBoolean(tt.source)
			assert.Equal(t, tt.wantDest, gotDest)
			assert.Equal(t, tt.wantWasNil, gotWasNil)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_convertFromBoolean(t *testing.T) {
	tests := []struct {
		name     string
		val      bool
		wasNull  bool
		dest     interface{}
		expected interface{}
		err      string
	}{
		{"to *bool nil dest", true, false, boolNilPtr(), boolNilPtr(), "cannot store bool into *bool: destination is nil"},
		{"to *bool nil source", false, true, new(bool), boolPtr(false), ""},
		{"to *bool non nil", true, false, new(bool), boolPtr(true), ""},
		{"to *int nil dest", true, false, intNilPtr(), intNilPtr(), "cannot store bool into *int: destination is nil"},
		{"to *int nil source", false, true, new(int), intPtr(0), ""},
		{"to *int non nil", true, false, new(int), intPtr(1), ""},
		{"to *int64 nil dest", true, false, int64NilPtr(), int64NilPtr(), "cannot store bool into *int64: destination is nil"},
		{"to *int64 nil source", false, true, new(int64), int64Ptr(0), ""},
		{"to *int64 non nil", true, false, new(int64), int64Ptr(1), ""},
		{"to *int32 nil dest", true, false, int32NilPtr(), int32NilPtr(), "cannot store bool into *int32: destination is nil"},
		{"to *int32 nil source", false, true, new(int32), int32Ptr(0), ""},
		{"to *int32 non nil", true, false, new(int32), int32Ptr(1), ""},
		{"to *int16 nil dest", true, false, int16NilPtr(), int16NilPtr(), "cannot store bool into *int16: destination is nil"},
		{"to *int16 nil source", false, true, new(int16), int16Ptr(0), ""},
		{"to *int16 non nil", true, false, new(int16), int16Ptr(1), ""},
		{"to *int8 nil dest", true, false, int8NilPtr(), int8NilPtr(), "cannot store bool into *int8: destination is nil"},
		{"to *int8 nil source", false, true, new(int8), int8Ptr(0), ""},
		{"to *int8 non nil", true, false, new(int8), int8Ptr(1), ""},
		{"to *uint nil dest", true, false, uintNilPtr(), uintNilPtr(), "cannot store bool into *uint: destination is nil"},
		{"to *uint nil source", false, true, new(uint), uintPtr(0), ""},
		{"to *uint non nil", true, false, new(uint), uintPtr(1), ""},
		{"to *uint64 nil dest", true, false, uint64NilPtr(), uint64NilPtr(), "cannot store bool into *uint64: destination is nil"},
		{"to *uint64 nil source", false, true, new(uint64), uint64Ptr(0), ""},
		{"to *uint64 non nil", true, false, new(uint64), uint64Ptr(1), ""},
		{"to *uint32 nil dest", true, false, uint32NilPtr(), uint32NilPtr(), "cannot store bool into *uint32: destination is nil"},
		{"to *uint32 nil source", false, true, new(uint32), uint32Ptr(0), ""},
		{"to *uint32 non nil", true, false, new(uint32), uint32Ptr(1), ""},
		{"to *uint16 nil dest", true, false, uint16NilPtr(), uint16NilPtr(), "cannot store bool into *uint16: destination is nil"},
		{"to *uint16 nil source", false, true, new(uint16), uint16Ptr(0), ""},
		{"to *uint16 non nil", true, false, new(uint16), uint16Ptr(1), ""},
		{"to *uint8 nil dest", true, false, uint8NilPtr(), uint8NilPtr(), "cannot store bool into *uint8: destination is nil"},
		{"to *uint8 nil source", false, true, new(uint8), uint8Ptr(0), ""},
		{"to *uint8 non nil", true, false, new(uint8), uint8Ptr(1), ""},
		{"to untyped nil", true, false, nil, nil, "cannot store bool into <nil>: destination is nil"},
		{"to non pointer", true, false, int64(0), int64(0), "cannot store bool into int64: destination is not pointer"},
		{"to unsupported pointer type", true, false, new(float64), new(float64), "cannot store bool into *float64: conversion not supported"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := convertFromBoolean(tt.val, tt.wasNull, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assertErrorMessage(t, tt.err, gotErr)
		})
	}
}

func Test_writeBoolean(t *testing.T) {
	tests := []struct {
		name     string
		val      bool
		expected []byte
	}{
		{"false", false, []byte{0}},
		{"true", true, []byte{1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := writeBool(tt.val)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func Test_readBoolean(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected bool
		wasNull  bool
		err      string
	}{
		{"nil", nil, false, true, ""},
		{"empty", []byte{}, false, true, ""},
		{"wrong length", []byte{1, 2}, false, false, "cannot read bool: expected 1 bytes but got: 2"},
		{"false", []byte{0}, false, false, ""},
		{"true", []byte{1}, true, false, ""},
		{"true 255", []byte{255}, true, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, wasNull, err := readBool(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.wasNull, wasNull)
			assertErrorMessage(t, tt.err, err)
		})
	}
}
