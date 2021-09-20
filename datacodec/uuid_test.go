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
)

var (
	uuid      = primitive.UUID{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
	uuidBytes = []byte{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
)

func Test_uuidCodec_DataType(t *testing.T) {
	assert.Equal(t, datatype.Uuid, Uuid.DataType())
	assert.Equal(t, datatype.Timeuuid, Timeuuid.DataType())
}

func Test_uuidCodec_Encode(t *testing.T) {
	codecs := []Codec{Uuid, Timeuuid}
	for _, codec := range codecs {
		t.Run(codec.DataType().String(), func(t *testing.T) {
			for _, version := range primitive.SupportedProtocolVersions() {
				t.Run(version.String(), func(t *testing.T) {
					tests := []struct {
						name     string
						source   interface{}
						expected []byte
						err      string
					}{
						{"nil", nil, nil, ""},
						{"zero", primitive.UUID{}, (&primitive.UUID{}).Bytes(), ""},
						{"non nil", uuid, uuidBytes, ""},
						{"non nil pointer", &uuid, uuidBytes, ""},
						{"conversion failed", 123, nil, fmt.Sprintf("cannot encode int as CQL %v with %v: cannot assign int to []uint8: conversion not supported", codec.DataType(), version)},
					}
					for _, tt := range tests {
						t.Run(tt.name, func(t *testing.T) {
							actual, err := codec.Encode(tt.source, version)
							assert.Equal(t, tt.expected, actual)
							assertErrorMessage(t, tt.err, err)
						})
					}
				})
			}
		})
	}
}

func Test_uuidCodec_Decode(t *testing.T) {
	codecs := []Codec{Uuid, Timeuuid}
	for _, codec := range codecs {
		t.Run(codec.DataType().String(), func(t *testing.T) {
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
						{"null", nil, new(primitive.UUID), new(primitive.UUID), true, ""},
						{"zero", []byte{}, new(primitive.UUID), new(primitive.UUID), true, ""},
						{"non null", uuidBytes, new(primitive.UUID), &uuid, false, ""},
						{"invalid", []byte{1, 2, 3}, new(primitive.UUID), new(primitive.UUID), false, fmt.Sprintf("cannot decode CQL %v as *primitive.UUID with %v: cannot read []uint8: expected 16 bytes but got: 3", codec.DataType(), version)},
						{"conversion failed", uuidBytes, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL %v as *float64 with %v: cannot store []uint8 into *float64: conversion not supported", codec.DataType(), version)},
					}
					for _, tt := range tests {
						t.Run(tt.name, func(t *testing.T) {
							wasNull, err := codec.Decode(tt.source, tt.dest, version)
							assert.Equal(t, tt.expected, tt.dest)
							assert.Equal(t, tt.wasNull, wasNull)
							assertErrorMessage(t, tt.err, err)
						})
					}
				})
			}
		})
	}
}

func Test_convertToUuid(t *testing.T) {
	tests := []struct {
		name     string
		source   interface{}
		wantDest []byte
		wantErr  string
	}{
		{"from primitive.UUID", uuid, uuidBytes, ""},
		{"from *primitive.UUID", &uuid, uuidBytes, ""},
		{"from *primitive.UUID nil", uuidNilPtr(), nil, ""},
		{"from []byte", uuidBytes, uuidBytes, ""},
		{"from []byte wrong length", []byte{1, 2, 3}, []byte{1, 2, 3}, ""},
		{"from []byte wrong length oversize", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, ""},
		{"from *[]byte", &uuidBytes, uuidBytes, ""},
		{"from *[]byte nil", byteSliceNilPtr(), nil, ""},
		{"from string", uuid.String(), uuidBytes, ""},
		{"from string wrong", "not a valid uuid", nil, "cannot assign string to []uint8: invalid UUID: \"not a valid uuid\""},
		{"from *string", stringPtr(uuid.String()), uuidBytes, ""},
		{"from *string nil", byteSliceNilPtr(), nil, ""},
		{"from *string wrong", stringPtr("not a valid uuid"), nil, "cannot assign *string to []uint8: invalid UUID: \"not a valid uuid\""},
		{"from untyped nil", nil, nil, ""},
		{"from unsupported value type", 123, nil, "cannot assign int to []uint8: conversion not supported"},
		{"from unsupported pointer type", intPtr(123), nil, "cannot assign *int to []uint8: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDest, gotErr := convertToUuidBytes(tt.source)
			assert.Equal(t, tt.wantDest, gotDest)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_convertFromUuid(t *testing.T) {
	tests := []struct {
		name     string
		val      []byte
		wasNull  bool
		dest     interface{}
		expected interface{}
		err      string
	}{
		{"to *primitive.UUID nil dest", uuidBytes, false, uuidNilPtr(), uuidNilPtr(), "cannot store []uint8 into *primitive.UUID: destination is nil"},
		{"to *primitive.UUID empty source", []byte{}, true, new(primitive.UUID), new(primitive.UUID), ""},
		{"to *primitive.UUID wrong length", []byte{1, 2, 3}, false, new(primitive.UUID), &primitive.UUID{1, 2, 3}, ""},
		{"to *primitive.UUID wrong length oversize", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, false, new(primitive.UUID), &primitive.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, ""},
		{"to *primitive.UUID non nil", uuidBytes, false, new(primitive.UUID), &uuid, ""},
		{"to *[]byte nil dest", uuidBytes, false, byteSliceNilPtr(), byteSliceNilPtr(), "cannot store []uint8 into *[]uint8: destination is nil"},
		{"to *[]byte empty source", []byte{}, true, new([]byte), new([]byte), ""},
		{"to *[]byte non nil", uuidBytes, false, new([]byte), &uuidBytes, ""},
		{"to *[]byte wrong length", []byte{1, 2, 3}, false, new([]byte), &[]byte{1, 2, 3}, ""},
		{"to *[]byte wrong length oversize", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, false, new([]byte), &[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, ""},
		{"to *string nil dest", uuidBytes, false, stringNilPtr(), stringNilPtr(), "cannot store []uint8 into *string: destination is nil"},
		{"to *string empty source", []byte{}, true, new(string), new(string), ""},
		{"to *string non nil", uuidBytes, false, new(string), stringPtr(uuid.String()), ""},
		{"to *string wrong length", []byte{1, 2, 3}, false, new(string), stringPtr("01020300-0000-0000-0000-000000000000"), ""},
		{"to *string wrong length oversize", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, false, new(string), stringPtr("01020304-0506-0708-090a-0b0c0d0e0f10"), ""},
		{"to untyped nil", uuidBytes, false, nil, nil, "cannot store []uint8 into <nil>: destination is nil"},
		{"to non pointer", uuidBytes, false, primitive.UUID{}, primitive.UUID{}, "cannot store []uint8 into primitive.UUID: destination is not pointer"},
		{"to unsupported pointer type", uuidBytes, false, new(float64), new(float64), "cannot store []uint8 into *float64: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := convertFromUuidBytes(tt.val, tt.wasNull, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assertErrorMessage(t, tt.err, gotErr)
		})
	}
}

func Test_readUuid(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected []byte
		wasNull  bool
		err      string
	}{
		{"null", nil, nil, true, ""},
		{"empty", []byte{}, nil, true, ""},
		{"wrong length", []byte{1}, nil, false, "cannot read []uint8: expected 16 bytes but got: 1"},
		{"non null", uuidBytes, uuidBytes, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, wasNull, err := readUuid(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.wasNull, wasNull)
			assertErrorMessage(t, tt.err, err)
		})
	}
}
