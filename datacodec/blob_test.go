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

func Test_blobCodec_DataType(t *testing.T) {
	assert.Equal(t, datatype.Blob, Blob.DataType())
	assert.Equal(t, datatype.Blob, PassThrough.DataType())
	customType := datatype.NewCustomType("com.example.Type")
	assert.Equal(t, customType, NewCustom(customType).DataType())
}

func Test_blobCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", nil, nil, ""},
				{"empty", []byte{}, []byte{}, ""},
				{"non nil", []byte{1, 2, 3}, []byte{1, 2, 3}, ""},
				{"conversion failed", 123, nil, fmt.Sprintf("cannot encode int as CQL blob with %v: cannot assign int to []uint8: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Blob.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_blobCodec_Decode(t *testing.T) {
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
				{"null", nil, new([]byte), new([]byte), true, ""},
				{"non null", []byte{1, 2, 3}, new([]byte), &[]byte{1, 2, 3}, false, ""},
				{"conversion failed", []byte{1, 2, 3}, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL blob as *float64 with %v: cannot store []uint8 into *float64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Blob.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_convertToBytes(t *testing.T) {
	tests := []struct {
		name     string
		source   interface{}
		wantDest []byte
		wantErr  string
	}{
		{"from byte", []byte{a, b, c}, []byte{a, b, c}, ""},
		{"from *byte", &[]byte{a, b, c}, []byte{a, b, c}, ""},
		{"from *byte nil", byteSliceNilPtr(), nil, ""},
		{"from string", "abc", []byte{a, b, c}, ""},
		{"from *string", stringPtr("abc"), []byte{a, b, c}, ""},
		{"from *string nil", stringNilPtr(), nil, ""},
		{"from untyped nil", nil, nil, ""},
		{"from unsupported value type", 42.0, nil, "cannot assign float64 to []uint8: conversion not supported"},
		{"from unsupported pointer type", float64Ptr(42.0), nil, "cannot assign *float64 to []uint8: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDest, gotErr := convertToBytes(tt.source)
			assert.Equal(t, tt.wantDest, gotDest)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_convertFromBytes(t *testing.T) {
	tests := []struct {
		name     string
		val      []byte
		wasNull  bool
		dest     interface{}
		expected interface{}
		err      string
	}{
		{"to *byte[] nil dest", []byte{1}, false, byteSliceNilPtr(), byteSliceNilPtr(), "cannot store []uint8 into *[]uint8: destination is nil"},
		{"to *byte[] nil source", nil, true, new([]byte), new([]byte), ""},
		{"to *byte[] empty source", []byte{}, false, new([]byte), &[]byte{}, ""},
		{"to *byte[] non nil", []byte{a, b, c}, false, new([]byte), &[]byte{a, b, c}, ""},
		{"to *string nil dest", []byte{1}, false, stringNilPtr(), stringNilPtr(), "cannot store []uint8 into *string: destination is nil"},
		{"to *string nil source", nil, true, new(string), new(string), ""},
		{"to *string empty source", []byte{}, false, new(string), new(string), ""},
		{"to *string non nil", []byte{a, b, c}, false, new(string), stringPtr("abc"), ""},
		{"to untyped nil", []byte{1}, false, nil, nil, "cannot store []uint8 into <nil>: destination is nil"},
		{"to non pointer", []byte{1}, false, []byte{}, []byte{}, "cannot store []uint8 into []uint8: destination is not pointer"},
		{"to unsupported pointer type", []byte{1}, false, new(float64), new(float64), "cannot store []uint8 into *float64: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWasNull, gotErr := convertFromBytes(tt.val, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assert.Equal(t, tt.wasNull, gotWasNull)
			assertErrorMessage(t, tt.err, gotErr)
		})
	}
}
