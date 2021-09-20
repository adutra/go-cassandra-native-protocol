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
	abcBytes   = []byte("abc")
	greekBytes = []byte("Μιλάτε αγγλικά;")
	abcRunes   = []rune("abc")
	greekRunes = []rune("Μιλάτε αγγλικά;")
)

func Test_stringCodec_DataType(t *testing.T) {
	assert.Equal(t, datatype.Varchar, Varchar.DataType())
	assert.Equal(t, datatype.Ascii, Ascii.DataType())
}

func Test_stringCodec_Encode(t *testing.T) {
	codecs := []Codec{Varchar, Ascii}
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
						{"empty", "", []byte{}, ""},
						{"non nil", "abc", abcBytes, ""},
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

func Test_stringCodec_Decode(t *testing.T) {
	codecs := []Codec{Varchar, Ascii}
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
						{"null", nil, new(string), new(string), true, ""},
						{"non null", abcBytes, new(string), stringPtr("abc"), false, ""},
						{"conversion failed", abcBytes, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL %v as *float64 with %v: cannot store []uint8 into *float64: conversion not supported", codec.DataType(), version)},
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

func Test_convertToStringBytes(t *testing.T) {
	tests := []struct {
		name     string
		source   interface{}
		wantDest []byte
		wantErr  string
	}{
		{"from []byte", abcBytes, abcBytes, ""},
		{"from *[]byte", &greekBytes, greekBytes, ""},
		{"from *[]byte nil", byteSliceNilPtr(), nil, ""},
		{"from []rune", abcRunes, abcBytes, ""},
		{"from *[]rune", &greekRunes, greekBytes, ""},
		{"from *[]rune nil", runeSliceNilPtr(), nil, ""},
		{"from string", "abc", abcBytes, ""},
		{"from *string", stringPtr("abc"), abcBytes, ""},
		{"from *string nil", stringNilPtr(), nil, ""},
		{"from untyped nil", nil, nil, ""},
		{"from unsupported value type", 42.0, nil, "cannot assign float64 to []uint8: conversion not supported"},
		{"from unsupported pointer type", float64Ptr(42.0), nil, "cannot assign *float64 to []uint8: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDest, gotErr := convertToStringBytes(tt.source)
			assert.Equal(t, tt.wantDest, gotDest)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_convertFromStringBytes(t *testing.T) {
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
		{"to *byte[] non nil", abcBytes, false, new([]byte), &abcBytes, ""},
		{"to *rune[] nil dest", []byte{1}, false, runeSliceNilPtr(), runeSliceNilPtr(), "cannot store []uint8 into *[]int32: destination is nil"},
		{"to *rune[] nil source", nil, true, new([]rune), new([]rune), ""},
		{"to *rune[] empty source", []byte{}, false, new([]rune), &[]rune{}, ""},
		{"to *rune[] non nil", abcBytes, false, new([]rune), &abcRunes, ""},
		{"to *string nil dest", []byte{1}, false, stringNilPtr(), stringNilPtr(), "cannot store []uint8 into *string: destination is nil"},
		{"to *string nil source", nil, true, new(string), new(string), ""},
		{"to *string empty source", []byte{}, false, new(string), new(string), ""},
		{"to *string non nil", abcBytes, false, new(string), stringPtr("abc"), ""},
		{"to untyped nil", []byte{1}, false, nil, nil, "cannot store []uint8 into <nil>: destination is nil"},
		{"to non pointer", []byte{1}, false, []byte{}, []byte{}, "cannot store []uint8 into []uint8: destination is not pointer"},
		{"to unsupported pointer type", []byte{1}, false, new(float64), new(float64), "cannot store []uint8 into *float64: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWasNull, gotErr := convertFromStringBytes(tt.val, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assert.Equal(t, tt.wasNull, gotWasNull)
			assertErrorMessage(t, tt.err, gotErr)
		})
	}
}
