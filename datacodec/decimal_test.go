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

var (
	decimalZero      = CqlDecimal{}
	decimalOne       = CqlDecimal{Unscaled: big.NewInt(1), Scale: 0}
	decimalMaxUint64 = CqlDecimal{Unscaled: new(big.Int).SetUint64(math.MaxUint64), Scale: 0}
	decimalSimple    = CqlDecimal{big.NewInt(123), -1}
)

var (
	decimalZeroBytes = []byte{
		0, 0, 0, 0,
		0,
	}
	decimalOneBytes = []byte{
		0, 0, 0, 0,
		1,
	}
	decimalMaxUint64Bytes = []byte{
		0, 0, 0, 0, 0,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	}
	decimalSimpleBytes = []byte{
		0xff, 0xff, 0xff, 0xff,
		0x7b,
	}
)

func Test_decimalCodec_DataType(t *testing.T) {
	assert.Equal(t, datatype.Decimal, Decimal.DataType())
}

func Test_decimalCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", nil, nil, ""},
				{"non nil", decimalSimple, decimalSimpleBytes, ""},
				{"non nil pointer", &decimalSimple, decimalSimpleBytes, ""},
				{"conversion failed", 123, nil, fmt.Sprintf("cannot encode int as CQL decimal with %v: cannot assign int to datacodec.CqlDecimal: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Decimal.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_decimalCodec_Decode(t *testing.T) {
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
				{"null", nil, new(CqlDecimal), new(CqlDecimal), true, ""},
				{"non null", decimalSimpleBytes, new(CqlDecimal), &decimalSimple, false, ""},
				{"read failed", []byte{1, 2, 3}, new(CqlDecimal), new(CqlDecimal), false, fmt.Sprintf("cannot decode CQL decimal as *datacodec.CqlDecimal with %v: cannot read datacodec.CqlDecimal: expected at least 4 bytes but got: 3", version)},
				{"conversion failed", decimalSimpleBytes, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL decimal as *float64 with %v: cannot store datacodec.CqlDecimal into *float64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Decimal.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_convertToDecimal(t *testing.T) {
	tests := []struct {
		name       string
		source     interface{}
		wantDest   CqlDecimal
		wantWasNil bool
		wantErr    string
	}{
		{"from CqlDecimal", decimalSimple, decimalSimple, false, ""},
		{"from *CqlDecimal", &decimalSimple, decimalSimple, false, ""},
		{"from *CqlDecimal nil", cqlDecimalNilPtr(), decimalZero, true, ""},
		{"from untyped nil", nil, decimalZero, true, ""},
		{"from unsupported value type", 123, decimalZero, false, "cannot assign int to datacodec.CqlDecimal: conversion not supported"},
		{"from unsupported pointer type", intPtr(123), decimalZero, false, "cannot assign *int to datacodec.CqlDecimal: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDest, gotWasNil, gotErr := convertToDecimal(tt.source)
			assert.Equal(t, tt.wantDest, gotDest)
			assert.Equal(t, tt.wantWasNil, gotWasNil)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_convertFromDecimal(t *testing.T) {
	tests := []struct {
		name     string
		val      CqlDecimal
		wasNull  bool
		dest     interface{}
		expected interface{}
		err      string
	}{
		{"to *CqlDecimal nil dest", decimalZero, false, cqlDecimalNilPtr(), cqlDecimalNilPtr(), "cannot store datacodec.CqlDecimal into *datacodec.CqlDecimal: destination is nil"},
		{"to *CqlDecimal nil source", decimalZero, true, new(CqlDecimal), new(CqlDecimal), ""},
		{"to *CqlDecimal empty source", decimalZero, false, new(CqlDecimal), new(CqlDecimal), ""},
		{"to *CqlDecimal non nil", decimalSimple, false, new(CqlDecimal), &decimalSimple, ""},
		{"to untyped nil", decimalSimple, false, nil, nil, "cannot store datacodec.CqlDecimal into <nil>: destination is nil"},
		{"to non pointer", decimalSimple, false, CqlDecimal{}, CqlDecimal{}, "cannot store datacodec.CqlDecimal into datacodec.CqlDecimal: destination is not pointer"},
		{"to unsupported pointer type", decimalSimple, false, new(float64), new(float64), "cannot store datacodec.CqlDecimal into *float64: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := convertFromDecimal(tt.val, tt.wasNull, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assertErrorMessage(t, tt.err, gotErr)
		})
	}
}

func Test_writeDecimal(t *testing.T) {
	tests := []struct {
		name     string
		val      CqlDecimal
		expected []byte
	}{
		{"zero", decimalZero, decimalZeroBytes},
		{"one", decimalOne, decimalOneBytes},
		{"simple", decimalSimple, decimalSimpleBytes},
		{"max", decimalMaxUint64, decimalMaxUint64Bytes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := writeDecimal(tt.val)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func Test_readDecimal(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected CqlDecimal
		wasNull  bool
		err      string
	}{
		{"nil", nil, decimalZero, true, ""},
		{"empty", []byte{}, decimalZero, true, ""},
		{"wrong length", []byte{1}, decimalZero, false, "cannot read datacodec.CqlDecimal: expected at least 4 bytes but got: 1"},
		{"zero", decimalZeroBytes, CqlDecimal{zeroBigInt, 0}, false, ""},
		{"simple", decimalSimpleBytes, decimalSimple, false, ""},
		{"max", decimalMaxUint64Bytes, decimalMaxUint64, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, wasNull, err := readDecimal(tt.source)
			assert.Zero(t, tt.expected.Unscaled.Cmp(actual.Unscaled))
			assert.Equal(t, tt.expected.Scale, actual.Scale)
			assert.Equal(t, tt.wasNull, wasNull)
			assertErrorMessage(t, tt.err, err)
		})
	}
}
