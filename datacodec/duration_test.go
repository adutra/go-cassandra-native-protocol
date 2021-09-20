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
	cqlDurationZero = CqlDuration{}
	cqlDurationPos  = CqlDuration{1, 2, 3}
	cqlDurationNeg  = CqlDuration{-1, -2, -3}
	cqlDurationMax  = CqlDuration{math.MaxInt32, math.MaxInt32, math.MaxInt64}
	cqlDurationMin  = CqlDuration{math.MinInt32, math.MinInt32, math.MinInt64}
)
var (
	cqlDurationPosBytes = []byte{2, 4, 6}
	cqlDurationNegBytes = []byte{1, 3, 5}
	cqlDurationMaxBytes = []byte{0xf0, 0xff, 0xff, 0xff, 0xfe, 0xf0, 0xff, 0xff, 0xff, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}
	cqlDurationMinBytes = []byte{0xf0, 0xff, 0xff, 0xff, 0xff, 0xf0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
)

func Test_durationCodec_DataType(t *testing.T) {
	assert.Equal(t, datatype.Duration, Duration.DataType())
}

func Test_durationCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion5) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", nil, nil, ""},
				{"non nil", cqlDurationPos, cqlDurationPosBytes, ""},
				{"non nil pointer", &cqlDurationPos, cqlDurationPosBytes, ""},
				{"conversion failed", 123, nil, fmt.Sprintf("cannot encode int as CQL duration with %v: cannot assign int to datacodec.CqlDuration: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Duration.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion5) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"null", nil, nil, "data type duration not supported"},
				{"non null", cqlDurationPos, nil, "data type duration not supported"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Duration.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_durationCodec_Decode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion5) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				dest     interface{}
				expected interface{}
				wasNull  bool
				err      string
			}{
				{"null", nil, new(CqlDuration), new(CqlDuration), true, ""},
				{"non null", cqlDurationPosBytes, new(CqlDuration), &cqlDurationPos, false, ""},
				{"read failed", []byte{1}, new(CqlDuration), new(CqlDuration), false, fmt.Sprintf("cannot decode CQL duration as *datacodec.CqlDuration with %v: cannot read datacodec.CqlDuration: cannot read duration days: cannot read [vint]: cannot read [unsigned vint]: EOF", version)},
				{"conversion failed", cqlDurationPosBytes, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL duration as *float64 with %v: cannot store datacodec.CqlDuration into *float64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Duration.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion5) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				dest     interface{}
				expected interface{}
				wasNull  bool
				err      string
			}{
				{"null", nil, new(CqlDuration), new(CqlDuration), true, "data type duration not supported"},
				{"non null", cqlDurationPosBytes, new(CqlDuration), new(CqlDuration), false, "data type duration not supported"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Duration.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_convertFromDuration(t *testing.T) {
	tests := []struct {
		name       string
		source     interface{}
		wantDest   CqlDuration
		wantWasNil bool
		wantErr    string
	}{
		{"from CqlDuration", cqlDurationPos, cqlDurationPos, false, ""},
		{"from *CqlDuration", &cqlDurationPos, cqlDurationPos, false, ""},
		{"from *CqlDuration nil", cqlDurationNilPtr(), cqlDurationZero, true, ""},
		{"from untyped nil", nil, cqlDurationZero, true, ""},
		{"from unsupported value type", 123, cqlDurationZero, false, "cannot assign int to datacodec.CqlDuration: conversion not supported"},
		{"from unsupported pointer type", intPtr(123), cqlDurationZero, false, "cannot assign *int to datacodec.CqlDuration: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDest, gotWasNil, gotErr := convertToDuration(tt.source)
			assert.Equal(t, tt.wantDest, gotDest)
			assert.Equal(t, tt.wantWasNil, gotWasNil)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_convertToDuration(t *testing.T) {
	tests := []struct {
		name     string
		val      CqlDuration
		wasNull  bool
		dest     interface{}
		expected interface{}
		err      string
	}{
		{"to *CqlDuration nil dest", cqlDurationZero, false, cqlDurationNilPtr(), cqlDurationNilPtr(), "cannot store datacodec.CqlDuration into *datacodec.CqlDuration: destination is nil"},
		{"to *CqlDuration nil source", cqlDurationZero, true, new(CqlDuration), new(CqlDuration), ""},
		{"to *CqlDuration empty source", cqlDurationZero, false, new(CqlDuration), new(CqlDuration), ""},
		{"to *CqlDuration non nil", cqlDurationPos, false, new(CqlDuration), &cqlDurationPos, ""},
		{"to untyped nil", cqlDurationPos, false, nil, nil, "cannot store datacodec.CqlDuration into <nil>: destination is nil"},
		{"to non pointer", cqlDurationPos, false, CqlDuration{}, CqlDuration{}, "cannot store datacodec.CqlDuration into datacodec.CqlDuration: destination is not pointer"},
		{"to unsupported pointer type", cqlDurationPos, false, new(float64), new(float64), "cannot store datacodec.CqlDuration into *float64: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := convertFromDuration(tt.val, tt.wasNull, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assertErrorMessage(t, tt.err, gotErr)
		})
	}
}

func Test_writeDuration(t *testing.T) {
	tests := []struct {
		name string
		val  CqlDuration
		want []byte
	}{
		{"pos", cqlDurationPos, cqlDurationPosBytes},
		{"neg", cqlDurationNeg, cqlDurationNegBytes},
		{"max", cqlDurationMax, cqlDurationMaxBytes},
		{"min", cqlDurationMin, cqlDurationMinBytes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := writeDuration(tt.val)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_readDuration(t *testing.T) {
	tests := []struct {
		name        string
		source      []byte
		wantVal     CqlDuration
		wantWasNull bool
		wantErr     string
	}{
		{"nil", nil, CqlDuration{}, true, ""},
		{"empty", []byte{}, CqlDuration{}, true, ""},
		{"pos", cqlDurationPosBytes, cqlDurationPos, false, ""},
		{"neg", cqlDurationNegBytes, cqlDurationNeg, false, ""},
		{"max", cqlDurationMaxBytes, cqlDurationMax, false, ""},
		{"min", cqlDurationMinBytes, cqlDurationMin, false, ""},
		{"wrong months", []byte{255}, CqlDuration{}, false, "cannot read datacodec.CqlDuration: cannot read duration months: cannot read [vint]: cannot read [unsigned vint]: EOF"},
		{"wrong days", []byte{1}, CqlDuration{}, false, "cannot read datacodec.CqlDuration: cannot read duration days: cannot read [vint]: cannot read [unsigned vint]: EOF"},
		{"wrong nanos", []byte{1, 2}, CqlDuration{}, false, "cannot read datacodec.CqlDuration: cannot read duration nanos: cannot read [vint]: cannot read [unsigned vint]: EOF"},
		{"bytes remaining", []byte{2, 4, 6, 8}, CqlDuration{}, false, "cannot read datacodec.CqlDuration: source was not fully read: bytes total: 4, read: 3, remaining: 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotWasNull, gotErr := readDuration(tt.source)
			assert.Equal(t, tt.wantVal, gotVal)
			assert.Equal(t, tt.wantWasNull, gotWasNull)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}
