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
	"testing"
	"time"
)

var (
	datePos, _     = time.Parse(DateLayoutDefault, "2021-10-12") // 18912
	dateNeg, _     = time.Parse(DateLayoutDefault, "1951-06-24") // -6766
	dateOutOfRange = time.Unix((math.MaxInt32+1)*86400, 0).UTC()

	datePosBytes = encodeUint32(0x800049e0)
)

func TestConvertTimeToEpochDays(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected int32
		err      string
	}{
		{"epoch", time.Unix(0, 0), 0, ""},
		{"negative", dateNeg, -6766, ""},
		{"positive", datePos, 18912, ""},
		{"negative truncation", time.Date(1951, time.June, 24, 23, 59, 59, 999999999, time.UTC), -6766, ""},
		{"positive truncation", time.Date(2021, time.October, 12, 23, 59, 59, 999999999, time.UTC), 18912, ""},
		{"min", DateMin, math.MinInt32, ""},
		{"max", DateMax, math.MaxInt32, ""},
		{"out of range negative", time.Date(-5877641, time.June, 22, 23, 59, 59, 999999999, time.UTC), 0, "value out of range: -5877641-06-22 23:59:59.999999999 +0000 UTC"},
		{"out of range positive", time.Date(5881580, time.July, 12, 0, 0, 0, 0, time.UTC), 0, "value out of range: 5881580-07-12 00:00:00 +0000 UTC"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ConvertTimeToEpochDays(tt.input)
			assert.Equal(t, tt.expected, actual)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func TestConvertEpochDaysToTime(t *testing.T) {
	tests := []struct {
		name     string
		input    int32
		expected time.Time
	}{
		{"epoch", 0, time.Unix(0, 0)},
		{"negative", -6766, dateNeg},
		{"positive", 18912, datePos},
		{"min", math.MinInt32, DateMin},
		{"max", math.MaxInt32, DateMax},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := ConvertEpochDaysToTime(tt.input)
			assert.True(t, tt.expected.Equal(actual))
		})
	}
}

func Test_dateCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", nil, nil, ""},
				{"non nil", datePos, datePosBytes, ""},
				{"conversion failed", dateOutOfRange, nil, fmt.Sprintf("cannot encode time.Time as CQL date with %v: cannot assign time.Time to int32: value out of range: 5881580-07-12 00:00:00 +0000 UTC", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Date.Encode(tt.source, version)
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
				{"nil", nil, nil, "data type date not supported"},
				{"non nil", datePos, nil, "data type date not supported"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Date.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_dateCodec_Decode(t *testing.T) {
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
				{"null", nil, new(int32), new(int32), true, ""},
				{"non null", datePosBytes, new(time.Time), &datePos, false, ""},
				{"read failed", []byte{1}, new(int32), new(int32), false, fmt.Sprintf("cannot decode CQL date as *int32 with %v: cannot read int32: expected 4 bytes but got: 1", version)},
				{"conversion failed", datePosBytes, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL date as *float64 with %v: cannot store int32 into *float64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Date.Decode(tt.source, tt.dest, version)
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
				{"null", nil, new(int32), new(int32), true, "data type date not supported"},
				{"non null", datePosBytes, new(time.Time), new(time.Time), false, "data type date not supported"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Date.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_convertToInt32Date(t *testing.T) {
	for _, layout := range []string{DateLayoutDefault, "Jan 02 2006"} {
		t.Run(layout, func(t *testing.T) {
			tests := []struct {
				name       string
				source     interface{}
				wantVal    int32
				wantWasNil bool
				wantErr    string
			}{
				{"from time", datePos, 18912, false, ""},
				{"from time out of range", dateOutOfRange, 0, false, "cannot assign time.Time to int32: value out of range: 5881580-07-12 00:00:00 +0000 UTC"},
				{"from *time nil", timeNilPtr(), 0, true, ""},
				{"from *time non nil", &datePos, 18912, false, ""},
				{"from *time out of range", &dateOutOfRange, 0, false, "cannot assign *time.Time to int32: value out of range: 5881580-07-12 00:00:00 +0000 UTC"},
				{"from string", datePos.Format(layout), 18912, false, ""},
				{"from string malformed", "not a date", 0, false, "cannot assign string to int32: parsing time \"not a date\" as \"" + layout + "\""},
				{"from *string nil", stringNilPtr(), 0, true, ""},
				{"from *string non nil", stringPtr(datePos.Format(layout)), 18912, false, ""},
				{"from *string malformed", stringPtr("not a date"), 0, false, "cannot assign *string to int32: parsing time \"not a date\" as \"" + layout + "\""},
				{"from untyped nil", nil, 0, true, ""},
				{"from numeric", 1234, 1234, false, ""},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					gotVal, gotWasNil, gotErr := convertToInt32Date(tt.source, layout)
					assert.Equal(t, tt.wantVal, gotVal)
					assert.Equal(t, tt.wantWasNil, gotWasNil)
					assertErrorMessage(t, tt.wantErr, gotErr)
				})
			}
		})
	}
}

func Test_convertFromInt32Date(t *testing.T) {
	for _, layout := range []string{DateLayoutDefault, "Jan 02 2006"} {
		t.Run(layout, func(t *testing.T) {
			tests := []struct {
				name     string
				val      int32
				wasNull  bool
				dest     interface{}
				expected interface{}
				err      string
			}{
				{"to *time nil dest", 1, false, timeNilPtr(), timeNilPtr(), "cannot store int32 into *time.Time: destination is nil"},
				{"to *time nil source", 0, true, new(time.Time), new(time.Time), ""},
				{"to *time", 18912, false, new(time.Time), &datePos, ""},
				{"to *string nil dest", 1, false, stringNilPtr(), stringNilPtr(), "cannot store int32 into *string: destination is nil"},
				{"to *string nil source", 0, true, new(string), new(string), ""},
				{"to *string", 18912, false, new(string), stringPtr(datePos.Format(layout)), ""},
				{"to numeric", 1234, false, new(int32), int32Ptr(1234), ""},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					err := convertFromInt32Date(tt.val, tt.wasNull, layout, tt.dest)
					assert.Equal(t, tt.expected, tt.dest)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}
