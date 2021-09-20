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
	"testing"
	"time"
)

var (
	timeSimple, _ = time.Parse("15:04:05.999999999", "12:34:56.123456789") // 45296123456789
	timeMax, _    = time.Parse("15:04:05.999999999", "23:59:59.999999999")
	timeMin, _    = time.Parse("15:04:05", "00:00:00")

	durationSimple, _     = time.ParseDuration("12h34m56s123456789ns")
	durationMax           = TimeMaxDuration
	durationMin           = time.Duration(0)
	durationOutOfRangePos = TimeMaxDuration + 1
	durationOutOfRangeNeg = time.Duration(-1)

	timeSimpleBytes = encodeUint64(0x0000293253592d15)
)

func TestTimeToNanosOfDay(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected int64
	}{
		{"zero", time.Time{}, 0},
		{"simple", timeSimple, 45296123456789},
		{"min", timeMin, 0},
		{"max", timeMax, int64(TimeMaxDuration)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := ConvertTimeToNanosOfDay(tt.input)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestDurationToNanosOfDay(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Duration
		expected int64
		err      string
	}{
		{"zero", 0, 0, ""},
		{"simple", durationSimple, 45296123456789, ""},
		{"min", durationMin, 0, ""},
		{"max", durationMax, int64(TimeMaxDuration), ""},
		{"out of range positive", durationOutOfRangePos, 0, "value out of range: 24h0m0s"},
		{"out of range negative", durationOutOfRangeNeg, 0, "value out of range: -1ns"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ConvertDurationToNanosOfDay(tt.input)
			assert.Equal(t, tt.expected, actual)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func TestConvertNanosOfDayToTime(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected time.Time
		err      string
	}{
		{"simple", 45296123456789, timeSimple, ""},
		{"min", 0, time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC), ""},
		{"max", int64(TimeMaxDuration), time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC).Add(TimeMaxDuration), ""},
		{"out of range positive", int64(TimeMaxDuration) + 1, time.Time{}, "value out of range: 24h0m0s"},
		{"out of range negative", -1, time.Time{}, "value out of range: -1ns"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ConvertNanosOfDayToTime(tt.input)
			assert.Equal(t, tt.expected, actual)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func TestConvertNanosOfDayToDuration(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected time.Duration
		err      string
	}{
		{"simple", 45296123456789, durationSimple, ""},
		{"min", 0, 0, ""},
		{"max", int64(TimeMaxDuration), TimeMaxDuration, ""},
		{"out of range positive", int64(TimeMaxDuration) + 1, 0, "value out of range: 24h0m0s"},
		{"out of range negative", -1, 0, "value out of range: -1ns"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ConvertNanosOfDayToDuration(tt.input)
			assert.Equal(t, tt.expected, actual)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_timeCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion4) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", nil, nil, ""},
				{"non nil", timeSimple, timeSimpleBytes, ""},
				{"conversion failed", TimeMaxDuration + 1, nil, fmt.Sprintf("cannot encode time.Duration as CQL time with %v: cannot assign time.Duration to int64: value out of range: 24h0m0s", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Time.Encode(tt.source, version)
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
				{"nil", nil, nil, "data type time not supported"},
				{"non nil", timeSimple, nil, "data type time not supported"},
				{"conversion failed", TimeMaxDuration + 1, nil, "data type time not supported"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Time.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_timeCodec_Decode(t *testing.T) {
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
				{"null", nil, new(int64), new(int64), true, ""},
				{"non null", timeSimpleBytes, new(time.Time), &timeSimple, false, ""},
				{"read failed", []byte{1}, new(int64), new(int64), false, fmt.Sprintf("cannot decode CQL time as *int64 with %v: cannot read int64: expected 8 bytes but got: 1", version)},
				{"conversion failed", timeSimpleBytes, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL time as *float64 with %v: cannot store int64 into *float64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Time.Decode(tt.source, tt.dest, version)
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
				{"null", nil, new(int64), new(int64), true, "data type time not supported"},
				{"non null", timeSimpleBytes, new(time.Time), new(time.Time), false, "data type time not supported"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Time.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_convertToInt64Time(t *testing.T) {
	for _, layout := range []string{TimeLayoutDefault, "150405.999999999"} {
		t.Run(layout, func(t *testing.T) {
			tests := []struct {
				name       string
				source     interface{}
				wantVal    int64
				wantWasNil bool
				wantErr    string
			}{
				{"from duration", durationSimple, 45296123456789, false, ""},
				{"from duration out of range", durationOutOfRangePos, 0, false, "cannot assign time.Duration to int64: value out of range: 24h0m0s"},
				{"from *duration nil", durationNilPtr(), 0, true, ""},
				{"from *duration non nil", &durationSimple, 45296123456789, false, ""},
				{"from *duration out of range", &durationOutOfRangePos, 0, false, "cannot assign *time.Duration to int64: value out of range: 24h0m0s"},
				{"from time", timeSimple, 45296123456789, false, ""},
				{"from *time nil", timeNilPtr(), 0, true, ""},
				{"from *time non nil", &timeSimple, 45296123456789, false, ""},
				{"from string", timeSimple.Format(layout), 45296123456789, false, ""},
				{"from string malformed", "not a time", 0, false, "cannot assign string to int64: parsing time \"not a time\" as \"" + layout + "\""},
				{"from *string nil", stringNilPtr(), 0, true, ""},
				{"from *string non nil", stringPtr(timeSimple.Format(layout)), 45296123456789, false, ""},
				{"from *string malformed", stringPtr("not a time"), 0, false, "cannot assign *string to int64: parsing time \"not a time\" as \"" + layout + "\""},
				{"from untyped nil", nil, 0, true, ""},
				{"from numeric", 1234, 1234, false, ""},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					gotVal, gotWasNil, gotErr := convertToInt64Time(tt.source, layout)
					assert.Equal(t, tt.wantVal, gotVal)
					assert.Equal(t, tt.wantWasNil, gotWasNil)
					assertErrorMessage(t, tt.wantErr, gotErr)
				})
			}
		})
	}
}

func Test_convertFromInt64Time(t *testing.T) {
	for _, layout := range []string{TimeLayoutDefault, "150405"} {
		t.Run(layout, func(t *testing.T) {
			tests := []struct {
				name     string
				val      int64
				wasNull  bool
				dest     interface{}
				expected interface{}
				wantErr  string
			}{
				{"to *duration nil dest", 1, false, durationNilPtr(), durationNilPtr(), "cannot store int64 into *time.Duration: destination is nil"},
				{"to *duration nil source", 0, true, new(time.Duration), new(time.Duration), ""},
				{"to *duration", 45296123456789, false, new(time.Duration), &durationSimple, ""},
				{"to *duration out of range", int64(TimeMaxDuration + 1), false, new(time.Duration), new(time.Duration), "cannot store int64 into *time.Duration: value out of range: 24h0m0s"},
				{"to *time nil dest", 1, false, timeNilPtr(), timeNilPtr(), "cannot store int64 into *time.Time: destination is nil"},
				{"to *time nil source", 0, true, new(time.Time), new(time.Time), ""},
				{"to *time", 45296123456789, false, new(time.Time), &timeSimple, ""},
				{"to *time out of range", int64(TimeMaxDuration + 1), false, new(time.Time), new(time.Time), "cannot store int64 into *time.Time: value out of range: 24h0m0s"},
				{"to *string nil dest", 1, false, stringNilPtr(), stringNilPtr(), "cannot store int64 into *string: destination is nil"},
				{"to *string nil source", 0, true, new(string), new(string), ""},
				{"to *string", 45296123456789, false, new(string), stringPtr(timeSimple.Format(layout)), ""},
				{"to *string out of range", int64(TimeMaxDuration + 1), false, new(string), new(string), "cannot store int64 into *string: value out of range: 24h0m0s"},
				{"to numeric", 1234, false, new(int32), int32Ptr(1234), ""},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					gotErr := convertFromInt64Time(tt.val, tt.wasNull, tt.dest, layout)
					assert.Equal(t, tt.expected, tt.dest)
					assertErrorMessage(t, tt.wantErr, gotErr)
				})
			}
		})
	}
}
