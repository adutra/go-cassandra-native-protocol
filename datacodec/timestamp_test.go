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
	timestampNegUTC, _     = time.Parse("2006-01-02 15:04:05 MST", "1951-06-24 23:00:00.999 UTC")       // -584499599001
	timestampNegZoned, _   = time.Parse("2006-01-02 15:04:05 -07:00", "1951-06-24 16:00:00.999 -07:00") // -584499599001
	timestampPosUTC, _     = time.Parse("2006-01-02 15:04:05 MST", "2021-10-11 23:00:00.999 UTC")       // 1633993200999
	timestampPosZoned, _   = time.Parse("2006-01-02 15:04:05 -07:00", "2021-10-12 00:00:00.999 +01:00") // 1633993200999
	timestampEpoch         = time.Unix(0, 0).UTC()
	timestampEpochZoned, _ = time.Parse("2006-01-02 15:04:05 -07:00", "1969-12-31 23:00:00 -01:00")
	timestampOufOfRangeNeg = time.Date(-292275055, time.May, 16, 16, 47, 04, 191_000_000, time.UTC)   // -292275055-05-16T16:47:04.191Z
	timestampOufOfRangePos = time.Date(292278994, time.August, 17, 07, 12, 55, 808_000_000, time.UTC) // +292278994-08-17T07:12:55.808Z
	paris, _               = time.LoadLocation("Europe/Paris")
	timestampPosBytes      = encodeUint64(0x0000017c71959567)
)

func TestConvertTimeToEpochMillis(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected int64
		err      string
	}{
		{"epoch", timestampEpoch, 0, ""},
		{"epoch zoned", timestampEpochZoned, 0, ""},
		{"negative", timestampNegUTC, -584499599001, ""},
		{"negative zoned", timestampNegZoned, -584499599001, ""},
		{"positive", timestampPosUTC, 1633993200999, ""},
		{"positive zoned", timestampPosZoned, 1633993200999, ""},
		{"min", TimestampMin, math.MinInt64, ""},
		{"max", TimestampMax, math.MaxInt64, ""},
		{"out of range negative", timestampOufOfRangeNeg, 0, "value out of range: -292275055-05-16 16:47:04.191 +0000 UTC"},
		{"out of range positive", timestampOufOfRangePos, 0, "value out of range: 292278994-08-17 07:12:55.808 +0000 UTC"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ConvertTimeToEpochMillis(tt.input)
			assert.Equal(t, tt.expected, actual)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func TestConvertEpochMillisToTime(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected time.Time
	}{
		{"epoch", 0, timestampEpoch},
		{"negative", -584499599001, timestampNegUTC},
		{"positive", 1633993200999, timestampPosUTC},
		{"min", math.MinInt64, TimestampMin},
		{"max", math.MaxInt64, TimestampMax},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := ConvertEpochMillisToTime(tt.input)
			assert.True(t, tt.expected.Equal(actual))
		})
	}
}

func Test_timestampCodec_Encode(t *testing.T) {
	codecs := map[string]Codec{"UTC": TimestampUTC, "Local": TimestampLocal}
	for locationName, codec := range codecs {
		t.Run(locationName, func(t *testing.T) {
			for _, version := range primitive.SupportedProtocolVersions() {
				t.Run(version.String(), func(t *testing.T) {
					tests := []struct {
						name     string
						source   interface{}
						expected []byte
						err      string
					}{
						{"nil", nil, nil, ""},
						{"non nil", timestampPosUTC, timestampPosBytes, ""},
						{"conversion failed", timestampOufOfRangePos, nil, fmt.Sprintf("cannot encode time.Time as CQL timestamp with %v: cannot assign time.Time to int64: value out of range: 292278994-08-17 07:12:55.808 +0000 UTC", version)},
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

func Test_timestampCodec_Decode(t *testing.T) {
	codecs := map[string]Codec{"UTC": TimestampUTC, "Local": TimestampLocal}
	for locationName, codec := range codecs {
		location, _ := time.LoadLocation(locationName)
		t.Run(locationName, func(t *testing.T) {
			for _, version := range primitive.SupportedProtocolVersions() {
				t.Run(version.String(), func(t *testing.T) {
					ts := timestampPosUTC.In(location)
					tests := []struct {
						name     string
						source   []byte
						dest     interface{}
						expected interface{}
						wasNull  bool
						err      string
					}{
						{"null", nil, new(int64), new(int64), true, ""},
						{"non null", timestampPosBytes, new(time.Time), &ts, false, ""},
						{"read failed", []byte{1}, new(int64), new(int64), false, fmt.Sprintf("cannot decode CQL timestamp as *int64 with %v: cannot read int64: expected 8 bytes but got: 1", version)},
						{"conversion failed", timestampPosBytes, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL timestamp as *float64 with %v: cannot store int64 into *float64: conversion not supported", version)},
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

func Test_convertToInt64Timestamp(t *testing.T) {
	for _, layout := range []string{TimestampLayoutDefault, "2006-01-02 15:04:05.999 MST"} {
		t.Run(layout, func(t *testing.T) {
			for _, location := range []*time.Location{time.UTC, paris} {
				t.Run(location.String(), func(t *testing.T) {
					ts := timestampPosUTC.In(location)
					tests := []struct {
						name       string
						source     interface{}
						wantVal    int64
						wantWasNil bool
						wantErr    string
					}{
						{"from time", ts, 1633993200999, false, ""},
						{"from time out of range", timestampOufOfRangePos, 0, false, "cannot assign time.Time to int64: value out of range: 292278994-08-17 07:12:55.808 +0000 UTC"},
						{"from *time nil", timeNilPtr(), 0, true, ""},
						{"from *time non nil", &ts, 1633993200999, false, ""},
						{"from *time out of range", &timestampOufOfRangePos, 0, false, "cannot assign *time.Time to int64: value out of range: 292278994-08-17 07:12:55.808 +0000 UTC"},
						{"from string", ts.Format(layout), 1633993200999, false, ""},
						{"from string malformed", "not a timestamp", 0, false, "cannot assign string to int64: parsing time \"not a timestamp\" as \"" + layout + "\""},
						{"from *string nil", stringNilPtr(), 0, true, ""},
						{"from *string non nil", stringPtr(ts.Format(layout)), 1633993200999, false, ""},
						{"from *string malformed", stringPtr("not a time"), 0, false, "cannot assign *string to int64: parsing time \"not a time\" as \"" + layout + "\""},
						{"from untyped nil", nil, 0, true, ""},
						{"from numeric", 1633993200999, 1633993200999, false, ""},
					}
					for _, tt := range tests {
						t.Run(tt.name, func(t *testing.T) {
							gotVal, gotWasNil, gotErr := convertToInt64Timestamp(tt.source, layout, location)
							assert.Equal(t, tt.wantVal, gotVal)
							assert.Equal(t, tt.wantWasNil, gotWasNil)
							assertErrorMessage(t, tt.wantErr, gotErr)
						})
					}
				})
			}
		})
	}
}

func Test_convertFromInt64Timestamp(t *testing.T) {
	for _, layout := range []string{TimestampLayoutDefault, "2006-01-02 15:04:05 MST"} {
		t.Run(layout, func(t *testing.T) {
			for _, location := range []*time.Location{time.UTC, paris} {
				t.Run(location.String(), func(t *testing.T) {
					ts := timestampPosUTC.In(location)
					tests := []struct {
						name     string
						val      int64
						wasNull  bool
						dest     interface{}
						expected interface{}
						wantErr  string
					}{
						{"to *time nil dest", 1, false, timeNilPtr(), timeNilPtr(), "cannot store int64 into *time.Time: destination is nil"},
						{"to *time nil source", 0, true, new(time.Time), new(time.Time), ""},
						{"to *time", 1633993200999, false, new(time.Time), &ts, ""},
						{"to *string nil dest", 1, false, stringNilPtr(), stringNilPtr(), "cannot store int64 into *string: destination is nil"},
						{"to *string nil source", 0, true, new(string), new(string), ""},
						{"to *string", 1633993200999, false, new(string), stringPtr(ts.Format(layout)), ""},
						{"to numeric", 1234, false, new(int32), int32Ptr(1234), ""},
					}
					for _, tt := range tests {
						t.Run(tt.name, func(t *testing.T) {
							gotErr := convertFromInt64Timestamp(tt.val, tt.wasNull, tt.dest, layout, location)
							assert.Equal(t, tt.expected, tt.dest)
							assertErrorMessage(t, tt.wantErr, gotErr)
						})
					}
				})
			}
		})
	}
}
