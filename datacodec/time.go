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
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"time"
)

const (
	TimeLayoutDefault = "15:04:05.999999999"

	// TimeMaxDuration is the maximum duration that can be stored in a CQL time value. Any int64 value that is lesser
	// than zero or grater than this value will be rejected.
	TimeMaxDuration = 24*time.Hour - 1
)

// ConvertTimeToNanosOfDay is a function that converts from a time.Time into nanos since the beginning of the day.
// The given time is normalized to UTC before the computation.
func ConvertTimeToNanosOfDay(t time.Time) int64 {
	t = t.UTC()
	return int64(t.Nanosecond()) +
		int64(t.Second())*int64(time.Second) +
		int64(t.Minute())*int64(time.Minute) +
		int64(t.Hour())*int64(time.Hour)
}

// ConvertDurationToNanosOfDay is a function that converts from a time.Duration into nanos since the beginning of the
// day. An error is returned if the given time value is outside the valid range for CQL time values: from 0 to
// TimeMaxDuration inclusive.
func ConvertDurationToNanosOfDay(d time.Duration) (int64, error) {
	if d < 0 || d > TimeMaxDuration {
		return 0, errValueOutOfRange(d)
	} else {
		return d.Nanoseconds(), nil
	}
}

// ConvertNanosOfDayToTime is a function that converts from nanos since the beginning of the day into a time.Time in UTC.
// The returned time will have its date part set to 0001-01-01 and its time zone will be UTC. An error is returned if
// the given time value is outside the valid range for CQL time values: from 0 to TimeMaxDuration inclusive.
func ConvertNanosOfDayToTime(nanos int64) (time.Time, error) {
	if d := time.Duration(nanos); d < 0 || d > TimeMaxDuration {
		return time.Time{}, errValueOutOfRange(d)
	} else {
		return time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC).Add(d), nil
	}
}

// ConvertNanosOfDayToDuration is a function that converts from nanos since the beginning of the day into a
// time.Duration. An error is returned if the given time value is outside the valid range for CQL time values: from 0 to
// TimeMaxDuration inclusive.
func ConvertNanosOfDayToDuration(nanos int64) (time.Duration, error) {
	if d := time.Duration(nanos); d < 0 || d > TimeMaxDuration {
		return 0, errValueOutOfRange(d)
	} else {
		return d, nil
	}
}

// Time is a codec for the CQL time type with default layout. Its preferred Go type is time.Duration, but it
// can encode from and decode to time.Time, string and to most numeric types as well.
// When encoding from and decoding to time.Duration, the duration must be >= 0 and <= TimeMaxDuration, otherwise an
// error is returned.
// When encoding from and decoding to time.Time, only the clock part is considered, the date part is ignored. Also note
// that all time.Time values are normalized to UTC before encoding and after decoding.
// When encoding from and decoding to numeric types, the numeric value represents the number of nanoseconds since the
// beginning of the day.
// Note that a better representation for the CQL date type can be found in the civil package
// from cloud.google.com, see https://pkg.go.dev/cloud.google.com/go/civil.
var Time = NewTime(TimeLayoutDefault)

// NewTime creates a new codec for CQL time type, with the given layout. The Layout is used only when
// encoding from or decoding to string; it is ignored otherwise. See NewTime for important notes on accepted types.
func NewTime(layout string) Codec {
	return &timeCodec{layout: layout}
}

type timeCodec struct {
	layout string
}

func (c *timeCodec) DataType() datatype.DataType {
	return datatype.Time
}

func (c *timeCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var val int64
		var wasNil bool
		if val, wasNil, err = convertToInt64Time(source, c.layout); err == nil && !wasNil {
			dest = writeInt64(val)
		}
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *timeCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		wasNull = len(source) == 0
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var val int64
		if val, wasNull, err = readInt64(source); err == nil {
			err = convertFromInt64Time(val, wasNull, dest, c.layout)
		}
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToInt64Time(source interface{}, layout string) (val int64, wasNil bool, err error) {
	switch s := source.(type) {
	case time.Duration:
		val, err = ConvertDurationToNanosOfDay(s)
	case *time.Duration:
		if wasNil = s == nil; !wasNil {
			val, err = ConvertDurationToNanosOfDay(*s)
		}
	case time.Time:
		val = ConvertTimeToNanosOfDay(s)
	case *time.Time:
		if wasNil = s == nil; !wasNil {
			val = ConvertTimeToNanosOfDay(*s)
		}
	case string:
		val, err = stringToNanosOfDay(s, layout)
	case *string:
		if wasNil = s == nil; !wasNil {
			val, err = stringToNanosOfDay(*s, layout)
		}
	case nil:
		wasNil = true
	default:
		return convertToInt64(source)
	}
	if err != nil {
		err = errSourceFetchFailed(source, val, err)
	}
	return
}

func convertFromInt64Time(val int64, wasNull bool, dest interface{}, layout string) (err error) {
	switch d := dest.(type) {
	case *time.Duration:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = ConvertNanosOfDayToDuration(val)
		}
	case *time.Time:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = time.Time{}
		} else {
			*d, err = ConvertNanosOfDayToTime(val)
		}
	case *string:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = ""
		} else {
			var t time.Time
			if t, err = ConvertNanosOfDayToTime(val); err == nil {
				*d = t.Format(layout)
			}
		}
	default:
		return convertFromInt64(val, wasNull, dest)
	}
	if err != nil {
		err = errDestStoreFailed(val, dest, err)
	}
	return
}
