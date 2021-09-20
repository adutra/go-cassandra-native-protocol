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

const TimestampLayoutDefault = "2006-01-02T15:04:05.999999999-07:00"

const millisecond = int64(time.Millisecond)

var (
	// TimestampMin is the minimum representable CQL timestamp: -292275055-05-16 16:47:04.192 UTC.
	TimestampMin = time.Unix(-9223372036854776, 192_000_000).UTC()

	// TimestampMax is the maximum representable CQL date: +292278994-08-17 07:12:55.807 UTC.
	TimestampMax = time.Unix(9223372036854775, 807_000_000).UTC()
)

// ConvertTimeToEpochMillis is a function that converts from a time.Time into milliseconds since the Epoch. An error is
// returned if the given time value cannot be converted to milliseconds since the Epoch; convertible values range from
// -292275055-05-16 16:47:04.192 UTC to +292278994-08-17 07:12:55.807 UTC inclusive.
func ConvertTimeToEpochMillis(t time.Time) (int64, error) {
	// Implementation note: we avoid t.UnixNano() because it has a limited range of [1678,2262];
	// values outside this range overflow.
	seconds := t.Unix()
	nanos := int64(t.Nanosecond())
	var millis int64
	var overflow bool
	// This is taken from Java's Instant.toEpochMilli()
	if seconds < 0 && nanos > 0 {
		if millis, overflow = multiplyExact(seconds+1, 1000); !overflow {
			millis, overflow = addExact(millis, nanos/millisecond-1000)
		}
	} else {
		if millis, overflow = multiplyExact(seconds, 1000); !overflow {
			millis, overflow = addExact(millis, nanos/millisecond)
		}
	}
	if overflow {
		return 0, errValueOutOfRange(t)
	}
	return millis, nil
}

// ConvertEpochMillisToTime is a function that converts from milliseconds since the Epoch into a time.Time. The returned
// time will be in UTC.
func ConvertEpochMillisToTime(millis int64) time.Time {
	// This is taken from Java's Instant.ofEpochMilli()
	seconds := floorDiv(millis, 1000)
	nanos := floorMod(millis, 1000) * millisecond
	return time.Unix(seconds, nanos).UTC()
}

// TimestampLocal is a codec for the CQL timestamp type that uses the system's time zone when encoding and
// decoding strings.
// Its preferred Go type is time.Time, but it can encode from and decode to string and to most numeric types as well.
// Note that not all time.Time values can be converted to CQL timestamp: the valid range is from
// -292275055-05-16 16:47:04.192 UTC to +292278994-08-17 07:12:55.807 UTC inclusive. Furthermore, the CQL timestamp
// type has milliseconds precision; any sub-millisecond value will be lost when encoded.
// When encoding from and decoding to numeric types, the numeric value is supposed to be the number of milliseconds
// since the Epoch.
// The layout is Cassandra's preferred layout for CQL timestamp literals and is ISO-8601-compatible.
var TimestampLocal = NewTimestamp(TimestampLayoutDefault, time.Local)

// TimestampUTC is a codec for the CQL timestamp type that uses UTC when encoding and decoding strings.
// Its preferred Go type is time.Time, but it can encode from and decode to string and to most numeric types as well.
// Note that not all time.Time values can be converted to CQL timestamp: the valid range is from
// -292275055-05-16 16:47:04.192 UTC to +292278994-08-17 07:12:55.807 UTC inclusive.
// When encoding from and decoding to numeric types, the numeric value is supposed to be the number of milliseconds
// since the Epoch.
// The layout is Cassandra's preferred layout for CQL timestamp literals and is ISO-8601-compatible.
var TimestampUTC = NewTimestamp(TimestampLayoutDefault, time.UTC)

// NewTimestamp creates a new codec for CQL timestamp values, with the given layout. The Layout is
// used only when encoding from or decoding to string; it is ignored otherwise. The location is used when encoding
// strings, and when decoding values into time.Time and string; it is ignored otherwise.
func NewTimestamp(layout string, location *time.Location) Codec {
	return &timestampCodec{
		layout:     layout,
		location:   location,
		innerCodec: &bigintCodec{dataType: datatype.Timestamp},
	}
}

type timestampCodec struct {
	layout     string
	location   *time.Location
	innerCodec *bigintCodec
}

func (c *timestampCodec) DataType() datatype.DataType {
	return datatype.Timestamp
}

func (c *timestampCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	var val int64
	var wasNil bool
	if val, wasNil, err = convertToInt64Timestamp(source, c.layout, c.location); err == nil && !wasNil {
		dest = writeInt64(val)
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *timestampCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	var val int64
	if val, wasNull, err = readInt64(source); err == nil {
		err = convertFromInt64Timestamp(val, wasNull, dest, c.layout, c.location)
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToInt64Timestamp(source interface{}, layout string, location *time.Location) (val int64, wasNil bool, err error) {
	switch s := source.(type) {
	case time.Time:
		val, err = ConvertTimeToEpochMillis(s)
	case *time.Time:
		if wasNil = s == nil; !wasNil {
			val, err = ConvertTimeToEpochMillis(*s)
		}
	case string:
		val, err = stringToEpochMillis(s, layout, location)
	case *string:
		if wasNil = s == nil; !wasNil {
			val, err = stringToEpochMillis(*s, layout, location)
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

func convertFromInt64Timestamp(val int64, wasNull bool, dest interface{}, layout string, location *time.Location) (err error) {
	switch d := dest.(type) {
	case *time.Time:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = time.Time{}
		} else {
			*d = ConvertEpochMillisToTime(val).In(location)
		}
	case *string:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = ""
		} else {
			*d = ConvertEpochMillisToTime(val).In(location).Format(layout)
		}
	default:
		return convertFromInt64(val, wasNull, dest)
	}
	if err != nil {
		err = errDestStoreFailed(val, dest, err)
	}
	return
}
