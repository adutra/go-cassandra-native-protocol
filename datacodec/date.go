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
	"math"
	"time"
)

const DateLayoutDefault = "2006-01-02"

var (
	// DateMin is the minimum representable CQL date: -5877641-06-23; it corresponds to math.MinInt32 days before the
	// Epoch.
	DateMin = ConvertEpochDaysToTime(math.MinInt32)

	// DateMax is the maximum representable CQL date: 5881580-07-11; it corresponds to math.MaxInt32 days after the
	// Epoch.
	DateMax = ConvertEpochDaysToTime(math.MaxInt32)
)

// ConvertTimeToEpochDays is a function that converts from a time.Time into days since the Epoch. The given time is
// normalized to UTC before the computation. An error is returned if the given time value is outside the valid range
// for CQL date values: from -5877641-06-23 UTC to 5881580-07-11 UTC inclusive.
func ConvertTimeToEpochDays(t time.Time) (int32, error) {
	// Taken from civil.Date: we convert to Unix time so that we do not have to worry about leap seconds:
	// Unix time increases by exactly 86400 seconds per day.
	days := floorDiv(t.UTC().Unix(), 86400)
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, errValueOutOfRange(t)
	}
	return int32(days), nil
}

// ConvertEpochDaysToTime is a function that converts from days since the Epoch into a time.Time in UTC.
// The returned time will have its clock part set to zero.
// ConvertEpochDaysToTime(math.MinInt32) returns the minimum valid CQL date: -5877641-06-23 UTC.
// ConvertEpochDaysToTime(math.MaxInt32) returns the maximum valid CQL date: 5881580-07-11 UTC.
func ConvertEpochDaysToTime(days int32) time.Time {
	return time.Unix(int64(days)*86400, 0).UTC()
}

// Date is a codec for the CQL date type with default layout. Its preferred Go type is time.Time, but it can
// encode from and decode to string and to and from most numeric types as well.
// When encoding from and decoding to time.Time, only the date part is considered, the clock part is ignored. Also note
// that all time.Time values are normalized to UTC before encoding and after decoding. Also note that not all time.Time
// values can be converted to CQL dates: the valid range is from -5877641-06-23 UTC to 5881580-07-11 UTC inclusive;
// these values can be obtained with ConvertEpochDaysToTime(math.MinInt32) and ConvertEpochDaysToTime(math.MaxInt32).
// When encoding from and decoding to numeric types, the numeric value represents the number of days since the Epoch.
// Note that a better representation for the CQL date type can be found in the civil package
// from cloud.google.com, see https://pkg.go.dev/cloud.google.com/go/civil.
var Date = NewDate(DateLayoutDefault)

// NewDate creates a new codec for the CQL date type, with the given layout. The Layout is used only when
// encoding from or decoding to string; it is ignored otherwise. See NewDate for important notes on accepted types.
func NewDate(layout string) Codec {
	return &dateCodec{layout: layout}
}

type dateCodec struct {
	layout string
}

func (c *dateCodec) DataType() datatype.DataType {
	return datatype.Date
}

// Implementation note: CQL dates are encoded as a number of days since the epoch, stored in 8 bytes with 0 being
// 0x80000000, that is, math.MinInt32. This is why we need to add or subtract math.MinInt32 when encoding and decoding.
// Note that this relies on the fact that some additions will overflow: this is expected.

func (c *dateCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var val int32
		var wasNil bool
		if val, wasNil, err = convertToInt32Date(source, c.layout); err == nil && !wasNil {
			dest = writeInt32(val - math.MinInt32)
		}
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *dateCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		wasNull = len(source) == 0
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var val int32
		if val, wasNull, err = readInt32(source); err == nil {
			err = convertFromInt32Date(val+math.MinInt32, wasNull, c.layout, dest)
		}
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToInt32Date(source interface{}, layout string) (val int32, wasNil bool, err error) {
	switch s := source.(type) {
	case time.Time:
		val, err = ConvertTimeToEpochDays(s)
	case *time.Time:
		if wasNil = s == nil; !wasNil {
			val, err = ConvertTimeToEpochDays(*s)
		}
	case string:
		val, err = stringToEpochDays(s, layout)
	case *string:
		if wasNil = s == nil; !wasNil {
			val, err = stringToEpochDays(*s, layout)
		}
	case nil:
		wasNil = true
	default:
		return convertToInt32(source)
	}
	if err != nil {
		err = errSourceFetchFailed(source, val, err)
	}
	return
}

func convertFromInt32Date(val int32, wasNull bool, layout string, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *time.Time:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = time.Time{}
		} else {
			*d = ConvertEpochDaysToTime(val)
		}
	case *string:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = ""
		} else {
			*d = ConvertEpochDaysToTime(val).Format(layout)
		}
	default:
		return convertFromInt32(val, wasNull, dest)
	}
	if err != nil {
		err = errDestStoreFailed(val, dest, err)
	}
	return
}
