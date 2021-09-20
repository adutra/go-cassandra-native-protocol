// Copyright 2020 DataStax
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
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
	"time"
)

// CqlDuration is a CQL type introduced in protocol v5.
// A duration can either be positive or negative. If a duration is positive
// all the integers must be positive or zero. If a duration is
// negative all the numbers must be negative or zero.
type CqlDuration struct {
	Months int32
	Days   int32
	Nanos  time.Duration
}

// Duration is a codec for the CQL duration type, introduced in protocol v5. There is no built-in representation of arbitrary-precision
// duration values in Go's standard library. This is why this codec can only encode from and decode to CqlDuration.
var Duration Codec = &durationCodec{}

type durationCodec struct {
}

func (c *durationCodec) DataType() datatype.DataType {
	return datatype.Duration
}

func (c *durationCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var val CqlDuration
		var wasNil bool
		if val, wasNil, err = convertToDuration(source); err == nil && !wasNil {
			dest = writeDuration(val)
		}
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *durationCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		wasNull = len(source) == 0
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var val CqlDuration
		if val, wasNull, err = readDuration(source); err == nil {
			err = convertFromDuration(val, wasNull, dest)
		}
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToDuration(source interface{}) (val CqlDuration, wasNil bool, err error) {
	switch s := source.(type) {
	case CqlDuration:
		val = s
	case *CqlDuration:
		if wasNil = s == nil; !wasNil {
			val = *s
		}
	case nil:
		wasNil = true
	default:
		err = errConversionNotSupported
	}
	if err != nil {
		err = errSourceFetchFailed(source, val, err)
	}
	return
}

func convertFromDuration(val CqlDuration, wasNull bool, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *CqlDuration:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = CqlDuration{}
		} else {
			*d = val
		}
	default:
		err = errDestinationInvalid(dest)
	}
	if err != nil {
		err = errDestStoreFailed(val, dest, err)
	}
	return
}

// Implementation notes from the protocol specs:
// A duration is composed of 3 signed variable length integers ([vint]s).
// The first [vint] represents a number of months, the second [vint] represents
// a number of days, and the last [vint] represents a number of nanoseconds.
// The number of months and days must be valid 32 bits integers whereas the
// number of nanoseconds must be a valid 64 bits integer.

func writeDuration(val CqlDuration) []byte {
	writer := &bytes.Buffer{}
	_, _ = primitive.WriteVint(int64(val.Months), writer)
	_, _ = primitive.WriteVint(int64(val.Days), writer)
	_, _ = primitive.WriteVint(int64(val.Nanos), writer)
	return writer.Bytes()
}

func readDuration(source []byte) (val CqlDuration, wasNull bool, err error) {
	length := len(source)
	wasNull = length == 0
	if !wasNull {
		var months, days, nanos int64
		var rm, rd, rn int
		reader := bytes.NewReader(source)
		months, rm, err = primitive.ReadVint(reader)
		if err == nil {
			_, _ = reader.Seek(int64(rm), io.SeekStart)
			days, rd, err = primitive.ReadVint(reader)
			if err == nil {
				_, _ = reader.Seek(int64(rm+rd), io.SeekStart)
				nanos, rn, err = primitive.ReadVint(reader)
				if err == nil {
					read := rm + rd + rn
					if length == read {
						val.Months = int32(months)
						val.Days = int32(days)
						val.Nanos = time.Duration(nanos)
					} else {
						err = errBytesRemaining(length, length-read)
					}
				} else {
					err = fmt.Errorf("cannot read duration nanos: %w", err)
				}
			} else {
				err = fmt.Errorf("cannot read duration days: %w", err)
			}
		} else {
			err = fmt.Errorf("cannot read duration months: %w", err)
		}
	}
	if err != nil {
		err = errCannotRead(val, err)
	}
	return
}
