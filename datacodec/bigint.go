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
	"encoding/binary"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"strconv"
)

// Bigint is a codec for the CQL bigint type. Its preferred Go type is int64, but it can encode from and decode
// to all numeric types. Note: contrary to what the name similarity suggests, bigint codecs cannot
// handle big.Int values; the CQL type for handling big.Int is varint, not bigint.
var Bigint Codec = &bigintCodec{dataType: datatype.Bigint}

// Counter is a codec for the CQL counter type. Its preferred Go type is int64, but it can encode from and
// to all numeric types. It is identical to the Bigint codec.
var Counter Codec = &bigintCodec{dataType: datatype.Counter}

type bigintCodec struct {
	dataType datatype.PrimitiveType
}

func (c *bigintCodec) DataType() datatype.DataType {
	return c.dataType
}

func (c *bigintCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	var val int64
	var wasNil bool
	if val, wasNil, err = convertToInt64(source); err == nil && !wasNil {
		dest = writeInt64(val)
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *bigintCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	var val int64
	if val, wasNull, err = readInt64(source); err == nil {
		err = convertFromInt64(val, wasNull, dest)
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToInt64(source interface{}) (val int64, wasNil bool, err error) {
	switch s := source.(type) {
	case int:
		val = int64(s)
	case int64:
		val = s
	case int32:
		val = int64(s)
	case int16:
		val = int64(s)
	case int8:
		val = int64(s)
	case uint:
		val, err = uintToInt64(s)
	case uint64:
		val, err = uint64ToInt64(s)
	case uint32:
		val = int64(s)
	case uint16:
		val = int64(s)
	case uint8:
		val = int64(s)
	case string:
		val, err = stringToInt64(s)
	case *int:
		if wasNil = s == nil; !wasNil {
			val = int64(*s)
		}
	case *int64:
		if wasNil = s == nil; !wasNil {
			val = *s
		}
	case *int32:
		if wasNil = s == nil; !wasNil {
			val = int64(*s)
		}
	case *int16:
		if wasNil = s == nil; !wasNil {
			val = int64(*s)
		}
	case *int8:
		if wasNil = s == nil; !wasNil {
			val = int64(*s)
		}
	case *uint:
		if wasNil = s == nil; !wasNil {
			val, err = uintToInt64(*s)
		}
	case *uint64:
		if wasNil = s == nil; !wasNil {
			val, err = uint64ToInt64(*s)
		}
	case *uint32:
		if wasNil = s == nil; !wasNil {
			val = int64(*s)
		}
	case *uint16:
		if wasNil = s == nil; !wasNil {
			val = int64(*s)
		}
	case *uint8:
		if wasNil = s == nil; !wasNil {
			val = int64(*s)
		}
	case *string:
		if wasNil = s == nil; !wasNil {
			val, err = stringToInt64(*s)
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

func convertFromInt64(val int64, wasNull bool, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *int64:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d = val
		}
	case *int:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int64ToInt(val)
		}
	case *int32:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int64ToInt32(val)
		}
	case *int16:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int64ToInt16(val)
		}
	case *int8:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int64ToInt8(val)
		}
	case *uint64:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int64ToUint64(val)
		}
	case *uint:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int64ToUint(val)
		}
	case *uint32:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int64ToUint32(val)
		}
	case *uint16:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int64ToUint16(val)
		}
	case *uint8:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int64ToUint8(val)
		}
	case *string:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = ""
		} else {
			*d = strconv.FormatInt(val, 10)
		}
	default:
		err = errDestinationInvalid(dest)
	}
	if err != nil {
		err = errDestStoreFailed(val, dest, err)
	}
	return
}

const lengthOfBigint = primitive.LengthOfLong

func writeInt64(val int64) (dest []byte) {
	dest = make([]byte, lengthOfBigint)
	binary.BigEndian.PutUint64(dest, uint64(val))
	return
}

func readInt64(source []byte) (val int64, wasNull bool, err error) {
	length := len(source)
	if length == 0 {
		wasNull = true
	} else if length != lengthOfBigint {
		err = errWrongFixedLength(lengthOfBigint, length)
	} else {
		val = int64(binary.BigEndian.Uint64(source))
	}
	if err != nil {
		err = errCannotRead(val, err)
	}
	return
}
