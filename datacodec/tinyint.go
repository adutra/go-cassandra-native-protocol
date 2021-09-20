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
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"math"
	"strconv"
)

// Tinyint is a codec for the CQL tinyint type. Its preferred Go type is int8, but it can encode from and decode
// to most numeric types.
var Tinyint Codec = &tinyintCodec{}

type tinyintCodec struct{}

func (c *tinyintCodec) DataType() datatype.DataType {
	return datatype.Tinyint
}

func (c *tinyintCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var val int8
		var wasNil bool
		if val, wasNil, err = convertToInt8(source); err == nil && !wasNil {
			dest = writeInt8(val)
		}
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *tinyintCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		wasNull = len(source) == 0
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var val int8
		if val, wasNull, err = readInt8(source); err == nil {
			err = convertFromInt8(val, wasNull, dest)
		}
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToInt8(source interface{}) (val int8, wasNil bool, err error) {
	switch s := source.(type) {
	case int64:
		val, err = int64ToInt8(s)
	case int:
		val, err = intToInt8(s)
	case int32:
		val, err = int32ToInt8(s)
	case int16:
		val, err = int16ToInt8(s)
	case int8:
		val = s
	case uint64:
		val, err = uint64ToInt8(s)
	case uint:
		val, err = uintToInt8(s)
	case uint32:
		val, err = uint32ToInt8(s)
	case uint16:
		val, err = uint16ToInt8(s)
	case uint8:
		val, err = uint8ToInt8(s)
	case string:
		val, err = stringToInt8(s)
	case *int64:
		if wasNil = s == nil; !wasNil {
			val, err = int64ToInt8(*s)
			if *s < math.MinInt8 || *s > math.MaxInt8 {
				err = errValueOutOfRange(*s)
			} else {
				val = int8(*s)
			}
		}
	case *int:
		if wasNil = s == nil; !wasNil {
			val, err = intToInt8(*s)
		}
	case *int32:
		if wasNil = s == nil; !wasNil {
			val, err = int32ToInt8(*s)
		}
	case *int16:
		if wasNil = s == nil; !wasNil {
			val, err = int16ToInt8(*s)
		}
	case *int8:
		if wasNil = s == nil; !wasNil {
			val = *s
		}
	case *uint64:
		if wasNil = s == nil; !wasNil {
			val, err = uint64ToInt8(*s)
		}
	case *uint:
		if wasNil = s == nil; !wasNil {
			val, err = uintToInt8(*s)
		}
	case *uint32:
		if wasNil = s == nil; !wasNil {
			val, err = uint32ToInt8(*s)
		}
	case *uint16:
		if wasNil = s == nil; !wasNil {
			val, err = uint16ToInt8(*s)
		}
	case *uint8:
		if wasNil = s == nil; !wasNil {
			val, err = uint8ToInt8(*s)
		}
	case *string:
		if wasNil = s == nil; !wasNil {
			val, err = stringToInt8(*s)
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

func convertFromInt8(val int8, wasNull bool, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *int64:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d = int64(val)
		}
	case *int:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d = int(val)
		}
	case *int32:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d = int32(val)
		}
	case *int16:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d = int16(val)
		}
	case *int8:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d = val
		}
	case *uint64:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int8ToUint64(val)
		}
	case *uint:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int8ToUint(val)
		}
	case *uint32:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int8ToUint32(val)
		}
	case *uint16:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int8ToUint16(val)
		}
	case *uint8:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int8ToUint8(val)
		}
	case *string:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = ""
		} else {
			*d = strconv.FormatInt(int64(val), 10)
		}
	default:
		err = errDestinationInvalid(dest)
	}
	if err != nil {
		err = errDestStoreFailed(val, dest, err)
	}
	return
}

const lengthOfTinyint = primitive.LengthOfByte

func writeInt8(val int8) (dest []byte) {
	return []byte{byte(val)}
}

func readInt8(source []byte) (val int8, wasNull bool, err error) {
	length := len(source)
	if length == 0 {
		wasNull = true
	} else if length != lengthOfTinyint {
		err = errWrongFixedLength(lengthOfTinyint, length)
	} else {
		val = int8(source[0])
	}
	if err != nil {
		err = errCannotRead(val, err)
	}
	return
}
