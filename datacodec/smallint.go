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

// Smallint is a codec for the CQL smallint type. Its preferred Go type is int16, but it can encode from and
// decode to most numeric types.
var Smallint Codec = &smallintCodec{}

type smallintCodec struct{}

func (c *smallintCodec) DataType() datatype.DataType {
	return datatype.Smallint
}

func (c *smallintCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var val int16
		var wasNil bool
		if val, wasNil, err = convertToInt16(source); err == nil && !wasNil {
			dest = writeInt16(val)
		}
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *smallintCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		wasNull = len(source) == 0
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var val int16
		if val, wasNull, err = readInt16(source); err == nil {
			err = convertFromInt16(val, wasNull, dest)
		}
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToInt16(source interface{}) (val int16, wasNil bool, err error) {
	switch s := source.(type) {
	case int64:
		val, err = int64ToInt16(s)
	case int:
		val, err = intToInt16(s)
	case int32:
		val, err = int32ToInt16(s)
	case int16:
		val = s
	case int8:
		val = int16(s)
	case uint64:
		val, err = uint64ToInt16(s)
	case uint:
		val, err = uintToInt16(s)
	case uint32:
		val, err = uint32ToInt16(s)
	case uint16:
		val, err = uint16ToInt16(s)
	case uint8:
		val = int16(s)
	case string:
		val, err = stringToInt16(s)
	case *int64:
		if wasNil = s == nil; !wasNil {
			val, err = int64ToInt16(*s)
		}
	case *int:
		if wasNil = s == nil; !wasNil {
			val, err = intToInt16(*s)
		}
	case *int32:
		if wasNil = s == nil; !wasNil {
			val, err = int32ToInt16(*s)
		}
	case *int16:
		if wasNil = s == nil; !wasNil {
			val = *s
		}
	case *int8:
		if wasNil = s == nil; !wasNil {
			val = int16(*s)
		}
	case *uint64:
		if wasNil = s == nil; !wasNil {
			val, err = uint64ToInt16(*s)
		}
	case *uint:
		if wasNil = s == nil; !wasNil {
			val, err = uintToInt16(*s)
		}
	case *uint32:
		if wasNil = s == nil; !wasNil {
			val, err = uint32ToInt16(*s)
		}
	case *uint16:
		if wasNil = s == nil; !wasNil {
			val, err = uint16ToInt16(*s)
		}
	case *uint8:
		if wasNil = s == nil; !wasNil {
			val = int16(*s)
		}
	case *string:
		if wasNil = s == nil; !wasNil {
			val, err = stringToInt16(*s)
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

func convertFromInt16(val int16, wasNull bool, dest interface{}) (err error) {
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
			*d = val
		}
	case *int8:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int16ToInt8(val)
		}
	case *uint64:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int16ToUint64(val)
		}
	case *uint:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int16ToUint(val)
		}
	case *uint32:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int16ToUint32(val)
		}
	case *uint16:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int16ToUint16(val)
		}
	case *uint8:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int16ToUint8(val)
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

const lengthOfSmallint = primitive.LengthOfShort

func writeInt16(val int16) (dest []byte) {
	dest = make([]byte, lengthOfSmallint)
	binary.BigEndian.PutUint16(dest, uint16(val))
	return
}

func readInt16(source []byte) (val int16, wasNull bool, err error) {
	length := len(source)
	if length == 0 {
		wasNull = true
	} else if length != lengthOfSmallint {
		err = errWrongFixedLength(lengthOfSmallint, length)
	} else {
		val = int16(binary.BigEndian.Uint16(source))
	}
	if err != nil {
		err = errCannotRead(val, err)
	}
	return
}
