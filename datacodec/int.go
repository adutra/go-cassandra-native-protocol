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

// Int is a codec for the CQL int type. Its preferred Go type is int32, but it can encode from and decode to
// most numeric types.
var Int Codec = &intCodec{}

type intCodec struct{}

func (c *intCodec) DataType() datatype.DataType {
	return datatype.Int
}

func (c *intCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	var val int32
	var wasNil bool
	if val, wasNil, err = convertToInt32(source); err == nil && !wasNil {
		dest = writeInt32(val)
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *intCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	var val int32
	if val, wasNull, err = readInt32(source); err == nil {
		err = convertFromInt32(val, wasNull, dest)
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToInt32(source interface{}) (val int32, wasNil bool, err error) {
	switch s := source.(type) {
	case int:
		val, err = intToInt32(s)
	case int64:
		val, err = int64ToInt32(s)
	case int32:
		val = s
	case int16:
		val = int32(s)
	case int8:
		val = int32(s)
	case uint:
		val, err = uintToInt32(s)
	case uint64:
		val, err = uint64ToInt32(s)
	case uint32:
		val, err = uint32ToInt32(s)
	case uint16:
		val = int32(s)
	case uint8:
		val = int32(s)
	case string:
		val, err = stringToInt32(s)
	case *int:
		if wasNil = s == nil; !wasNil {
			val, err = intToInt32(*s)
		}
	case *int64:
		if wasNil = s == nil; !wasNil {
			val, err = int64ToInt32(*s)
		}
	case *int32:
		if wasNil = s == nil; !wasNil {
			val = *s
		}
	case *int16:
		if wasNil = s == nil; !wasNil {
			val = int32(*s)
		}
	case *int8:
		if wasNil = s == nil; !wasNil {
			val = int32(*s)
		}
	case *uint:
		if wasNil = s == nil; !wasNil {
			val, err = uintToInt32(*s)
		}
	case *uint64:
		if wasNil = s == nil; !wasNil {
			val, err = uint64ToInt32(*s)
		}
	case *uint32:
		if wasNil = s == nil; !wasNil {
			val, err = uint32ToInt32(*s)
		}
	case *uint16:
		if wasNil = s == nil; !wasNil {
			val = int32(*s)
		}
	case *uint8:
		if wasNil = s == nil; !wasNil {
			val = int32(*s)
		}
	case *string:
		if wasNil = s == nil; !wasNil {
			val, err = stringToInt32(*s)
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

func convertFromInt32(val int32, wasNull bool, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *int:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d = int(val)
		}
	case *int64:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d = int64(val)
		}
	case *int32:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d = val
		}
	case *int16:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int32ToInt16(val)
		}
	case *int8:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int32ToInt8(val)
		}
	case *uint:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int32ToUint(val)
		}
	case *uint64:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int32ToUint64(val)
		}
	case *uint32:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int32ToUint32(val)
		}
	case *uint16:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int32ToUint16(val)
		}
	case *uint8:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = int32ToUint8(val)
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

const lengthOfInt = primitive.LengthOfInt

func writeInt32(val int32) (dest []byte) {
	dest = make([]byte, lengthOfInt)
	binary.BigEndian.PutUint32(dest, uint32(val))
	return
}

func readInt32(source []byte) (val int32, wasNull bool, err error) {
	length := len(source)
	if length == 0 {
		wasNull = true
	} else if length != lengthOfInt {
		err = errWrongFixedLength(lengthOfInt, length)
	} else {
		val = int32(binary.BigEndian.Uint32(source))
	}
	if err != nil {
		err = errCannotRead(val, err)
	}
	return
}
