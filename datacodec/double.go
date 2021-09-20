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
	"math"
)

// Double is a codec for the CQL double type. Its preferred Go type is float64, but it can encode from and
// decode to float32 as well.
var Double Codec = &doubleCodec{}

type doubleCodec struct{}

func (c *doubleCodec) DataType() datatype.DataType {
	return datatype.Double
}

func (c *doubleCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	var val float64
	var wasNil bool
	if val, wasNil, err = convertToFloat64(source); err == nil && !wasNil {
		dest = writeFloat64(val)
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *doubleCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	var val float64
	if val, wasNull, err = readFloat64(source); err == nil {
		err = convertFromFloat64(val, wasNull, dest)
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToFloat64(source interface{}) (val float64, wasNil bool, err error) {
	switch s := source.(type) {
	case float64:
		val = s
	case float32:
		val = float64(s)
	case *float64:
		if wasNil = s == nil; !wasNil {
			val = *s
		}
	case *float32:
		if wasNil = s == nil; !wasNil {
			val = float64(*s)
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

func convertFromFloat64(val float64, wasNull bool, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *float64:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d = val
		}
	case *float32:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = float64ToFloat32(val)
		}
	default:
		err = errDestinationInvalid(dest)
	}
	if err != nil {
		err = errDestStoreFailed(val, dest, err)
	}
	return
}

const lengthOfDouble = primitive.LengthOfLong

func writeFloat64(val float64) (dest []byte) {
	dest = make([]byte, lengthOfDouble)
	binary.BigEndian.PutUint64(dest, math.Float64bits(val))
	return
}

func readFloat64(source []byte) (val float64, wasNull bool, err error) {
	length := len(source)
	if length == 0 {
		wasNull = true
	} else if length != lengthOfDouble {
		err = errWrongFixedLength(lengthOfDouble, length)
	} else {
		val = math.Float64frombits(binary.BigEndian.Uint64(source))
	}
	if err != nil {
		err = errCannotRead(val, err)
	}
	return
}
