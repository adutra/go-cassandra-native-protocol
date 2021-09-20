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

// Float is a codec for the CQL float type. Its preferred Go type is float32, but it can encode from and decode
// to float64 as well.
var Float Codec = &floatCodec{}

type floatCodec struct{}

func (c *floatCodec) DataType() datatype.DataType {
	return datatype.Float
}

func (c *floatCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	var val float32
	var wasNil bool
	if val, wasNil, err = convertToFloat32(source); err == nil && !wasNil {
		dest = writeFloat32(val)
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *floatCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	var val float32
	if val, wasNull, err = readFloat32(source); err == nil {
		err = convertFromFloat32(val, wasNull, dest)
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToFloat32(source interface{}) (val float32, wasNil bool, err error) {
	switch s := source.(type) {
	case float64:
		val, err = float64ToFloat32(s)
	case float32:
		val = s
	case *float64:
		if wasNil = s == nil; !wasNil {
			val, err = float64ToFloat32(*s)
		}
	case *float32:
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

func convertFromFloat32(val float32, wasNull bool, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *float64:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d = float64(val)
		}
	case *float32:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
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

const lengthOfFloat = primitive.LengthOfInt

func writeFloat32(val float32) (dest []byte) {
	dest = make([]byte, lengthOfFloat)
	binary.BigEndian.PutUint32(dest, math.Float32bits(val))
	return
}

func readFloat32(source []byte) (val float32, wasNull bool, err error) {
	length := len(source)
	if length == 0 {
		wasNull = true
	} else if length != lengthOfFloat {
		err = errWrongFixedLength(lengthOfFloat, length)
	} else {
		val = math.Float32frombits(binary.BigEndian.Uint32(source))
	}
	if err != nil {
		err = errCannotRead(val, err)
	}
	return
}
