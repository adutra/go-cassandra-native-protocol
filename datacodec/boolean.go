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
)

// Boolean is a codec for the CQL boolean type. Its preferred Go type is bool, but it can encode from and decode
// to most numerical types as well (using the general convention: 0 = false, any other value = true).
var Boolean Codec = &booleanCodec{}

type booleanCodec struct{}

func (c *booleanCodec) DataType() datatype.DataType {
	return datatype.Boolean
}

func (c *booleanCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	var val bool
	var wasNil bool
	if val, wasNil, err = convertToBoolean(source); err == nil && !wasNil {
		dest = writeBool(val)
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *booleanCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	var val bool
	if val, wasNull, err = readBool(source); err == nil {
		err = convertFromBoolean(val, wasNull, dest)
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToBoolean(source interface{}) (val bool, wasNil bool, err error) {
	switch s := source.(type) {
	case bool:
		val = s
	case int64:
		val = s != 0
	case int:
		val = s != 0
	case int32:
		val = s != 0
	case int16:
		val = s != 0
	case int8:
		val = s != 0
	case uint64:
		val = s != 0
	case uint:
		val = s != 0
	case uint32:
		val = s != 0
	case uint16:
		val = s != 0
	case uint8:
		val = s != 0
	case *bool:
		if wasNil = s == nil; !wasNil {
			val = *s
		}
	case *int64:
		if wasNil = s == nil; !wasNil {
			val = *s != 0
		}
	case *int:
		if wasNil = s == nil; !wasNil {
			val = *s != 0
		}
	case *int32:
		if wasNil = s == nil; !wasNil {
			val = *s != 0
		}
	case *int16:
		if wasNil = s == nil; !wasNil {
			val = *s != 0
		}
	case *int8:
		if wasNil = s == nil; !wasNil {
			val = *s != 0
		}
	case *uint64:
		if wasNil = s == nil; !wasNil {
			val = *s != 0
		}
	case *uint:
		if wasNil = s == nil; !wasNil {
			val = *s != 0
		}
	case *uint32:
		if wasNil = s == nil; !wasNil {
			val = *s != 0
		}
	case *uint16:
		if wasNil = s == nil; !wasNil {
			val = *s != 0
		}
	case *uint8:
		if wasNil = s == nil; !wasNil {
			val = *s != 0
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

func convertFromBoolean(val bool, wasNull bool, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *bool:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = false
		} else {
			*d = val
		}
	case *int64:
		if d == nil {
			err = errNilDestination
		} else if wasNull || !val {
			*d = 0
		} else {
			*d = 1
		}
	case *int:
		if d == nil {
			err = errNilDestination
		} else if wasNull || !val {
			*d = 0
		} else {
			*d = 1
		}
	case *int32:
		if d == nil {
			err = errNilDestination
		} else if wasNull || !val {
			*d = 0
		} else {
			*d = 1
		}
	case *int16:
		if d == nil {
			err = errNilDestination
		} else if wasNull || !val {
			*d = 0
		} else {
			*d = 1
		}
	case *int8:
		if d == nil {
			err = errNilDestination
		} else if wasNull || !val {
			*d = 0
		} else {
			*d = 1
		}
	case *uint64:
		if d == nil {
			err = errNilDestination
		} else if wasNull || !val {
			*d = 0
		} else {
			*d = 1
		}
	case *uint:
		if d == nil {
			err = errNilDestination
		} else if wasNull || !val {
			*d = 0
		} else {
			*d = 1
		}
	case *uint32:
		if d == nil {
			err = errNilDestination
		} else if wasNull || !val {
			*d = 0
		} else {
			*d = 1
		}
	case *uint16:
		if d == nil {
			err = errNilDestination
		} else if wasNull || !val {
			*d = 0
		} else {
			*d = 1
		}
	case *uint8:
		if d == nil {
			err = errNilDestination
		} else if wasNull || !val {
			*d = 0
		} else {
			*d = 1
		}
	default:
		err = errDestinationInvalid(dest)
	}
	if err != nil {
		err = errDestStoreFailed(val, dest, err)
	}
	return
}

func writeBool(val bool) []byte {
	if val {
		return []byte{1}
	} else {
		return []byte{0}
	}
}

func readBool(source []byte) (val bool, wasNull bool, err error) {
	length := len(source)
	if length == 0 {
		wasNull = true
	} else if length != 1 {
		err = errWrongFixedLength(1, length)
	} else {
		val = source[0] != 0
	}
	if err != nil {
		err = errCannotRead(val, err)
	}
	return
}
