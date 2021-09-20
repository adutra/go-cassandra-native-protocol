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

// Varchar is a codec for the CQL varchar (or text) type. Its preferred Go type is string, but it can encode
// from and decode to []byte and []rune as well.
var Varchar Codec = &stringCodec{datatype.Varchar}

// Ascii is a codec for the CQL ascii type. Its preferred Go type is string, but it can encode from
// and decode to []byte and []rune as well.
// The returned codec does not actually enforce that all strings are valid ASCII; it's the caller's responsibility to
// ensure that they are valid.
var Ascii Codec = &stringCodec{datatype.Ascii}

type stringCodec struct {
	dataType datatype.DataType
}

func (c *stringCodec) DataType() datatype.DataType {
	return c.dataType
}

func (c *stringCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	if dest, err = convertToStringBytes(source); err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *stringCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	if wasNull, err = convertFromStringBytes(source, dest); err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToStringBytes(source interface{}) (val []byte, err error) {
	switch s := source.(type) {
	case string:
		val = []byte(s)
	case []byte:
		val = s
	case []rune:
		val = []byte(string(s))
	case *string:
		if s != nil {
			val = []byte(*s)
		}
	case *[]byte:
		if s != nil {
			val = *s
		}
	case *[]rune:
		if s != nil {
			val = []byte(string(*s))
		}
	case nil:
	default:
		err = errConversionNotSupported
	}
	if err != nil {
		err = errSourceFetchFailed(source, val, err)
	}
	return
}

func convertFromStringBytes(val []byte, dest interface{}) (wasNull bool, err error) {
	wasNull = val == nil
	switch d := dest.(type) {
	case *string:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = ""
		} else {
			*d = string(val)
		}
	case *[]byte:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = nil
		} else {
			*d = val
		}
	case *[]rune:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = nil
		} else {
			*d = []rune(string(val))
		}
	default:
		err = errDestinationInvalid(dest)
	}
	if err != nil {
		err = errDestStoreFailed(val, dest, err)
	}
	return
}
