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

// Uuid is a codec for the CQL uuid type. Out of better options available in Go's standard library, its
// preferred Go type is primitive.Uuid, but it can encode from and decode to []byte and string as well.
// When dealing with UUIDs in Go, consider using a high-level library such as Google's uuid package:
// https://pkg.go.dev/github.com/google/uuid.
var Uuid Codec = &uuidCodec{dataType: datatype.Uuid}

// Timeuuid is a codec for the CQL timeuuid type. Out of better options available in Go's standard library, its
// preferred Go type is primitive.Uuid, but it can encode from and decode to []byte and string as well.
// This codec does not actually enforce that user-provided UUIDs are time UUIDs; it is functionally equivalent to
// the codecs returned by NewUuid.
// When dealing with UUIDs in Go, consider using a high-level library such as Google's uuid package:
// https://pkg.go.dev/github.com/google/uuid.
var Timeuuid Codec = &uuidCodec{dataType: datatype.Timeuuid}

type uuidCodec struct {
	dataType datatype.DataType
}

func (c *uuidCodec) DataType() datatype.DataType {
	return c.dataType
}

func (c *uuidCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	if dest, err = convertToUuidBytes(source); err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *uuidCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	var val []byte
	if val, wasNull, err = readUuid(source); err == nil {
		err = convertFromUuidBytes(val, wasNull, dest)
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToUuidBytes(source interface{}) (val []byte, err error) {
	switch s := source.(type) {
	case primitive.UUID:
		val = s.Bytes()
	case *primitive.UUID:
		if s != nil {
			val = s.Bytes()
		}
	case []byte:
		val = s
	case *[]byte:
		if s != nil {
			val = *s
		}
	case string:
		var uuid *primitive.UUID
		if uuid, err = primitive.ParseUuid(s); err == nil {
			val = uuid.Bytes()
		}
	case *string:
		if s != nil {
			var uuid *primitive.UUID
			if uuid, err = primitive.ParseUuid(*s); err == nil {
				val = uuid.Bytes()
			}
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

func convertFromUuidBytes(val []byte, wasNull bool, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *primitive.UUID:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = primitive.UUID{}
		} else {
			copy((*d)[:], val)
		}
	case *[]byte:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = nil
		} else {
			*d = val
		}
	case *string:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = ""
		} else {
			var uuid primitive.UUID
			copy(uuid[:], val)
			*d = uuid.String()
		}
	default:
		err = errDestinationInvalid(dest)
	}
	if err != nil {
		err = errDestStoreFailed(val, dest, err)
	}
	return
}

// The below function is roughly equivalent to primitive.ReadUuid.

func readUuid(source []byte) (val []byte, wasNull bool, err error) {
	length := len(source)
	if length == 0 {
		wasNull = true
	} else if length != primitive.LengthOfUuid {
		err = errWrongFixedLength(primitive.LengthOfUuid, length)
	} else {
		val = source
	}
	if err != nil {
		err = errCannotRead(val, err)
	}
	return
}
