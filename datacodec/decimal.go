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
	"math/big"
)

// CqlDecimal is the poor man's representation in Go of a CQL decimal value, since there is no built-in representation
// of arbitrary-precision decimal values in Go's standard library.
// Note that this value is pretty useless as is. It's highly recommended converting this value to some other type using
// a dedicated library. The most popular one is: https://pkg.go.dev/github.com/ericlagergren/decimal/v3.
// The zero value of a CqlDecimal is encoded as zero, with zero scale.
//
type CqlDecimal struct {

	// Unscaled is a big.Int representing the unscaled decimal value.
	Unscaled *big.Int

	// Scale is the decimal value scale.
	Scale int32
}

// Decimal is a codec for the CQL decimal type. There is no built-in representation of arbitrary-precision
// decimal values in Go's standard library. This is why this codec can only encode from and decode to CqlDecimal.
var Decimal Codec = &decimalCodec{}

type decimalCodec struct{}

func (c *decimalCodec) DataType() datatype.DataType {
	return datatype.Decimal
}

func (c *decimalCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	var val CqlDecimal
	var wasNil bool
	if val, wasNil, err = convertToDecimal(source); err == nil && !wasNil {
		dest = writeDecimal(val)
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *decimalCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	var val CqlDecimal
	if val, wasNull, err = readDecimal(source); err == nil {
		err = convertFromDecimal(val, wasNull, dest)
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToDecimal(source interface{}) (val CqlDecimal, wasNil bool, err error) {
	switch s := source.(type) {
	case CqlDecimal:
		val = s
	case *CqlDecimal:
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

func convertFromDecimal(val CqlDecimal, wasNull bool, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *CqlDecimal:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = CqlDecimal{}
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

func writeDecimal(val CqlDecimal) []byte {
	n := val.Unscaled
	if n == nil {
		n = zeroBigInt
	}
	unscaled := writeBigInt(n)
	dest := make([]byte, lengthOfInt, lengthOfInt+len(unscaled))
	binary.BigEndian.PutUint32(dest, uint32(val.Scale))
	return append(dest, unscaled...)
}

func readDecimal(source []byte) (val CqlDecimal, wasNull bool, err error) {
	length := len(source)
	if length == 0 {
		wasNull = true
	} else if length <= lengthOfInt {
		err = errWrongMinimumLength(lengthOfInt, length)
	} else {
		val.Scale = int32(binary.BigEndian.Uint32(source))
		val.Unscaled = readBigInt(source[lengthOfInt:])
	}
	if err != nil {
		err = errCannotRead(val, err)
	}
	return
}
