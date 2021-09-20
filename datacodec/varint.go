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
	"math/big"
)

// Varint is a codec for the CQL varint type, a type that can handle arbitrary-length integers. Its preferred
// Go type is big.Int, but it can encode from and decode to most numeric types.
var Varint Codec = &varintCodec{}

type varintCodec struct{}

func (c *varintCodec) DataType() datatype.DataType {
	return datatype.Varint
}

func (c *varintCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	var val *big.Int
	if val, err = convertToBigInt(source); err == nil && val != nil {
		dest = val.Bytes()
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *varintCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	val := readBigInt(source)
	wasNull = val == nil
	if err = convertFromBigInt(val, wasNull, dest); err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToBigInt(source interface{}) (val *big.Int, err error) {
	switch s := source.(type) {
	case int64:
		val = big.NewInt(s)
	case int:
		val = big.NewInt(int64(s))
	case int32:
		val = big.NewInt(int64(s))
	case int16:
		val = big.NewInt(int64(s))
	case int8:
		val = big.NewInt(int64(s))
	case uint64:
		val = new(big.Int).SetUint64(s)
	case uint:
		val = new(big.Int).SetUint64(uint64(s))
	case uint32:
		val = new(big.Int).SetUint64(uint64(s))
	case uint16:
		val = new(big.Int).SetUint64(uint64(s))
	case uint8:
		val = new(big.Int).SetUint64(uint64(s))
	case string:
		val, err = stringToBigInt(s)
	case *int64:
		if s != nil {
			val = big.NewInt(*s)
		}
	case *int:
		if s != nil {
			val = big.NewInt(int64(*s))
		}
	case *int32:
		if s != nil {
			val = big.NewInt(int64(*s))
		}
	case *int16:
		if s != nil {
			val = big.NewInt(int64(*s))
		}
	case *int8:
		if s != nil {
			val = big.NewInt(int64(*s))
		}
	case *uint64:
		if s != nil {
			val = new(big.Int).SetUint64(*s)
		}
	case *uint:
		if s != nil {
			val = new(big.Int).SetUint64(uint64(*s))
		}
	case *uint32:
		if s != nil {
			val = new(big.Int).SetUint64(uint64(*s))
		}
	case *uint16:
		if s != nil {
			val = new(big.Int).SetUint64(uint64(*s))
		}
	case *uint8:
		if s != nil {
			val = new(big.Int).SetUint64(uint64(*s))
		}
	case *big.Int:
		// Note: non-pointer big.Int is not supported as per its docs, it should always be a pointer.
		val = s
	case *string:
		if s != nil {
			val, err = stringToBigInt(*s)
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

func convertFromBigInt(val *big.Int, wasNull bool, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *int64:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = bigIntToInt64(val)
		}
	case *int:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = bigIntToInt(val)
		}
	case *int32:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = bigIntToInt32(val)
		}
	case *int16:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = bigIntToInt16(val)
		}
	case *int8:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = bigIntToInt8(val)
		}
	case *uint64:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = bigIntToUint64(val)
		}
	case *uint:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = bigIntToUint(val)
		}
	case *uint32:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = bigIntToUint32(val)
		}
	case *uint16:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = bigIntToUint16(val)
		}
	case *uint8:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = 0
		} else {
			*d, err = bigIntToUint8(val)
		}
	case *big.Int:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = big.Int{}
		} else {
			*d = *val
		}
	case *string:
		if d == nil {
			err = errNilDestination
		} else if wasNull {
			*d = ""
		} else {
			*d = val.Text(10)
		}
	default:
		err = errDestinationInvalid(dest)
	}
	if err != nil {
		err = errDestStoreFailed(val, dest, err)
	}
	return
}

var (
	zeroBigInt = big.NewInt(0)
	oneBigInt  = big.NewInt(1)
)

// Implementation note: the encoding scheme used for CQL varint is dictated by Java's implementation of
// BigInteger.toByteArray(). This scheme has nothing to do with the "Varint" functions declared in Go's binary package.
// Relevant readings for varint encoding in Go:
// https://groups.google.com/g/golang-nuts/c/TV4bRVrHZUw
// https://github.com/gocql/gocql/blob/go1.2/marshal.go#L729-L767

func writeBigInt(n *big.Int) []byte {
	if n == nil {
		return nil
	}
	switch n.Sign() {
	case 1:
		b := n.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}
		return b
	case -1:
		length := uint(n.BitLen()/8+1) * 8
		b := new(big.Int).Add(n, new(big.Int).Lsh(oneBigInt, length)).Bytes()
		// When the most significant bit is on a byte
		// boundary, we can get some extra significant
		// bits, so strip them off when that happens.
		if len(b) >= 2 && b[0] == 0xff && b[1]&0x80 != 0 {
			b = b[1:]
		}
		return b
	default:
		return []byte{0}
	}
}

func readBigInt(source []byte) (val *big.Int) {
	length := len(source)
	if length > 0 {
		val = new(big.Int).SetBytes(source)
		if source[0]&0x80 > 0 {
			val.Sub(val, new(big.Int).Lsh(oneBigInt, uint(length)*8))
		}
	}
	return
}
