// Copyright 2021 DataStax
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

package primitive

import (
	"fmt"
	"io"
	"math/bits"
)

// [unsigned vint] and [vint] are protocol-level structures.
// They were first introduced in DSE protocols v1 and v2, then introduced in OSS protocol v5.
// Since they are declared in section 3 of protocol specs, they are handled in the primitive package.
// However, they are currently only used for encoding and decoding the CQL duration type, also introduced in the
// same versions above.

// [unsigned vint]
// An unsigned variable length integer. A vint is encoded with the most significant byte (MSB) first.
// The most significant byte will contain the information about how many extra bytes need to be read
// as well as the most significant bits of the integer.
// The number of extra bytes to read is encoded as 1 bit on the left side.
// For example, if we need to read 2 more bytes the first byte will start with 110
// (e.g. 256 000 will be encoded on 3 bytes as [110]00011 11101000 00000000)
// If the encoded integer is 8 bytes long, the vint will be encoded on 9 bytes and the first
// byte will be: 11111111.

// Implementation note:
// the binary package has the functions: PutVarint, PutUvarint, ReadVarint and ReadUvarint. The encoding scheme
// used by these functions is similar to the one used here, but unfortunately Cassandra vints are big-endian,
// while varints, in the functions above, are encoded in little-endian order.

func ReadUnsignedVint(source io.Reader) (val uint64, read int, err error) {
	data, err := io.ReadAll(source)
	if err == nil {
		length := len(data)
		if length == 0 {
			err = io.EOF
		} else {
			firstByte := data[0]
			read = 1
			if firstByte&0x80 == 0 {
				val = uint64(firstByte)
			} else {
				numBytes := bits.LeadingZeros32(uint32(^firstByte)) - 24
				if length < numBytes+1 {
					read = length
					err = io.EOF
				} else {
					read += numBytes
					val = uint64(firstByte & (0xff >> uint(numBytes)))
					for i := 0; i < numBytes; i++ {
						val <<= 8
						val |= uint64(data[i+1] & 0xff)
					}
				}
			}
		}
	}
	if err != nil {
		err = fmt.Errorf("cannot read [unsigned vint]: %w", err)
	}
	return
}

func WriteUnsignedVint(v uint64, dest io.Writer) (written int, err error) {
	magnitude := bits.LeadingZeros64(v)
	numBytes := (639 - magnitude*9) >> 6
	// It can be 1 or 0 is v ==0
	if numBytes <= 1 {
		written, err = dest.Write([]byte{byte(v)})
	} else {
		extraBytes := numBytes - 1
		var buf = make([]byte, numBytes)
		for i := extraBytes; i >= 0; i-- {
			buf[i] = byte(v)
			v >>= 8
		}
		buf[0] |= byte(^(0xff >> uint(extraBytes)))
		written, err = dest.Write(buf)
	}
	if err != nil {
		err = fmt.Errorf("cannot write [unsigned vint]: %w", err)
	}
	return
}

func LengthOfUnsignedVint(v uint64) int {
	magnitude := bits.LeadingZeros64(v)
	numBytes := (639 - magnitude*9) >> 6
	// It can be 1 or 0 is v ==0
	if numBytes <= 1 {
		return 1
	}
	return numBytes
}

// [vint]
// A signed variable length integer. This is encoded using zig-zag encoding and then sent
// like an [unsigned vint]. Zig-zag encoding converts numbers as follows:
// 0 = 0, -1 = 1, 1 = 2, -2 = 3, 2 = 4, -3 = 5, 3 = 6 and so forth.
// The purpose is to send small negative values as small unsigned values, so that we save bytes on the wire.
// To encode a value n use "(n >> 31) ^ (n << 1)" for 32 bit values, and "(n >> 63) ^ (n << 1)"
// for 64 bit values where "^" is the xor operation, "<<" is the left shift operation and ">>" is
// the arithmetic right shift operation (highest-order bit is replicated).
// Decode with "(n >> 1) ^ -(n & 1)".

func ReadVint(source io.Reader) (val int64, read int, err error) {
	var unsigned uint64
	unsigned, read, err = ReadUnsignedVint(source)
	if err != nil {
		err = fmt.Errorf("cannot read [vint]: %w", err)
	} else {
		val = decodeZigZag(unsigned)
	}
	return
}

func WriteVint(v int64, dest io.Writer) (written int, err error) {
	written, err = WriteUnsignedVint(encodeZigZag(v), dest)
	if err != nil {
		err = fmt.Errorf("cannot write [vint]: %w", err)
	}
	return
}

func LengthOfVint(v int64) int {
	return LengthOfUnsignedVint(encodeZigZag(v))
}

func decodeZigZag(n uint64) int64 {
	return int64((n >> 1) ^ -(n & 1))
}

func encodeZigZag(n int64) uint64 {
	return uint64((n >> 63) ^ (n << 1))
}
