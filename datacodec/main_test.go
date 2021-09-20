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

package datacodec

import (
	"encoding/binary"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/big"
	"net"
	"reflect"
	"testing"
	"time"
)

const (
	a = byte('a')
	b = byte('b')
	c = byte('c')
	d = byte('d')
	e = byte('e')
	f = byte('f')
)

func boolPtr(v bool) *bool            { return &v }
func intPtr(v int) *int               { return &v }
func int64Ptr(v int64) *int64         { return &v }
func int32Ptr(v int32) *int32         { return &v }
func int16Ptr(v int16) *int16         { return &v }
func int8Ptr(v int8) *int8            { return &v }
func uintPtr(v uint) *uint            { return &v }
func uint64Ptr(v uint64) *uint64      { return &v }
func uint32Ptr(v uint32) *uint32      { return &v }
func uint16Ptr(v uint16) *uint16      { return &v }
func uint8Ptr(v uint8) *uint8         { return &v }
func stringPtr(v string) *string      { return &v }
func float64Ptr(v float64) *float64   { return &v }
func float32Ptr(v float32) *float32   { return &v }
func boolNilPtr() *bool               { return nil }
func intNilPtr() *int                 { return nil }
func int64NilPtr() *int64             { return nil }
func int32NilPtr() *int32             { return nil }
func int16NilPtr() *int16             { return nil }
func int8NilPtr() *int8               { return nil }
func uintNilPtr() *uint               { return nil }
func uint64NilPtr() *uint64           { return nil }
func uint32NilPtr() *uint32           { return nil }
func uint16NilPtr() *uint16           { return nil }
func uint8NilPtr() *uint8             { return nil }
func stringNilPtr() *string           { return nil }
func bigIntNilPtr() *big.Int          { return nil }
func float64NilPtr() *float64         { return nil }
func float32NilPtr() *float32         { return nil }
func timeNilPtr() *time.Time          { return nil }
func durationNilPtr() *time.Duration  { return nil }
func cqlDecimalNilPtr() *CqlDecimal   { return nil }
func cqlDurationNilPtr() *CqlDuration { return nil }
func byteSliceNilPtr() *[]byte        { return nil }
func runeSliceNilPtr() *[]rune        { return nil }
func netIPNilPtr() *net.IP            { return nil }
func uuidNilPtr() *primitive.UUID     { return nil }

func encodeUint64(v uint64) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, v)
	return bs
}

func encodeUint32(v uint32) []byte {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, v)
	return bs
}

func encodeUint16(v uint16) []byte {
	bs := make([]byte, 2)
	binary.BigEndian.PutUint16(bs, v)
	return bs
}

// Assert that either the error is nil if the expected message is empty, or that the error message contains the expected
// message.
func assertErrorMessage(t *testing.T, expectedMessage string, actual error) {
	if expectedMessage == "" {
		assert.NoError(t, actual)
	} else {
		require.NotNil(t, actual)
		assert.Contains(t, actual.Error(), expectedMessage)
	}
}

// Special assertion logic for slices with elements of type interface{}.
func assertSlicesEqual(t *testing.T, expected []interface{}, actual []interface{}) {
	if assert.Len(t, actual, len(expected)) {
		for i, exp := range expected {
			act := actual[i]
			if exp == nil {
				assert.Nil(t, act, "expected element %d of slice %v to be nil", i, actual)
			} else {
				if assert.NotNil(t, act, "expected element %d of slice %v to be not nil", i, actual) {
					if assert.IsType(t, exp, act, "expected element %d of slice %v to be of type %T", i, actual, exp) {
						expV := reflect.ValueOf(exp)
						actV := reflect.ValueOf(act)
						if expV.Type().Kind() == reflect.Ptr {
							exp = reflect.Indirect(expV).Interface()
							act = reflect.Indirect(actV).Interface()
						}
						assert.Equal(t, exp, act, "expected element %d of slice %v to be equal to %v", i, actual, exp)
					}
				}
			}
		}
	}
}

type wrongDataType struct{}

func (w wrongDataType) String() string                          { return "666" }
func (w wrongDataType) GetDataTypeCode() primitive.DataTypeCode { return 666 }
func (w wrongDataType) Clone() datatype.DataType                { return &wrongDataType{} }
