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
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestNewMap(t *testing.T) {
	tests := []struct {
		name     string
		dataType datatype.MapType
		want     Codec
		wantErr  string
	}{
		{
			"nil",
			nil,
			nil,
			"data type is nil",
		},
		{
			"simple",
			datatype.NewMapType(datatype.Int, datatype.Varchar),
			&mapCodec{
				dataType:   datatype.NewMapType(datatype.Int, datatype.Varchar),
				keyCodec:   &intCodec{},
				valueCodec: &stringCodec{dataType: datatype.Varchar},
			},
			"",
		},
		{
			"complex",
			datatype.NewMapType(datatype.Int, datatype.NewMapType(datatype.Int, datatype.Varchar)),
			&mapCodec{
				dataType: datatype.NewMapType(datatype.Int, datatype.NewMapType(datatype.Int, datatype.Varchar)),
				keyCodec: &intCodec{},
				valueCodec: &mapCodec{
					dataType:   datatype.NewMapType(datatype.Int, datatype.Varchar),
					keyCodec:   &intCodec{},
					valueCodec: &stringCodec{dataType: datatype.Varchar},
				},
			},
			"",
		},
		{
			"wrong key type",
			datatype.NewMapType(wrongDataType{}, datatype.Int),
			nil,
			"cannot create codec for map keys: cannot create data codec for CQL type 666",
		},
		{
			"wrong value type",
			datatype.NewMapType(datatype.Int, wrongDataType{}),
			nil,
			"cannot create codec for map values: cannot create data codec for CQL type 666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := NewMap(tt.dataType)
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_mapCodec_Encode(t *testing.T) {
	mapSimple, _ := NewMap(datatype.NewMapType(datatype.Int, datatype.Varchar))
	mapComplex, _ := NewMap(datatype.NewMapType(datatype.Int, datatype.NewMapType(datatype.Int, datatype.Varchar)))
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				codec    Codec
				source   interface{}
				expected []byte
				err      string
			}{
				{"map<int,text> nil untyped", mapSimple, nil, nil, ""},
				{"map<int,text> nil slice", mapSimple, new(map[int]string), nil, ""},
				{"map<int,text> empty", mapSimple, map[int]string{}, []byte{0, 0, 0, 0}, ""},
				{"map<int,text> one elem", mapSimple, map[int]string{123: "abc"}, []byte{
					0, 0, 0, 1,
					0, 0, 0, 4,
					0, 0, 0, 123,
					0, 0, 0, 3,
					a, b, c,
				}, ""},
				{"map<int,text> non-empty", mapSimple, map[int]string{12: "abc"}, []byte{
					0, 0, 0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, ""},
				{"map<int,text> map pointer", mapSimple, &map[int]string{12: "abc"}, []byte{
					0, 0, 0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, ""},
				{"map<int,text> non-empty elems pointesr", mapSimple, map[*int]*string{intPtr(12): stringPtr("abc")}, []byte{
					0, 0, 0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, ""},
				{"map<int,text> non-empty map pointer elems pointers", mapSimple, &map[*int]*string{intPtr(12): stringPtr("abc")}, []byte{
					0, 0, 0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, ""},
				{"map<int,text> non-empty interface{}", mapSimple, map[int]interface{}{12: "abc"}, []byte{
					0, 0, 0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, ""},
				{
					"map<int,text> nil key",
					mapSimple,
					map[interface{}]interface{}{nil: "abc"},
					nil,
					fmt.Sprintf(
						"cannot encode map[interface {}]interface {} as CQL %s with %s: entry 0 key is nil",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> key encoded to nil",
					mapSimple,
					map[*int]interface{}{nil: "abc"},
					nil,
					fmt.Sprintf(
						"cannot encode map[*int]interface {} as CQL %s with %s: entry 0 key was encoded to nil",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> nil value",
					mapSimple,
					map[int]interface{}{12: nil},
					nil,
					fmt.Sprintf(
						"cannot encode map[int]interface {} as CQL %s with %s: entry 0 value is nil",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> value encoded to nil",
					mapSimple,
					map[int]*string{12: nil},
					nil,
					fmt.Sprintf(
						"cannot encode map[int]*string as CQL %s with %s: entry 0 value was encoded to nil",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> wrong source type",
					mapSimple,
					123,
					nil,
					fmt.Sprintf(
						"cannot encode int as CQL %s with %s: source type not supported",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> wrong source type nil",
					mapSimple,
					[]int(nil),
					nil,
					fmt.Sprintf(
						"cannot encode []int as CQL %s with %s: source type not supported",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> wrong source type nil pointer",
					mapSimple,
					new([]int),
					nil,
					fmt.Sprintf(
						"cannot encode *[]int as CQL %s with %s: source type not supported",
						mapSimple.DataType(),
						version,
					),
				},
				{"map<int,map<int,varchar>> nil untyped", mapComplex, nil, nil, ""},
				{"map<int,map<int,varchar>> nil slice", mapComplex, map[int]map[int]string(nil), nil, ""},
				{"map<int,map<int,varchar>> empty", mapComplex, map[int]map[int]string{}, []byte{0, 0, 0, 0}, ""},
				{"map<int,map<int,varchar>> non-empty", mapComplex, map[int]map[int]string{0: {12: "abc"}}, []byte{
					0, 0, 0, 1, // length of outer collection
					0, 0, 0, 4, // length of outer collection 1st key
					0, 0, 0, 0, // 1st key
					0, 0, 0, 19, // length of outer collection 1st value
					0, 0, 0, 1, // length of 1st inner collection
					0, 0, 0, 4, // length of 1st inner collection 1st key
					0, 0, 0, 12, // 1st inner collection 1st key
					0, 0, 0, 3, // length of 1st inner collection 1st value
					a, b, c, // 1st inner collection 1st value
				}, ""},
				{"map<int,map<int,varchar>> non-empty pointer", mapComplex, &map[int]map[int]string{0: {12: "abc"}}, []byte{
					0, 0, 0, 1, // length of outer collection
					0, 0, 0, 4, // length of outer collection 1st key
					0, 0, 0, 0, // 1st key
					0, 0, 0, 19, // length of outer collection 1st value
					0, 0, 0, 1, // length of 1st inner collection
					0, 0, 0, 4, // length of 1st inner collection 1st key
					0, 0, 0, 12, // 1st inner collection 1st key
					0, 0, 0, 3, // length of 1st inner collection 1st value
					a, b, c, // 1st inner collection 1st value
				}, ""},
				{"map<int,map<int,varchar>> non-empty pointer elem pointers", mapComplex, &map[*int]map[*int]*string{intPtr(0): {intPtr(12): stringPtr("abc")}}, []byte{
					0, 0, 0, 1, // length of outer collection
					0, 0, 0, 4, // length of outer collection 1st key
					0, 0, 0, 0, // 1st key
					0, 0, 0, 19, // length of outer collection 1st value
					0, 0, 0, 1, // length of 1st inner collection
					0, 0, 0, 4, // length of 1st inner collection 1st key
					0, 0, 0, 12, // 1st inner collection 1st key
					0, 0, 0, 3, // length of 1st inner collection 1st value
					a, b, c, // 1st inner collection 1st value
				}, ""},
				{"map<int,map<int,varchar>> interface", mapComplex, &map[interface{}]map[interface{}]interface{}{intPtr(0): {intPtr(12): stringPtr("abc")}}, []byte{
					0, 0, 0, 1, // length of outer collection
					0, 0, 0, 4, // length of outer collection 1st key
					0, 0, 0, 0, // 1st key
					0, 0, 0, 19, // length of outer collection 1st value
					0, 0, 0, 1, // length of 1st inner collection
					0, 0, 0, 4, // length of 1st inner collection 1st key
					0, 0, 0, 12, // 1st inner collection 1st key
					0, 0, 0, 3, // length of 1st inner collection 1st value
					a, b, c, // 1st inner collection 1st value
				}, ""},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := tt.codec.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				codec    Codec
				source   interface{}
				expected []byte
				err      string
			}{
				{"map<int,text> nil untyped", mapSimple, nil, nil, ""},
				{"map<int,text> nil slice", mapSimple, new(map[int]string), nil, ""},
				{"map<int,text> empty", mapSimple, map[int]string{}, []byte{0, 0}, ""},
				{"map<int,text> non-empty", mapSimple, map[int]string{12: "abc"}, []byte{
					0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, ""},
				{"map<int,text> non-empty pointer", mapSimple, &map[int]string{12: "abc"}, []byte{
					0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, ""},
				{"map<int,text> non-empty elems pointers", mapSimple, map[*int]*string{intPtr(12): stringPtr("abc")}, []byte{
					0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, ""},
				{"map<int,text> non-empty map pointer elems pointers", mapSimple, &map[*int]*string{intPtr(12): stringPtr("abc")}, []byte{
					0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, ""},
				{"map<int,text> non-empty interface{}", mapSimple, map[int]interface{}{12: "abc"}, []byte{
					0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, ""},
				{
					"map<int,text> nil key",
					mapSimple,
					map[interface{}]interface{}{nil: "abc"},
					nil,
					fmt.Sprintf(
						"cannot encode map[interface {}]interface {} as CQL %s with %s: entry 0 key is nil",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> key encoded to nil",
					mapSimple,
					map[*int]interface{}{nil: "abc"},
					nil,
					fmt.Sprintf(
						"cannot encode map[*int]interface {} as CQL %s with %s: entry 0 key was encoded to nil",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> nil value",
					mapSimple,
					map[int]interface{}{12: nil},
					nil,
					fmt.Sprintf(
						"cannot encode map[int]interface {} as CQL %s with %s: entry 0 value is nil",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> value encoded to nil",
					mapSimple,
					map[int]*string{12: nil},
					nil,
					fmt.Sprintf(
						"cannot encode map[int]*string as CQL %s with %s: entry 0 value was encoded to nil",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> wrong source type",
					mapSimple,
					123,
					nil,
					fmt.Sprintf(
						"cannot encode int as CQL %s with %s: source type not supported",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> wrong source type nil",
					mapSimple,
					[]int(nil),
					nil,
					fmt.Sprintf(
						"cannot encode []int as CQL %s with %s: source type not supported",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> wrong source type nil pointer",
					mapSimple,
					new([]int),
					nil,
					fmt.Sprintf(
						"cannot encode *[]int as CQL %s with %s: source type not supported",
						mapSimple.DataType(),
						version,
					),
				},
				{"map<int,map<int,varchar>> nil untyped", mapComplex, nil, nil, ""},
				{"map<int,map<int,varchar>> nil slice", mapComplex, map[int]map[int]string(nil), nil, ""},
				{"map<int,map<int,varchar>> empty", mapComplex, map[int]map[int]string{}, []byte{0, 0}, ""},
				{"map<int,map<int,varchar>> non-empty", mapComplex, map[int]map[int]string{0: {12: "abc"}}, []byte{
					0, 1, // length of outer collection
					0, 0, 0, 4, // length of outer collection 1st key
					0, 0, 0, 0, // 1st key
					0, 0, 0, 17, // length of outer collection 1st value
					0, 1, // length of 1st inner collection
					0, 0, 0, 4, // length of 1st inner collection 1st key
					0, 0, 0, 12, // 1st inner collection 1st key
					0, 0, 0, 3, // length of 1st inner collection 1st value
					a, b, c, // 1st inner collection 1st value
				}, ""},
				{"map<int,map<int,varchar>> non-empty pointer", mapComplex, &map[int]map[int]string{0: {12: "abc"}}, []byte{
					0, 1, // length of outer collection
					0, 0, 0, 4, // length of outer collection 1st key
					0, 0, 0, 0, // 1st key
					0, 0, 0, 17, // length of outer collection 1st value
					0, 1, // length of 1st inner collection
					0, 0, 0, 4, // length of 1st inner collection 1st key
					0, 0, 0, 12, // 1st inner collection 1st key
					0, 0, 0, 3, // length of 1st inner collection 1st value
					a, b, c, // 1st inner collection 1st value
				}, ""},
				{"map<int,map<int,varchar>> non-empty pointer elem pointers", mapComplex, &map[*int]map[*int]*string{intPtr(0): {intPtr(12): stringPtr("abc")}}, []byte{
					0, 1, // length of outer collection
					0, 0, 0, 4, // length of outer collection 1st key
					0, 0, 0, 0, // 1st key
					0, 0, 0, 17, // length of outer collection 1st value
					0, 1, // length of 1st inner collection
					0, 0, 0, 4, // length of 1st inner collection 1st key
					0, 0, 0, 12, // 1st inner collection 1st key
					0, 0, 0, 3, // length of 1st inner collection 1st value
					a, b, c, // 1st inner collection 1st value
				}, ""},
				{"map<int,map<int,varchar>> interface", mapComplex, &map[interface{}]map[interface{}]interface{}{intPtr(0): {intPtr(12): stringPtr("abc")}}, []byte{
					0, 1, // length of outer collection
					0, 0, 0, 4, // length of outer collection 1st key
					0, 0, 0, 0, // 1st key
					0, 0, 0, 17, // length of outer collection 1st value
					0, 1, // length of 1st inner collection
					0, 0, 0, 4, // length of 1st inner collection 1st key
					0, 0, 0, 12, // 1st inner collection 1st key
					0, 0, 0, 3, // length of 1st inner collection 1st value
					a, b, c, // 1st inner collection 1st value
				}, ""},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := tt.codec.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_mapCodec_Decode(t *testing.T) {
	mapSimple, _ := NewMap(datatype.NewMapType(datatype.Int, datatype.Varchar))
	mapComplex, _ := NewMap(datatype.NewMapType(datatype.Int, datatype.NewMapType(datatype.Int, datatype.Varchar)))
	for _, version := range primitive.SupportedProtocolVersionsLesserThan(primitive.ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				codec    Codec
				source   []byte
				dest     interface{}
				want     interface{}
				wantNull bool
				err      string
			}{
				{
					"map<int,text> nil untyped",
					mapSimple,
					nil,
					nil,
					nil,
					true,
					fmt.Sprintf("cannot decode CQL map<int,varchar> as <nil> with %s: destination is nil", version),
				},
				{"map<int,text> nil slice", mapSimple, nil, new(map[int]string), new(map[int]string), true, ""},
				{"map<int,text> empty", mapSimple, []byte{0, 0}, new(map[int]string), &map[int]string{}, false, ""},
				{"map<int,text> non-empty", mapSimple, []byte{
					0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, new(map[int]string), &map[int]string{12: "abc"}, false, ""},
				{"map<int,text> non-empty pointers", mapSimple, []byte{
					0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, new(map[int]*string), &map[int]*string{12: stringPtr("abc")}, false, ""},
				{"map<int,text> non-empty interface", mapSimple, []byte{
					0, 1,
					0, 0, 0, 4,
					0, 0, 0, 12,
					0, 0, 0, 3,
					a, b, c,
				}, new(map[interface{}]interface{}), &map[interface{}]interface{}{12: "abc"}, false, ""},
				{
					"map<int,text> pointer required",
					mapSimple,
					nil,
					map[int]string{},
					map[int]string{},
					true,
					fmt.Sprintf(
						"cannot decode CQL %s as map[int]string with %v: destination is not pointer",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,text> destination type not supported",
					mapSimple,
					nil,
					new([]int),
					new([]int),
					true,
					fmt.Sprintf(
						"cannot decode CQL %s as *[]int with %v: destination type not supported",
						mapSimple.DataType(),
						version,
					),
				},
				{
					"map<int,map<int,varchar>> nil untyped",
					mapComplex,
					nil,
					nil,
					nil,
					true,
					fmt.Sprintf("cannot decode CQL map<int,map<int,varchar>> as <nil> with %s: destination is nil", version),
				},
				{"map<int,map<int,varchar>> nil slice", mapComplex, nil, new(map[int]map[int]string), new(map[int]map[int]string), true, ""},
				{"map<int,map<int,varchar>> empty", mapComplex, []byte{0, 0}, new(map[int]map[int]string), &map[int]map[int]string{}, false, ""},
				{"map<int,map<int,varchar>> non-empty", mapComplex, []byte{
					0, 1, // length of outer collection
					0, 0, 0, 4, // length of outer collection 1st key
					0, 0, 0, 0, // 1st key
					0, 0, 0, 17, // length of outer collection 1st value
					0, 1, // length of 1st inner collection
					0, 0, 0, 4, // length of 1st inner collection 1st key
					0, 0, 0, 12, // 1st inner collection 1st key
					0, 0, 0, 3, // length of 1st inner collection 1st value
					a, b, c, // 1st inner collection 1st value
				}, new(map[int]map[int]string), &map[int]map[int]string{0: {12: "abc"}}, false, ""},
				{"map<int,map<int,varchar>> non-empty pointers", mapComplex, []byte{
					0, 1, // length of outer collection
					0, 0, 0, 4, // length of outer collection 1st key
					0, 0, 0, 0, // 1st key
					0, 0, 0, 17, // length of outer collection 1st value
					0, 1, // length of 1st inner collection
					0, 0, 0, 4, // length of 1st inner collection 1st key
					0, 0, 0, 12, // 1st inner collection 1st key
					0, 0, 0, 3, // length of 1st inner collection 1st value
					a, b, c, // 1st inner collection 1st value
				}, new(map[int]map[int]*string), &map[int]map[int]*string{0: {12: stringPtr("abc")}}, false, ""},
				{"map<int,map<int,varchar>> non-empty interface{}", mapComplex, []byte{
					0, 1, // length of outer collection
					0, 0, 0, 4, // length of outer collection 1st key
					0, 0, 0, 0, // 1st key
					0, 0, 0, 17, // length of outer collection 1st value
					0, 1, // length of 1st inner collection
					0, 0, 0, 4, // length of 1st inner collection 1st key
					0, 0, 0, 12, // 1st inner collection 1st key
					0, 0, 0, 3, // length of 1st inner collection 1st value
					a, b, c, // 1st inner collection 1st value
				}, new(map[interface{}]map[interface{}]interface{}), &map[interface{}]map[interface{}]interface{}{0: {12: "abc"}}, false, ""},
				{
					"map<int,map<int,varchar>> pointer required",
					mapComplex,
					nil,
					map[int]map[int]string{},
					map[int]map[int]string{},
					true,
					fmt.Sprintf(
						"cannot decode CQL %s as map[int]map[int]string with %s: destination is not pointer",
						mapComplex.DataType(),
						version,
					),
				},
				{
					"map<int,map<int,varchar>> wrong destination type",
					mapComplex,
					nil,
					new([]string),
					new([]string),
					true,
					fmt.Sprintf(
						"cannot decode CQL %s as *[]string with %s: destination type not supported",
						mapComplex.DataType(),
						version,
					),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := tt.codec.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.want, tt.dest)
					assert.Equal(t, tt.wantNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_writeMap(t *testing.T) {
	type args struct {
		ext        keyValueExtractor
		keyCodec   Codec
		valueCodec Codec
		size       int
		version    primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr string
	}{
		{
			"cannot write size",
			args{nil, nil, nil, -1, primitive.ProtocolVersion5},
			nil,
			"cannot write collection size: expected collection size >= 0, got: -1",
		},
		{
			"nil key",
			args{func() keyValueExtractor {
				ext := &mockKeyValueExtractor{}
				ext.On("getKey", 0).Return(nil, nil)
				return ext
			}(), nil, nil, 1, primitive.ProtocolVersion5},
			nil,
			"entry 0 key is nil",
		},
		{
			"cannot extract value",
			args{func() keyValueExtractor {
				ext := &mockKeyValueExtractor{}
				ext.On("getKey", 0).Return(123, nil)
				ext.On("getElem", 0, 123).Return(nil, errors.New("cannot extract elem"))
				return ext
			}(), nil, nil, 1, primitive.ProtocolVersion5},
			nil,
			"cannot extract entry 0 value: cannot extract elem",
		},
		{
			"extracted value is nil",
			args{func() keyValueExtractor {
				ext := &mockKeyValueExtractor{}
				ext.On("getKey", 0).Return(123, nil)
				ext.On("getElem", 0, 123).Return(nil, nil)
				return ext
			}(), nil, nil, 1, primitive.ProtocolVersion5},
			nil,
			"entry 0 value is nil",
		},
		{
			"cannot encode key",
			args{
				func() keyValueExtractor {
					ext := &mockKeyValueExtractor{}
					ext.On("getKey", 0).Return(123, nil)
					ext.On("getElem", 0, 123).Return("abc", nil)
					return ext
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", 123, primitive.ProtocolVersion5).Return(nil, errors.New("write key failed"))
					return codec
				}(),
				nil,
				1,
				primitive.ProtocolVersion5,
			},
			nil,
			"cannot encode entry 0 key: write key failed",
		},
		{
			"cannot encode value",
			args{
				func() keyValueExtractor {
					ext := &mockKeyValueExtractor{}
					ext.On("getKey", 0).Return(123, nil)
					ext.On("getElem", 0, 123).Return("abc", nil)
					return ext
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", 123, primitive.ProtocolVersion5).Return([]byte{1}, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", "abc", primitive.ProtocolVersion5).Return(nil, errors.New("write value failed"))
					return codec
				}(),
				1,
				primitive.ProtocolVersion5,
			},
			nil,
			"cannot encode entry 0 value: write value failed",
		},
		{
			"encoded key nil",
			args{
				func() keyValueExtractor {
					ext := &mockKeyValueExtractor{}
					ext.On("getKey", 0).Return(123, nil)
					ext.On("getElem", 0, 123).Return("abc", nil)
					return ext
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", 123, primitive.ProtocolVersion5).Return(nil, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", "abc", primitive.ProtocolVersion5).Return([]byte{1}, nil)
					return codec
				}(),
				1,
				primitive.ProtocolVersion5,
			},
			nil,
			"entry 0 key was encoded to nil",
		},
		{
			"encoded value nil",
			args{
				func() keyValueExtractor {
					ext := &mockKeyValueExtractor{}
					ext.On("getKey", 0).Return(123, nil)
					ext.On("getElem", 0, 123).Return("abc", nil)
					return ext
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", 123, primitive.ProtocolVersion5).Return([]byte{1}, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", "abc", primitive.ProtocolVersion5).Return(nil, nil)
					return codec
				}(),
				1,
				primitive.ProtocolVersion5,
			},
			nil,
			"entry 0 value was encoded to nil",
		},
		{"success",
			args{
				func() keyValueExtractor {
					ext := &mockKeyValueExtractor{}
					ext.On("getKey", 0).Return(12, nil)
					ext.On("getElem", 0, 12).Return("abc", nil)
					ext.On("getKey", 1).Return(34, nil)
					ext.On("getElem", 1, 34).Return("def", nil)
					return ext
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", 12, primitive.ProtocolVersion5).Return([]byte{12}, nil)
					codec.On("Encode", 34, primitive.ProtocolVersion5).Return([]byte{34}, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("Encode", "abc", primitive.ProtocolVersion5).Return([]byte{a, b, c}, nil)
					codec.On("Encode", "def", primitive.ProtocolVersion5).Return([]byte{d, e, f}, nil)
					return codec
				}(),
				2,
				primitive.ProtocolVersion5,
			},
			[]byte{
				0, 0, 0, 2, // size
				0, 0, 0, 1, // elem 1 key
				12,
				0, 0, 0, 3, // elem 1 value
				a, b, c,
				0, 0, 0, 1, // elem 2 key
				34,
				0, 0, 0, 3, // elem 2 value
				d, e, f,
			}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := writeMap(tt.args.ext, tt.args.size, tt.args.keyCodec, tt.args.valueCodec, tt.args.version)
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_readMap(t *testing.T) {
	type args struct {
		source     []byte
		inj        keyValueInjector
		keyCodec   Codec
		valueCodec Codec
		version    primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		args    args
		wantErr string
	}{
		{
			"cannot read size",
			args{[]byte{1}, nil, nil, nil, primitive.ProtocolVersion5},
			"cannot read collection size: cannot read [int]: unexpected EOF",
		},
		{
			"cannot read key",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, // wrong [bytes]
				},
				func() keyValueInjector {
					inj := &mockKeyValueInjector{}
					inj.On("ensureSize", 1)
					return inj
				}(),
				nil,
				nil,
				primitive.ProtocolVersion5,
			},
			"cannot read entry 0 key: cannot read [bytes] length: cannot read [int]: unexpected EOF",
		},
		{
			"cannot read value",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, // wrong [bytes]
				},
				func() keyValueInjector {
					inj := &mockKeyValueInjector{}
					inj.On("ensureSize", 1)
					return inj
				}(),
				nil,
				nil,
				primitive.ProtocolVersion5,
			},
			"cannot read entry 0 value: cannot read [bytes] length: cannot read [int]: unexpected EOF",
		},
		{
			"cannot create key",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 1, // value
				},
				func() keyValueInjector {
					inj := &mockKeyValueInjector{}
					inj.On("ensureSize", 1)
					inj.On("zeroKey", 0, datatype.Int).Return(nil, errors.New("wrong data type"))
					return inj
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					return codec
				}(),
				nil,
				primitive.ProtocolVersion5,
			},
			"cannot create zero entry 0 key: wrong data type",
		},
		{
			"cannot decode key",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 1, // value
				},
				func() keyValueInjector {
					inj := &mockKeyValueInjector{}
					inj.On("ensureSize", 1)
					inj.On("zeroKey", 0, datatype.Int).Return(new(int), nil)
					return inj
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Return(false, errors.New("decode key failed"))
					return codec
				}(),
				nil,
				primitive.ProtocolVersion5,
			},
			"cannot decode entry 0 key: decode key failed",
		},
		{
			"cannot create value",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 1, // value
				},
				func() keyValueInjector {
					inj := &mockKeyValueInjector{}
					inj.On("ensureSize", 1)
					inj.On("zeroKey", 0, datatype.Int).Return(new(int), nil)
					inj.On("zeroElem", 0, intPtr(12), datatype.Varchar).Return(nil, errors.New("wrong data type"))
					return inj
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 12
					}).Return(false, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Varchar)
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot create zero entry 0 value: wrong data type",
		},
		{
			"cannot decode value",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 2, // value
				},
				func() keyValueInjector {
					inj := &mockKeyValueInjector{}
					inj.On("ensureSize", 1)
					inj.On("zeroKey", 0, datatype.Int).Return(new(int), nil)
					inj.On("zeroElem", 0, intPtr(12), datatype.Varchar).Return(new(string), nil)
					return inj
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 12
					}).Return(false, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Varchar)
					codec.On("Decode", []byte{2}, new(string), primitive.ProtocolVersion5).Return(false, errors.New("decode value failed"))
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot decode entry 0 value: decode value failed",
		},
		{
			"cannot set element",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 2, // value
				},
				func() keyValueInjector {
					inj := &mockKeyValueInjector{}
					inj.On("ensureSize", 1)
					inj.On("zeroKey", 0, datatype.Int).Return(new(int), nil)
					inj.On("zeroElem", 0, intPtr(12), datatype.Varchar).Return(new(string), nil)
					inj.On("setElem", 0, intPtr(12), stringPtr("abc"), false, false, datatype.Int, datatype.Varchar).Return(errors.New("cannot set elem"))
					return inj
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 12
					}).Return(false, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Varchar)
					codec.On("Decode", []byte{2}, new(string), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*string)
						*decodedElement = "abc"
					}).Return(false, nil)
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"cannot inject entry 0: cannot set elem",
		},
		{
			"bytes remaining",
			args{
				[]byte{
					0, 0, 0, 1, // size
					0, 0, 0, 1, 1, // key
					0, 0, 0, 1, 2, // value
					1, // trailing
				},
				func() keyValueInjector {
					inj := &mockKeyValueInjector{}
					inj.On("ensureSize", 1)
					inj.On("zeroKey", 0, datatype.Int).Return(new(int), nil)
					inj.On("zeroElem", 0, intPtr(12), datatype.Varchar).Return(new(string), nil)
					inj.On("setElem", 0, intPtr(12), stringPtr("abc"), false, false, datatype.Int,
						datatype.Varchar).Return(nil)
					return inj
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 12
					}).Return(false, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Varchar)
					codec.On("Decode", []byte{2}, new(string), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*string)
						*decodedElement = "abc"
					}).Return(false, nil)
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"source was not fully read: bytes total: 15, read: 14, remaining: 1",
		},
		{
			"success",
			args{
				[]byte{
					0, 0, 0, 2, // size
					0, 0, 0, 1, 1, // key1
					0, 0, 0, 1, 2, // value1
					0, 0, 0, 1, 3, // key2
					0, 0, 0, 1, 4, // value2
				},
				func() keyValueInjector {
					inj := &mockKeyValueInjector{}
					inj.On("ensureSize", 2)
					inj.On("zeroKey", 0, datatype.Int).Return(new(int), nil)
					inj.On("zeroKey", 1, datatype.Int).Return(new(int), nil)
					inj.On("zeroElem", 0, intPtr(12), datatype.Varchar).Return(new(string), nil)
					inj.On("zeroElem", 1, intPtr(34), datatype.Varchar).Return(new(string), nil)
					inj.On("setElem", 0, intPtr(12), stringPtr("abc"), false, false, datatype.Int, datatype.Varchar).Return(nil)
					inj.On("setElem", 1, intPtr(34), stringPtr("def"), false, false, datatype.Int, datatype.Varchar).Return(nil)
					return inj
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Int)
					codec.On("Decode", []byte{1}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 12
					}).Return(false, nil)
					// the call should be with
					codec.On("Decode", []byte{3}, new(int), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*int)
						*decodedElement = 34
					}).Return(false, nil)
					return codec
				}(),
				func() Codec {
					codec := &mockCodec{}
					codec.On("DataType").Return(datatype.Varchar)
					codec.On("Decode", []byte{2}, new(string), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*string)
						*decodedElement = "abc"
					}).Return(false, nil)
					codec.On("Decode", []byte{4}, new(string), primitive.ProtocolVersion5).Run(func(args mock.Arguments) {
						decodedElement := args.Get(1).(*string)
						*decodedElement = "def"
					}).Return(false, nil)
					return codec
				}(),
				primitive.ProtocolVersion5,
			},
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := readMap(tt.args.source, tt.args.inj, tt.args.keyCodec, tt.args.valueCodec, tt.args.version)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}
