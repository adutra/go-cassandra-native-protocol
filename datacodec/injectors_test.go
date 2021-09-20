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
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/big"
	"net"
	"reflect"
	"testing"
)

type testStruct struct {
	Value        int
	Pointer      *string
	Slice        []int
	PointerSlice []*string `cassandra:"pointer_slice"`
	Array        [2]int
	PointerArray [2]*string `cassandra:"pointer_array"`
	Map          map[string]interface{}
	Untyped      interface{}
	unexported   bool
}

func Test_newSliceInjector(t *testing.T) {
	var nilSlice []int
	tests := []struct {
		name      string
		dest      reflect.Value
		wantValue bool
		wantErr   string
	}{
		{"nil", reflect.Value{}, false, "destination type not supported"},
		{"nil typed", reflect.ValueOf(&nilSlice).Elem(), true, ""},
		{"not array nor slice", reflect.ValueOf(&map[string]int{}).Elem(), false, "expected slice or array, got: map[string]int"},
		{"unaddressable", reflect.ValueOf([]int{}), true, "destination of type []int is not addressable"},
		{"success", reflect.ValueOf(&[]int{1}).Elem(), true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, gotErr := newSliceInjector(tt.dest)
			assert.True(t, tt.wantValue || gotValue == nil)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}
func Test_newStructInjector(t *testing.T) {
	tests := []struct {
		name      string
		dest      reflect.Value
		wantValue bool
		wantErr   string
	}{
		{"nil", reflect.Value{}, false, "destination type not supported"},
		{"not struct", reflect.ValueOf(&map[string]int{}).Elem(), false, "expected struct, got: map[string]int"},
		{"unaddressable", reflect.ValueOf(testStruct{}), true, "destination of type datacodec.testStruct is not addressable"},
		{"success", reflect.ValueOf(&testStruct{}).Elem(), true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, gotErr := newStructInjector(tt.dest)
			assert.True(t, tt.wantValue || gotValue == nil)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_newMapInjector(t *testing.T) {
	var nilMap map[int]int
	tests := []struct {
		name      string
		dest      reflect.Value
		wantValue bool
		wantErr   string
	}{
		{"nil", reflect.Value{}, false, "destination type not supported"},
		{"nil typed", reflect.ValueOf(&nilMap).Elem(), true, ""},
		{"not map", reflect.ValueOf(&[]int{}).Elem(), false, "expected map, got: []int"},
		{"unaddressable", reflect.ValueOf(map[int]int{123: 456}), true, "destination of type map[int]int is not addressable"},
		{"success", reflect.ValueOf(&map[int]int{123: 456}).Elem(), true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, gotErr := newMapInjector(tt.dest)
			assert.True(t, tt.wantValue || gotValue == nil)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_sliceInjector_zeroElem(t *testing.T) {
	tests := []struct {
		name      string
		dest      interface{}
		dt        datatype.DataType
		wantValue interface{}
		wantErr   string
	}{
		{"[]int zero value", []int{0}, datatype.Int, intPtr(0), ""},
		{"[]int non zero value", []int{123}, datatype.Int, intPtr(0), ""},
		{"[]int datatype ignored", []int{123}, datatype.Varchar, intPtr(0), ""},
		{"[]interface nil", []interface{}{nil}, datatype.Int, intPtr(0), ""},
		{"[]interface value ignored", []interface{}{123}, datatype.Varchar, stringPtr(""), ""},
		{"[1]interface value", [1]interface{}{123}, datatype.Int, intPtr(0), ""},
		{"[1]interface nil", [1]interface{}{nil}, datatype.Int, intPtr(0), ""},
		{"[1]interface value ignored", [1]interface{}{123}, datatype.Varchar, stringPtr(""), ""},
		{"wrong data type", []interface{}{123, 456}, wrongDataType{}, nil, "could not find any suitable Go type for CQL type 666"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := pointerTo(reflect.ValueOf(tt.dest)).Elem()
			i, err := newSliceInjector(dest)
			require.NoError(t, err)
			gotValue, gotErr := i.zeroElem(-1, nil, tt.dt)
			assert.Equal(t, tt.wantValue, gotValue)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_structInjector_zeroElem(t *testing.T) {
	tests := []struct {
		name      string
		key       interface{}
		dt        datatype.DataType
		wantValue interface{}
		wantErr   string
	}{
		{"by index value", 0, nil, new(int), ""},
		{"by index pointer", 1, nil, new(string), ""},
		{"by index slice", 2, nil, new([]int), ""},
		{"by index pointer slice", 3, nil, new([]*string), ""},
		{"by index array", 4, nil, new([2]int), ""},
		{"by index pointer array", 5, nil, new([2]*string), ""},
		{"by index map", 6, nil, new(map[string]interface{}), ""},
		{"by index untyped int", 7, datatype.Int, new(int), ""},
		{"by index untyped varchar", 7, datatype.Varchar, new(string), ""},
		{"by index untyped list<inet>", 7, datatype.NewListType(datatype.Inet), new([]net.IP), ""},
		{"by index out of range neg", -1, nil, nil, "no accessible field with index -1 found in struct datacodec.testStruct"},
		{"by index out of range pos", 100, nil, nil, "no accessible field with index 100 found in struct datacodec.testStruct"},
		{"by name value", "value", nil, new(int), ""},
		{"by name pointer", "pointer", nil, new(string), ""},
		{"by name slice", "slice", nil, new([]int), ""},
		{"by name pointer slice", "pointer_slice", nil, new([]*string), ""},
		{"by name array", "array", nil, new([2]int), ""},
		{"by name pointer array", "pointer_array", nil, new([2]*string), ""},
		{"by name map", "MAP", nil, new(map[string]interface{}), ""},
		{"by name untyped int", "untyped", datatype.Int, new(int), ""},
		{"by name untyped varchar", "UNTYPED", datatype.Varchar, new(string), ""},
		{"by name untyped list<inet>", "Untyped", datatype.NewListType(datatype.Inet), new([]net.IP), ""},
		{"by name unexported", "unexported", nil, nil, "no accessible field with name 'unexported' found in struct datacodec.testStruct"},
		{"by name nonexistent", "nonexistent", nil, nil, "no accessible field with name 'nonexistent' found in struct datacodec.testStruct"},
		{"wrong key", true, nil, nil, "wrong struct field key, expected int or string, got: 'true'"},
		{"wrong data type", "Untyped", wrongDataType{}, nil, "could not find any suitable Go type for CQL type 666"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i, err := newStructInjector(reflect.ValueOf(&testStruct{}).Elem())
			require.NoError(t, err)
			gotValue, gotErr := i.zeroElem(-1, tt.key, tt.dt)
			assert.Equal(t, tt.wantValue, gotValue)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_mapInjector_zeroKey(t *testing.T) {
	tests := []struct {
		name      string
		dest      interface{}
		dt        datatype.DataType
		wantValue interface{}
		wantErr   string
	}{
		{"map[string]interface{}", map[string]interface{}{}, nil, new(string), ""},
		{"map[interface{}]int", map[interface{}]int{}, datatype.Varchar, new(string), ""},
		{"wrong data type", map[interface{}]int{}, &wrongDataType{}, nil, "could not find any suitable Go type for CQL type 666"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := pointerTo(reflect.ValueOf(tt.dest)).Elem()
			i, err := newMapInjector(dest)
			require.NoError(t, err)
			gotValue, gotErr := i.zeroKey(0, tt.dt)
			assert.Equal(t, tt.wantValue, gotValue)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_mapInjector_zeroElem(t *testing.T) {
	tests := []struct {
		name      string
		dest      interface{}
		dt        datatype.DataType
		wantValue interface{}
		wantErr   string
	}{
		{"map[string]interface{}", map[string]interface{}{}, datatype.Varchar, new(string), ""},
		{"map[int]int", map[int]int{}, nil, new(int), ""},
		{"map[string][]map[string]interface{}", map[string][]map[string]interface{}{}, nil, new([]map[string]interface{}), ""},
		{"wrong data type", map[string]interface{}{}, &wrongDataType{}, nil, "could not find any suitable Go type for CQL type 666"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := pointerTo(reflect.ValueOf(tt.dest)).Elem()
			i, err := newMapInjector(dest)
			require.NoError(t, err)
			gotValue, gotErr := i.zeroElem(-1, "", tt.dt)
			assert.Equal(t, tt.wantValue, gotValue)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_sliceInjector_setElem(t *testing.T) {
	tests := []struct {
		name    string
		index   int
		value   interface{}
		wasNull bool
		dt      datatype.DataType
		dest    interface{}
		want    interface{}
		wantErr string
	}{
		{"[]int null", 0, new(int), true, datatype.Int, []int{0}, []int{0}, ""},
		{"[]int non null", 0, intPtr(1), false, datatype.Int, []int{0}, []int{1}, ""},
		{"[]*int null", 0, new(int), true, datatype.Int, []*int{nil}, []*int{nil}, ""},
		{"[]*int non null", 0, intPtr(1), false, datatype.Int, []*int{nil}, []*int{intPtr(1)}, ""},
		{"[]*int non null multi", 1, intPtr(1), false, datatype.Int, []*int{nil, nil}, []*int{nil, intPtr(1)}, ""},
		{"[]int out of range neg", -1, intPtr(1), false, datatype.Int, []int{123, 456}, []int{123, 456}, "slice index out of range: -1"},
		{"[]int out of range neg", 2, intPtr(1), false, datatype.Int, []int{123, 456}, []int{123, 456}, "slice index out of range: 2"},
		{"[]interface{} null", 0, new(int), true, datatype.Int, []interface{}{nil}, []interface{}{nil}, ""},
		{"[]interface{} non null", 0, intPtr(1), false, datatype.Int, []interface{}{nil}, []interface{}{1}, ""},
		{"[]interface{} wrong type", 0, intPtr(1), false, datatype.Varchar, []interface{}{nil}, []interface{}{nil}, "wrong slice element, expected string, got: '1'"},
		{"[]int wrong value", 0, stringPtr("abc"), false, datatype.Int, []int{123}, []int{123}, "wrong slice element, expected int, got: 'abc'"},
		{"[1]int wrong value", 0, stringPtr("abc"), false, datatype.Int, [1]int{123}, [1]int{123}, "wrong array element, expected int, got: 'abc'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := pointerTo(reflect.ValueOf(tt.dest)).Elem()
			i, err := newSliceInjector(dest)
			require.NoError(t, err)
			gotErr := i.setElem(tt.index, nil, tt.value, false, tt.wasNull, nil, tt.dt)
			assert.Equal(t, tt.want, tt.dest)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_structInjector_setElem(t *testing.T) {
	tests := []struct {
		name    string
		key     interface{}
		value   interface{}
		wasNull bool
		dt      datatype.DataType
		want    testStruct
		wantErr string
	}{
		{"by index value null", 0, new(int), true, nil, testStruct{}, ""},
		{"by index value non null", 0, intPtr(1), false, nil, testStruct{Value: 1}, ""},
		{"by index pointer null", 1, new(string), true, nil, testStruct{}, ""},
		{"by index pointer non null", 1, stringPtr("abc"), false, nil, testStruct{Pointer: stringPtr("abc")}, ""},
		{"by index slice null", 2, new([]int), true, nil, testStruct{}, ""},
		{"by index slice non null", 2, &[]int{123, 456}, false, nil, testStruct{Slice: []int{123, 456}}, ""},
		{"by index pointer slice null", 3, new([]*string), true, nil, testStruct{}, ""},
		{"by index pointer slice non null", 3, &[]*string{stringPtr("abc"), nil}, false, nil, testStruct{PointerSlice: []*string{stringPtr("abc"), nil}}, ""},
		{"by index array null", 4, new([2]int), true, nil, testStruct{}, ""},
		{"by index array non null", 4, &[2]int{123, 456}, false, nil, testStruct{Array: [2]int{123, 456}}, ""},
		{"by index pointer array null", 5, new([2]*string), true, nil, testStruct{}, ""},
		{"by index pointer array non null", 5, &[2]*string{stringPtr("abc"), nil}, false, nil, testStruct{PointerArray: [2]*string{stringPtr("abc"), nil}}, ""},
		{"by index map null", 6, new(map[string]interface{}), true, nil, testStruct{}, ""},
		{"by index map non null", 6, &map[string]interface{}{"abc": 123, "def": nil}, false, nil, testStruct{Map: map[string]interface{}{"abc": 123, "def": nil}}, ""},
		{"by index untyped null", 7, new(interface{}), true, nil, testStruct{}, ""},
		{"by index untyped non null", 7, stringPtr("abc"), false, datatype.Varchar, testStruct{Untyped: "abc"}, ""},
		{"by index untyped wrong type", 7, stringPtr("abc"), false, datatype.Int, testStruct{Untyped: "abc"}, ""},
		{"by name value null", "Value", new(int), true, nil, testStruct{}, ""},
		{"by name value non null", "Value", intPtr(1), false, nil, testStruct{Value: 1}, ""},
		{"by name pointer null", "Pointer", new(string), true, nil, testStruct{}, ""},
		{"by name pointer non null", "Pointer", stringPtr("abc"), false, nil, testStruct{Pointer: stringPtr("abc")}, ""},
		{"by name slice null", "Slice", new([]int), true, nil, testStruct{}, ""},
		{"by name slice non null", "Slice", &[]int{123, 456}, false, nil, testStruct{Slice: []int{123, 456}}, ""},
		{"by name pointer slice null", "pointer_slice", new([]*string), true, nil, testStruct{}, ""},
		{"by name pointer slice non null", "pointer_slice", &[]*string{stringPtr("abc"), nil}, false, nil, testStruct{PointerSlice: []*string{stringPtr("abc"), nil}}, ""},
		{"by name array null", "array", new([2]int), true, nil, testStruct{}, ""},
		{"by name array non null", "array", &[2]int{123, 456}, false, nil, testStruct{Array: [2]int{123, 456}}, ""},
		{"by name pointer array null", "pointer_array", new([2]*string), true, nil, testStruct{}, ""},
		{"by name pointer array non null", "pointer_array", &[2]*string{stringPtr("abc"), nil}, false, nil, testStruct{PointerArray: [2]*string{stringPtr("abc"), nil}}, ""},
		{"by name map null", "MAP", new(map[string]interface{}), true, nil, testStruct{}, ""},
		{"by name map non null", "Map", &map[string]interface{}{"abc": 123, "def": nil}, false, nil, testStruct{Map: map[string]interface{}{"abc": 123, "def": nil}}, ""},
		{"by name untyped null", "UNTYPED", new(interface{}), true, nil, testStruct{}, ""},
		{"by name untyped non null", "untyped", stringPtr("abc"), false, datatype.Varchar, testStruct{Untyped: "abc"}, ""},
		{"by name untyped wrong type", "untyped", stringPtr("abc"), false, datatype.Int, testStruct{Untyped: "abc"}, ""},
		{"wrong key", true, nil, false, nil, testStruct{}, "no accessible field with name 'true' found in struct datacodec.testStruct"},
		{"wrong value", 0, stringPtr("abc"), false, nil, testStruct{}, "wrong struct field value, expected int, got: 'abc'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := testStruct{}
			i, err := newStructInjector(reflect.ValueOf(&dest).Elem())
			require.NoError(t, err)
			// setElem expects the field to have been previously cached
			_, err = i.(*structInjector).locateAndStoreField(tt.key)
			if tt.wantErr == "" {
				require.NoError(t, err)
			}
			gotErr := i.setElem(-1, tt.key, tt.value, false, tt.wasNull, nil, datatype.Varchar)
			assert.Equal(t, tt.want, dest)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_mapInjector_setElem(t *testing.T) {
	tests := []struct {
		name          string
		key           interface{}
		value         interface{}
		keyWasNull    bool
		valueWasNull  bool
		keyDataType   datatype.DataType
		valueDataType datatype.DataType
		dest          interface{}
		want          interface{}
		wantErr       string
	}{
		{"[string]int value null", stringPtr("abc"), new(int), false, true, nil, nil, map[string]int{}, map[string]int{"abc": 0}, ""},
		{"[string]int value non null", stringPtr("abc"), intPtr(1), false, false, nil, nil, map[string]int{}, map[string]int{"abc": 1}, ""},
		{"[string]*int value null", stringPtr("abc"), new(int), false, true, nil, nil, map[string]*int{}, map[string]*int{"abc": nil}, ""},
		{"[string]*int value non null", stringPtr("abc"), intPtr(1), false, false, nil, nil, map[string]*int{}, map[string]*int{"abc": intPtr(1)}, ""},
		{"[int]interface{} value null", intPtr(123), new(int), false, true, nil, datatype.Varchar, map[int]interface{}{}, map[int]interface{}{123: nil}, ""},
		{"[int]interface{} value non null", intPtr(123), intPtr(456), false, false, nil, datatype.Int, map[int]interface{}{}, map[int]interface{}{123: 456}, ""},
		{"[string]int key null", new(string), intPtr(456), true, false, nil, nil, map[string]int{}, map[string]int{"": 456}, ""},
		{"[*string]int key null", new(string), intPtr(456), true, false, nil, nil, map[*string]int{}, map[*string]int{nil: 456}, ""},
		{"[interface{}]int key null", new(int), intPtr(456), true, false, nil, datatype.Int, map[interface{}]int{}, map[interface{}]int{nil: 456}, ""},
		{"[string]int key and value null", new(string), new(int), true, true, nil, nil, map[string]int{}, map[string]int{"": 0}, ""},
		{"[*string]int key and value null", new(string), new(int), true, true, nil, nil, map[*string]int{}, map[*string]int{nil: 0}, ""},
		{"[interface{}]int key and value null", new(int), new(int), true, true, nil, nil, map[interface{}]int{}, map[interface{}]int{nil: 0}, ""},
		{"wrong key", intPtr(123), stringPtr("abc"), false, false, nil, nil, map[string]string{}, map[string]string{}, "wrong map key, expected string, got: '123'"},
		{"wrong value", stringPtr("abc"), intPtr(456), false, false, nil, nil, map[string]string{}, map[string]string{}, "wrong map value, expected string, got: '456'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := pointerTo(reflect.ValueOf(tt.dest)).Elem()
			i, err := newMapInjector(dest)
			require.NoError(t, err)
			gotErr := i.setElem(-1, tt.key, tt.value, tt.keyWasNull, tt.valueWasNull, tt.keyDataType, tt.valueDataType)
			assert.Equal(t, tt.want, tt.dest)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_injectors_zeroDecodeAndSet(t *testing.T) {
	udt, _ := datatype.NewUserDefinedType("ks1", "table1", []string{"f1"}, []datatype.DataType{datatype.Int})
	dataTypes := map[datatype.DataType]interface{}{
		datatype.Ascii:   "ascii",
		datatype.Bigint:  int64(123),
		datatype.Blob:    []byte{1, 2, 3},
		datatype.Boolean: true,
		datatype.Counter: int64(456),
		datatype.Date:    datePos,
		datatype.Decimal: CqlDecimal{
			Unscaled: big.NewInt(123456),
			Scale:    -1,
		},
		datatype.Double: 123.456,
		// FIXME
		// datatype.Duration:                  time.Second,
		datatype.Float:                     float32(123.456),
		datatype.Inet:                      net.ParseIP("fe80::aede:48ff:fe00:1122"),
		datatype.Int:                       42,
		datatype.Smallint:                  int16(42),
		datatype.Time:                      durationSimple,
		datatype.Timestamp:                 timestampPosUTC,
		datatype.Timeuuid:                  primitive.UUID{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A},
		datatype.Tinyint:                   int8(42),
		datatype.Uuid:                      primitive.UUID{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A},
		datatype.Varchar:                   "UTF-8",
		datatype.Varint:                    big.NewInt(123456),
		datatype.NewListType(datatype.Int): []int{1, 2, 3},
		datatype.NewSetType(datatype.Int):  []int{1, 2, 3},
		datatype.NewMapType(datatype.Int, datatype.Int):       map[int]int{123: 456},
		datatype.NewTupleType(datatype.Int, datatype.Varchar): []interface{}{123, "abc"},
		udt: map[string]interface{}{"f1": 123},
	}
	for dataType, source := range dataTypes {
		t.Run(dataType.String()+" on interface{} receiver", func(t *testing.T) {
			t.Run("slice element", func(t *testing.T) {
				dest := make([]interface{}, 1)
				inj, err := newSliceInjector(reflect.ValueOf(&dest).Elem())
				require.NoError(t, err)
				testInjectorAndDataType(t, source, inj, dataType)
				assert.Equal(t, source, dest[0])
			})
			t.Run("map value", func(t *testing.T) {
				dest := make(map[int]interface{}, 1)
				inj, err := newMapInjector(reflect.ValueOf(&dest).Elem())
				require.NoError(t, err)
				testInjectorAndDataType(t, source, inj, dataType)
				assert.Equal(t, source, dest[0])
			})
			t.Run("struct field", func(t *testing.T) {
				dest := struct{ Field interface{} }{}
				inj, err := newStructInjector(reflect.ValueOf(&dest).Elem())
				require.NoError(t, err)
				testInjectorAndDataType(t, source, inj, dataType)
				assert.Equal(t, source, dest.Field)
			})
		})
	}
}

func testInjectorAndDataType(t *testing.T, source interface{}, inj injector, dataType datatype.DataType) {
	zero, err := inj.zeroElem(0, 0, dataType)
	require.NoError(t, err)
	elementCodec, err := NewCodec(dataType)
	require.NoError(t, err)
	bytes, err := elementCodec.Encode(source, primitive.ProtocolVersion5)
	require.NoError(t, err)
	wasNull, err := elementCodec.Decode(bytes, zero, primitive.ProtocolVersion5)
	require.NoError(t, err)
	assert.False(t, wasNull)
	err = inj.setElem(0, 0, zero, false, false, nil, dataType)
	require.NoError(t, err)
}
