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
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func Test_sliceExtractor_get(t *testing.T) {
	tests := []struct {
		name    string
		source  interface{}
		index   int
		want    interface{}
		wantErr string
	}{
		{"[]interface empty", []interface{}{}, 0, nil, "slice index out of range: 0"},
		{"[]interface single", []interface{}{123}, 0, 123, ""},
		{"[]interface single", []interface{}{123}, 0, 123, ""},
		{"[]interface multi", []interface{}{123, true, "abc"}, 2, "abc", ""},
		{"[3]interface multi", [3]interface{}{123, true, "abc"}, 2, "abc", ""},
		{"[]interface pointers", []interface{}{intPtr(123), boolPtr(true), stringPtr("abc")}, 2, stringPtr("abc"), ""},
		{"[]interface nil elem", []interface{}{123, nil}, 1, nil, ""},
		{"[]interface complex", []interface{}{[]interface{}{"abc"}}, 0, []interface{}{"abc"}, ""},
		{"[]int", []int{1, 2, 3}, 0, 1, ""},
		{"[3]int", [3]int{1, 2, 3}, 0, 1, ""},
		{"[]*int", []*int{intPtr(123)}, 0, intPtr(123), ""},
		{"[3]*int", [3]*int{intPtr(123)}, 0, intPtr(123), ""},
		{"out of range", []interface{}{123}, 1, nil, "slice index out of range: 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, _ := newSliceExtractor(reflect.ValueOf(tt.source))
			got, gotErr := e.getElem(tt.index, "")
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_mapExtractor_get(t *testing.T) {
	tests := []struct {
		name    string
		source  interface{}
		key     interface{}
		want    interface{}
		wantErr string
	}{
		{"[string]interface empty", map[string]interface{}{}, "abc", nil, ""},
		{"[string]interface single", map[string]interface{}{"abc": 123}, "abc", 123, ""},
		{"[string]interface multi", map[string]interface{}{"abc": 123, "def": true, "ghi": "abc"}, "abc", 123, ""},
		{"[string]interface pointers", map[string]interface{}{"abc": intPtr(123), "def": boolPtr(true), "ghi": stringPtr("abc")}, "abc", intPtr(123), ""},
		{"[string]interface nil elem", map[string]interface{}{"abc": 123, "def": nil}, "def", nil, ""},
		{"[string]interface complex", map[string]interface{}{"abc": map[int]int{123: 456}}, "abc", map[int]int{123: 456}, ""},
		{"[string]int", map[string]int{"abc": 1, "def": 2, "ghi": 3}, "ghi", 3, ""},
		{"[string]*int", map[string]*int{"abc": intPtr(123)}, "abc", intPtr(123), ""},
		{"[int]interface empty", map[int]interface{}{}, 123, nil, ""},
		{"[int]interface single", map[int]interface{}{123: 456}, 123, 456, ""},
		{"[int]interface multi", map[int]interface{}{123: 123, 456: true, 789: "abc"}, 789, "abc", ""},
		{"[int]interface pointers", map[int]interface{}{123: intPtr(123), 456: boolPtr(true), 789: stringPtr("abc")}, 789, stringPtr("abc"), ""},
		{"[int]interface nil elem", map[int]interface{}{123: 123, 456: nil}, 456, nil, ""},
		{"[int]interface complex", map[int]interface{}{123: map[int]int{123: 456}}, 123, map[int]int{123: 456}, ""},
		{"[int]int", map[int]int{123: 1, 456: 2, 789: 3}, 789, 3, ""},
		{"[int]*int", map[int]*int{123: intPtr(123)}, 123, intPtr(123), ""},
		{"wrong key type", map[string]int{"abc": 1}, 123, nil, "wrong map key, expected string, got: '123'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, _ := newMapExtractor(reflect.ValueOf(tt.source))
			got, gotErr := e.getElem(-1, tt.key)
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_structExtractor_get(t *testing.T) {
	type testStruct struct {
		Int        int
		String     *string `cassandra:"foo"`
		unexported bool
	}
	tests := []struct {
		name    string
		source  testStruct
		key     interface{}
		want    interface{}
		wantErr string
	}{
		{"by index empty", testStruct{Int: 0}, 0, 0, ""},
		{"by index simple", testStruct{String: stringPtr("abc")}, 1, stringPtr("abc"), ""},
		{"by index nil elem", testStruct{}, 1, stringNilPtr(), ""},
		{"by index unexported", testStruct{}, 2, nil, "no accessible field with index 2 found in struct datacodec.testStruct"},
		{"by index out of range", testStruct{}, -1, nil, "no accessible field with index -1 found in struct datacodec.testStruct"},
		{"by name empty", testStruct{Int: 0}, "int", 0, ""},
		{"by name simple", testStruct{String: stringPtr("abc")}, "foo", stringPtr("abc"), ""},
		{"by name nil elem", testStruct{}, "foo", stringNilPtr(), ""},
		{"by name unexported", testStruct{unexported: true}, "unexported", nil, "no accessible field with name 'unexported' found in struct datacodec.testStruct"},
		{"by name non existent", testStruct{}, "nonexistent", nil, "no accessible field with name 'nonexistent' found in struct datacodec.testStruct"},
		{"wrong key type", testStruct{}, true, nil, "no accessible field with name 'true' found in struct datacodec.testStruct"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, _ := newStructExtractor(reflect.ValueOf(tt.source))
			got, gotErr := e.getElem(-1, tt.key)
			assert.Equal(t, tt.want, got)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_newSliceExtractor(t *testing.T) {
	tests := []struct {
		name    string
		source  reflect.Value
		want    *sliceExtractor
		wantErr string
	}{
		{"wrong type", reflect.ValueOf(123), nil, "expected slice or array, got: int"},
		{"nil slice", reflect.ValueOf([]byte(nil)), nil, "slice is nil"},
		{"slice", reflect.ValueOf([]byte{1, 2, 3}), &sliceExtractor{reflect.ValueOf([]byte{1, 2, 3}), "slice"}, ""},
		{"array", reflect.ValueOf([3]byte{1, 2, 3}), &sliceExtractor{reflect.ValueOf([3]byte{1, 2, 3}), "array"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newSliceExtractor(tt.source)
			if got == nil {
				assert.Nil(t, tt.want)
			} else {
				assert.Equal(t, tt.want.source.Interface(), got.(*sliceExtractor).source.Interface())
				assert.Equal(t, tt.want.containerType, got.(*sliceExtractor).containerType)
			}
			assertErrorMessage(t, tt.wantErr, err)
		})
	}
}

func Test_newMapExtractor(t *testing.T) {
	validSource := reflect.ValueOf(map[string]int{"abc": 123})
	tests := []struct {
		name    string
		source  reflect.Value
		want    *mapExtractor
		wantErr string
	}{
		{"wrong type", reflect.ValueOf(123), nil, "expected map, got: int"},
		{"nil map", reflect.ValueOf(map[string]int(nil)), nil, "map is nil"},
		{"ok", validSource, &mapExtractor{validSource, validSource.MapKeys()}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newMapExtractor(tt.source)
			if got == nil {
				assert.Nil(t, tt.want)
			} else {
				assert.Equal(t, tt.want.source.Interface(), got.(*mapExtractor).source.Interface())
				assert.Len(t, tt.want.keys, 1)
				assert.Equal(t, tt.want.keys[0].Interface(), got.(*mapExtractor).keys[0].Interface())
			}
			assertErrorMessage(t, tt.wantErr, err)
		})
	}
}

func Test_newStructExtractor(t *testing.T) {
	type testStruct struct {
		Int int
	}
	tests := []struct {
		name    string
		source  reflect.Value
		want    *structExtractor
		wantErr string
	}{
		{"wrong type", reflect.ValueOf(123), nil, "expected struct, got: int"},
		{"ok", reflect.ValueOf(testStruct{}), &structExtractor{reflect.ValueOf(testStruct{})}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newStructExtractor(tt.source)
			if got == nil {
				assert.Nil(t, tt.want)
			} else {
				assert.Equal(t, tt.want.source.Interface(), got.(*structExtractor).source.Interface())
			}
			assertErrorMessage(t, tt.wantErr, err)
		})
	}
}
