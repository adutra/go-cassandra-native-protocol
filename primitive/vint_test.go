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
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"math"
	"testing"
)

var (
	vintMaxInt32Bytes = []byte{0xf0, 0xff, 0xff, 0xff, 0xfe}
	vintMinInt32Bytes = []byte{0xf0, 0xff, 0xff, 0xff, 0xff}
	vintMaxInt64Bytes = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}
	vintMinInt64Bytes = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
)

func TestReadUnsignedVint(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		wantVint uint64
		wantRead int
		wantErr  string
	}{
		{"1", []byte{1}, 1, 1, ""},
		{"2", []byte{2}, 2, 1, ""},
		{"3", []byte{3}, 3, 1, ""},
		{"max int32", vintMaxInt32Bytes, encodeZigZag(math.MaxInt32), 5, ""},
		{"min int32", vintMinInt32Bytes, encodeZigZag(math.MinInt32), 5, ""},
		{"max int64", vintMaxInt64Bytes, encodeZigZag(math.MaxInt64), 9, ""},
		{"min int64", vintMinInt64Bytes, encodeZigZag(math.MinInt64), 9, ""},
		{"empty", []byte{}, 0, 0, "cannot read [unsigned vint]: EOF"},
		{"wrong length", []byte{255}, 0, 1, "cannot read [unsigned vint]: EOF"},
		{"wrong length 2", []byte{255, 0, 0, 0, 0, 0, 0, 0}, 0, 8, "cannot read [unsigned vint]: EOF"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := bytes.NewReader(tt.source)
			gotVint, gotRead, gotErr := ReadUnsignedVint(source)
			if tt.wantErr == "" {
				assert.NoError(t, gotErr)
			} else {
				assert.EqualError(t, gotErr, tt.wantErr)
			}
			assert.Equal(t, tt.wantVint, gotVint)
			assert.Equal(t, tt.wantRead, gotRead)
		})
	}
}

func TestWriteUnsignedVint(t *testing.T) {
	tests := []struct {
		name        string
		val         uint64
		wantBytes   []byte
		wantWritten int
		wantErr     string
	}{
		{"1", 1, []byte{1}, 1, ""},
		{"2", 2, []byte{2}, 1, ""},
		{"3", 3, []byte{3}, 1, ""},
		{"max int32", encodeZigZag(math.MaxInt32), vintMaxInt32Bytes, 5, ""},
		{"min int32", encodeZigZag(math.MinInt32), vintMinInt32Bytes, 5, ""},
		{"max int64", encodeZigZag(math.MaxInt64), vintMaxInt64Bytes, 9, ""},
		{"min int64", encodeZigZag(math.MinInt64), vintMinInt64Bytes, 9, ""},
		{"write failed", 0, nil, 0, "cannot write [unsigned vint]: write failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest io.Writer
			if tt.wantErr == "" {
				dest = &bytes.Buffer{}
			} else {
				dest = mockWriter{}
			}
			gotWritten, gotErr := WriteUnsignedVint(tt.val, dest)
			if tt.wantErr == "" {
				assert.NoError(t, gotErr)
				assert.Equal(t, tt.wantBytes, dest.(*bytes.Buffer).Bytes())
			} else {
				assert.EqualError(t, gotErr, tt.wantErr)
			}
			assert.Equal(t, tt.wantWritten, gotWritten)
		})
	}
}

func TestLengthOfUnsignedVint(t *testing.T) {
	assert.Equal(t, 1, LengthOfUnsignedVint(1))
	assert.Equal(t, 1, LengthOfUnsignedVint(2))
	assert.Equal(t, 1, LengthOfUnsignedVint(3))
	assert.Equal(t, 5, LengthOfUnsignedVint(encodeZigZag(math.MaxInt32)))
	assert.Equal(t, 5, LengthOfUnsignedVint(encodeZigZag(math.MinInt32)))
	assert.Equal(t, 9, LengthOfUnsignedVint(encodeZigZag(math.MaxInt64)))
	assert.Equal(t, 9, LengthOfUnsignedVint(encodeZigZag(math.MinInt64)))
}

func TestReadVint(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		wantVint int64
		wantRead int
		err      string
	}{
		{"1", []byte{2}, 1, 1, ""},
		{"2", []byte{4}, 2, 1, ""},
		{"3", []byte{6}, 3, 1, ""},
		{"-1", []byte{1}, -1, 1, ""},
		{"-2", []byte{3}, -2, 1, ""},
		{"-3", []byte{5}, -3, 1, ""},
		{"max int32", vintMaxInt32Bytes, math.MaxInt32, 5, ""},
		{"min int32", vintMinInt32Bytes, math.MinInt32, 5, ""},
		{"max int64", vintMaxInt64Bytes, math.MaxInt64, 9, ""},
		{"min int64", vintMinInt64Bytes, math.MinInt64, 9, ""},
		{"empty", []byte{}, 0, 0, "cannot read [vint]: cannot read [unsigned vint]: EOF"},
		{"wrong length", []byte{255}, 0, 1, "cannot read [vint]: cannot read [unsigned vint]: EOF"},
		{"wrong length 2", []byte{255, 0, 0, 0, 0, 0, 0, 0}, 0, 8, "cannot read [vint]: cannot read [unsigned vint]: EOF"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := bytes.NewReader(tt.source)
			gotVint, gotRead, err := ReadVint(source)
			if tt.err == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.err)
			}
			assert.Equal(t, tt.wantVint, gotVint)
			assert.Equal(t, tt.wantRead, gotRead)
		})
	}
}

func TestWriteVint(t *testing.T) {
	tests := []struct {
		name        string
		val         int64
		wantBytes   []byte
		wantWritten int
		wantErr     string
	}{
		{"1", 1, []byte{2}, 1, ""},
		{"2", 2, []byte{4}, 1, ""},
		{"3", 3, []byte{6}, 1, ""},
		{"-1", -1, []byte{1}, 1, ""},
		{"-2", -2, []byte{3}, 1, ""},
		{"-3", -3, []byte{5}, 1, ""},
		{"max int32", math.MaxInt32, vintMaxInt32Bytes, 5, ""},
		{"min int32", math.MinInt32, vintMinInt32Bytes, 5, ""},
		{"max int64", math.MaxInt64, vintMaxInt64Bytes, 9, ""},
		{"min int64", math.MinInt64, vintMinInt64Bytes, 9, ""},
		{"write failed", 0, nil, 0, "cannot write [vint]: cannot write [unsigned vint]: write failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest io.Writer
			if tt.wantErr == "" {
				dest = &bytes.Buffer{}
			} else {
				dest = mockWriter{}
			}
			gotWritten, gotErr := WriteVint(tt.val, dest)
			if tt.wantErr == "" {
				assert.NoError(t, gotErr)
				assert.Equal(t, tt.wantBytes, dest.(*bytes.Buffer).Bytes())
			} else {
				assert.EqualError(t, gotErr, tt.wantErr)
			}
			assert.Equal(t, tt.wantWritten, gotWritten)
		})
	}
}

func TestLengthOfVint(t *testing.T) {
	assert.Equal(t, 1, LengthOfVint(1))
	assert.Equal(t, 1, LengthOfVint(2))
	assert.Equal(t, 1, LengthOfVint(3))
	assert.Equal(t, 1, LengthOfVint(-1))
	assert.Equal(t, 1, LengthOfVint(-2))
	assert.Equal(t, 1, LengthOfVint(-3))
	assert.Equal(t, 5, LengthOfVint(math.MaxInt32))
	assert.Equal(t, 5, LengthOfVint(math.MinInt32))
	assert.Equal(t, 9, LengthOfVint(math.MaxInt64))
	assert.Equal(t, 9, LengthOfVint(math.MinInt64))
}

type mockWriter struct{}

func (m mockWriter) Write(_ []byte) (n int, err error) { return 0, errors.New("write failed") }
