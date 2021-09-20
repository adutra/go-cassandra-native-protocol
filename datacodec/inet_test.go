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
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

var (
	inetZero  = net.IP{}
	inetAddr4 = net.IPv4(192, 168, 1, 1).To4()
	inetAddr6 = net.IP{0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34}
)

var (
	inetAddr4Bytes = []byte{192, 168, 1, 1}
	inetAddr6Bytes = []byte{0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34}
)

func Test_inetCodec_DataType(t *testing.T) {
	assert.Equal(t, datatype.Inet, Inet.DataType())
}

func Test_inetCodec_Encode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   interface{}
				expected []byte
				err      string
			}{
				{"nil", nil, nil, ""},
				{"zero", inetZero, nil, ""},
				{"non nil v4", inetAddr4, inetAddr4Bytes, ""},
				{"non nil v6", inetAddr6, inetAddr6Bytes, ""},
				{"non nil v4 pointer", &inetAddr4, inetAddr4Bytes, ""},
				{"non nil v6 pointer", &inetAddr6, inetAddr6Bytes, ""},
				{"conversion failed", 123, nil, fmt.Sprintf("cannot encode int as CQL inet with %v: cannot assign int to net.IP: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := Inet.Encode(tt.source, version)
					assert.Equal(t, tt.expected, actual)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_inetCodec_Decode(t *testing.T) {
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				dest     interface{}
				expected interface{}
				wasNull  bool
				err      string
			}{
				{"null", nil, new(net.IP), new(net.IP), true, ""},
				{"zero", []byte{}, new(net.IP), new(net.IP), true, ""},
				{"non null v4", inetAddr4Bytes, new(net.IP), &inetAddr4, false, ""},
				{"non null v6", inetAddr6Bytes, new(net.IP), &inetAddr6, false, ""},
				{"invalid", []byte{1, 2, 3}, new(net.IP), new(net.IP), false, fmt.Sprintf("cannot decode CQL inet as *net.IP with %v: cannot read net.IP: expected 4 or 16 bytes but got: 3", version)},
				{"conversion failed", inetAddr4Bytes, new(float64), new(float64), false, fmt.Sprintf("cannot decode CQL inet as *float64 with %v: cannot store net.IP into *float64: conversion not supported", version)},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					wasNull, err := Inet.Decode(tt.source, tt.dest, version)
					assert.Equal(t, tt.expected, tt.dest)
					assert.Equal(t, tt.wasNull, wasNull)
					assertErrorMessage(t, tt.err, err)
				})
			}
		})
	}
}

func Test_convertToIP(t *testing.T) {
	tests := []struct {
		name     string
		source   interface{}
		wantDest net.IP
		wantErr  string
	}{
		{"from net.IP", inetAddr4, inetAddr4, ""},
		{"from *net.IP", &inetAddr4, inetAddr4, ""},
		{"from *net.IP nil", netIPNilPtr(), nil, ""},
		{"from []byte", inetAddr4Bytes, inetAddr4, ""},
		{"from *[]byte", &inetAddr4Bytes, inetAddr4, ""},
		{"from *[]byte nil", byteSliceNilPtr(), nil, ""},
		{"from *[]byte wrong length", []byte{1, 2, 3}, net.IP{1, 2, 3}, ""}, // will be rejected by writeInet
		{"from untyped nil", nil, nil, ""},
		{"from unsupported value type", 123, nil, "cannot assign int to net.IP: conversion not supported"},
		{"from unsupported pointer type", intPtr(123), nil, "cannot assign *int to net.IP: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDest, gotErr := convertToIP(tt.source)
			assert.Equal(t, tt.wantDest, gotDest)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_convertFromIP(t *testing.T) {
	tests := []struct {
		name     string
		val      net.IP
		wasNull  bool
		dest     interface{}
		expected interface{}
		err      string
	}{
		{"to *net.IP nil dest", inetAddr4, false, netIPNilPtr(), netIPNilPtr(), "cannot store net.IP into *net.IP: destination is nil"},
		{"to *net.IP nil source", nil, true, new(net.IP), new(net.IP), ""},
		{"to *net.IP empty source", inetZero, true, new(net.IP), new(net.IP), ""},
		{"to *net.IP wrong length", net.IP{1, 2, 3}, false, new(net.IP), &net.IP{1, 2, 3}, ""}, // will be rejected by readInet
		{"to *net.IP v4 non nil", inetAddr4, false, new(net.IP), &inetAddr4, ""},
		{"to *net.IP v6 non nil", inetAddr6, false, new(net.IP), &inetAddr6, ""},
		{"to *[]byte nil dest", inetAddr4, false, byteSliceNilPtr(), byteSliceNilPtr(), "cannot store net.IP into *[]uint8: destination is nil"},
		{"to *[]byte nil source", nil, true, new([]byte), new([]byte), ""},
		{"to *[]byte empty source", inetZero, true, new([]byte), new([]byte), ""},
		{"to *[]byte v4 non nil", inetAddr4, false, new([]byte), &inetAddr4Bytes, ""},
		{"to *[]byte v6 non nil", inetAddr6, false, new([]byte), &inetAddr6Bytes, ""},
		{"to untyped nil", inetAddr4, false, nil, nil, "cannot store net.IP into <nil>: destination is nil"},
		{"to non pointer", inetAddr4, false, net.IP{}, net.IP{}, "cannot store net.IP into net.IP: destination is not pointer"},
		{"to unsupported pointer type", inetAddr4, false, new(float64), new(float64), "cannot store net.IP into *float64: conversion not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := convertFromIP(tt.val, tt.wasNull, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assertErrorMessage(t, tt.err, gotErr)
		})
	}
}

func Test_writeInet(t *testing.T) {
	tests := []struct {
		name     string
		val      net.IP
		expected []byte
		err      string
	}{
		{"nil", nil, nil, ""},
		{"empty", net.IP{}, nil, ""},
		{"non nil v4", inetAddr4, inetAddr4Bytes, ""},
		{"non nil v6", inetAddr6, inetAddr6Bytes, ""},
		{"invalid", net.IP{1, 2, 3}, nil, "cannot write net.IP: expected 4 or 16 bytes but got: 3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := writeInet(tt.val)
			assert.Equal(t, tt.expected, actual)
			assertErrorMessage(t, tt.err, err)
		})
	}
}

func Test_readInet(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected net.IP
		wasNull  bool
		err      string
	}{
		{"nil", nil, nil, true, ""},
		{"empty", []byte{}, nil, true, ""},
		{"wrong length", []byte{1}, nil, false, "cannot read net.IP: expected 4 or 16 bytes but got: 1"},
		{"non nil v4", inetAddr4Bytes, inetAddr4, false, ""},
		{"non nil v6", inetAddr6Bytes, inetAddr6, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, wasNull, err := readInet(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.wasNull, wasNull)
			assertErrorMessage(t, tt.err, err)
		})
	}
}
