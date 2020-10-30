package datatype

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetType(t *testing.T) {
	setType := NewSetType(Varchar)
	assert.Equal(t, primitive.DataTypeCodeSet, setType.GetDataTypeCode())
	assert.Equal(t, Varchar, setType.GetElementType())
}

func TestSetTypeCodecEncode(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    SetType
				expected []byte
				err      error
			}{
				{
					"simple set",
					NewSetType(Varchar),
					[]byte{0, byte(primitive.DataTypeCodeVarchar & 0xFF)},
					nil,
				},
				{
					"complex set",
					NewSetType(NewSetType(Varchar)),
					[]byte{
						0, byte(primitive.DataTypeCodeSet & 0xFF),
						0, byte(primitive.DataTypeCodeVarchar & 0xFF)},
					nil,
				},
				{"nil set", nil, nil, errors.New("expected SetType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeSet)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var dest = &bytes.Buffer{}
					var err error
					err = codec.encode(test.input, dest, version)
					actual := dest.Bytes()
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestSetTypeCodecEncodedLength(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    SetType
				expected int
				err      error
			}{
				{"simple set", NewSetType(Varchar), primitive.LengthOfShort, nil},
				{"complex set", NewSetType(NewSetType(Varchar)), primitive.LengthOfShort + primitive.LengthOfShort, nil},
				{"nil set", nil, -1, errors.New("expected SetType, got <nil>")},
			}
			codec, _ := findCodec(primitive.DataTypeCodeSet)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var actual int
					var err error
					actual, err = codec.encodedLength(test.input, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}

func TestSetTypeCodecDecode(t *testing.T) {
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []byte
				expected SetType
				err      error
			}{
				{
					"simple set",
					[]byte{0, byte(primitive.DataTypeCodeVarchar & 0xff)},
					NewSetType(Varchar),
					nil,
				},
				{
					"complex set",
					[]byte{
						0, byte(primitive.DataTypeCodeSet & 0xff),
						0, byte(primitive.DataTypeCodeVarchar & 0xff)},
					NewSetType(NewSetType(Varchar)),
					nil,
				},
				{
					"cannot read set",
					[]byte{},
					nil,
					fmt.Errorf("cannot read set element type: %w",
						fmt.Errorf("cannot read data type code: %w",
							fmt.Errorf("cannot read [short]: %w",
								errors.New("EOF")))),
				},
			}
			codec, _ := findCodec(primitive.DataTypeCodeSet)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var source = bytes.NewBuffer(test.input)
					var actual DataType
					var err error
					actual, err = codec.decode(source, version)
					assert.Equal(t, test.expected, actual)
					assert.Equal(t, test.err, err)
				})
			}
		})
	}
}
