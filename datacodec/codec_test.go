package datacodec

import (
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestNewCodec(t *testing.T) {
	customType := datatype.NewCustomType("com.example.Type")
	listType := datatype.NewListType(datatype.Int)
	listCodec, _ := NewList(listType)
	setType := datatype.NewSetType(datatype.Int)
	setCodec, _ := NewSet(setType)
	mapType := datatype.NewMapType(datatype.Int, datatype.Varchar)
	mapCodec, _ := NewMap(mapType)
	tupleType := datatype.NewTupleType(datatype.Int)
	tupleCodec, _ := NewTuple(tupleType)
	userDefinedType, _ := datatype.NewUserDefinedType("ks1", "table1", []string{"f1"}, []datatype.DataType{datatype.Int})
	userDefinedCodec, _ := NewUserDefined(userDefinedType)
	tests := []struct {
		name      string
		dt        datatype.DataType
		wantCodec Codec
		wantErr   string
	}{
		{"Ascii", datatype.Ascii, Ascii, ""},
		{"Bigint", datatype.Bigint, Bigint, ""},
		{"Blob", datatype.Blob, Blob, ""},
		{"Boolean", datatype.Boolean, Boolean, ""},
		{"Counter", datatype.Counter, Counter, ""},
		{"Custom", customType, NewCustom(customType), ""},
		{"Date", datatype.Date, Date, ""},
		{"Decimal", datatype.Decimal, Decimal, ""},
		{"Double", datatype.Double, Double, ""},
		{"Duration", datatype.Duration, Duration, ""},
		{"Float", datatype.Float, Float, ""},
		{"Inet", datatype.Inet, Inet, ""},
		{"Int", datatype.Int, Int, ""},
		{"Smallint", datatype.Smallint, Smallint, ""},
		{"Time", datatype.Time, Time, ""},
		{"Timestamp", datatype.Timestamp, TimestampUTC, ""},
		{"Timeuuid", datatype.Timeuuid, Timeuuid, ""},
		{"Tinyint", datatype.Tinyint, Tinyint, ""},
		{"Uuid", datatype.Uuid, Uuid, ""},
		{"Varchar", datatype.Varchar, Varchar, ""},
		{"Varint", datatype.Varint, Varint, ""},
		{"List", listType, listCodec, ""},
		{"Set", setType, setCodec, ""},
		{"Map", mapType, mapCodec, ""},
		{"Tuple", tupleType, tupleCodec, ""},
		{"UserDefined", userDefinedType, userDefinedCodec, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCodec, gotErr := NewCodec(tt.dt)
			assert.Equal(t, tt.wantCodec, gotCodec)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func TestPreferredGoType(t *testing.T) {
	customType := datatype.NewCustomType("com.example.Type")
	listType := datatype.NewListType(datatype.Int)
	setType := datatype.NewSetType(datatype.Int)
	mapType := datatype.NewMapType(datatype.Int, datatype.Varchar)
	tupleType := datatype.NewTupleType(datatype.Int)
	userDefinedType, _ := datatype.NewUserDefinedType("ks1", "table1", []string{"f1"}, []datatype.DataType{datatype.Int})
	tests := []struct {
		name     string
		dt       datatype.DataType
		wantType reflect.Type
		wantErr  string
	}{
		{"Ascii", datatype.Ascii, typeOfString, ""},
		{"Bigint", datatype.Bigint, typeOfInt64, ""},
		{"Blob", datatype.Blob, typeOfByteSlice, ""},
		{"Boolean", datatype.Boolean, typeOfBoolean, ""},
		{"Counter", datatype.Counter, typeOfInt64, ""},
		{"Custom", customType, typeOfByteSlice, ""},
		{"Date", datatype.Date, typeOfTime, ""},
		{"Decimal", datatype.Decimal, typeOfCqlDecimal, ""},
		{"Double", datatype.Double, typeOfFloat64, ""},
		{"Duration", datatype.Duration, typeOfCqlDuration, ""},
		{"Float", datatype.Float, typeOfFloat32, ""},
		{"Inet", datatype.Inet, typeOfNetIP, ""},
		{"Int", datatype.Int, typeOfInt, ""},
		{"Smallint", datatype.Smallint, typeOfInt16, ""},
		{"Time", datatype.Time, typeOfDuration, ""},
		{"Timestamp", datatype.Timestamp, typeOfTime, ""},
		{"Timeuuid", datatype.Timeuuid, typeOfUUID, ""},
		{"Tinyint", datatype.Tinyint, typeOfInt8, ""},
		{"Uuid", datatype.Uuid, typeOfUUID, ""},
		{"Varchar", datatype.Varchar, typeOfString, ""},
		{"Varint", datatype.Varint, typeOfBigIntPointer, ""},
		{"List", listType, reflect.TypeOf([]int{}), ""},
		{"Set", setType, reflect.TypeOf([]int{}), ""},
		{"Map", mapType, reflect.TypeOf(map[int]string{}), ""},
		{"Tuple", tupleType, reflect.TypeOf([]interface{}{}), ""},
		{"UserDefined", userDefinedType, reflect.TypeOf(map[string]interface{}{}), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotErr := PreferredGoType(tt.dt)
			assert.Equal(t, tt.wantType, gotType, "expected %s, got %s", tt.wantType, gotType)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}
