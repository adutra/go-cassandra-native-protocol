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
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"reflect"
)

func NewUserDefined(dataType datatype.UserDefinedType) (Codec, error) {
	if dataType == nil {
		return nil, errNilDataType
	}
	fieldCodecs := make([]Codec, len(dataType.GetFieldTypes()))
	for i, fieldType := range dataType.GetFieldTypes() {
		if fieldCodec, err := NewCodec(fieldType); err != nil {
			return nil, fmt.Errorf("cannot create codec for user-defined type field %d (%s): %w", i, dataType.GetFieldNames()[i], err)
		} else {
			fieldCodecs[i] = fieldCodec
		}
	}
	return &udtCodec{dataType, fieldCodecs}, nil
}

type udtCodec struct {
	dataType    datatype.UserDefinedType
	fieldCodecs []Codec
}

func (c *udtCodec) DataType() datatype.DataType {
	return c.dataType
}

func (c *udtCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var ext extractor
		if ext, err = c.createExtractor(source); err == nil && ext != nil {
			dest, err = writeUdt(ext, c.dataType.GetFieldNames(), c.fieldCodecs, version)
		}
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *udtCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	wasNull = len(source) == 0
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var inj injector
		if inj, err = c.createInjector(dest, wasNull); err == nil && inj != nil {
			err = readUdt(source, inj, c.dataType.GetFieldNames(), c.fieldCodecs, version)
		}
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func (c *udtCodec) createExtractor(source interface{}) (ext extractor, err error) {
	sourceValue, sourceType, wasNil := reflectSource(source)
	if sourceType != nil {
		switch sourceType.Kind() {
		case reflect.Struct:
			if !wasNil {
				ext, err = newStructExtractor(sourceValue)
			}
		case reflect.Map:
			if !wasNil {
				keyType := sourceValue.Type().Key()
				if keyType.Kind() != reflect.String {
					err = errWrongElementType("map key", typeOfString, keyType)
				} else {
					ext, err = newMapExtractor(sourceValue)
				}
			}
		case reflect.Slice, reflect.Array:
			if !wasNil {
				ext, err = newSliceExtractor(sourceValue)
			}
		default:
			err = errSourceTypeNotSupported
		}
	}
	return
}

func (c *udtCodec) createInjector(dest interface{}, wasNull bool) (inj injector, err error) {
	destValue, err := reflectDest(dest, wasNull)
	if err == nil {
		switch destValue.Kind() {
		case reflect.Struct:
			if !wasNull {
				inj, err = newStructInjector(destValue)
			}
		case reflect.Map:
			if !wasNull {
				keyType := destValue.Type().Key()
				if keyType.Kind() != reflect.String {
					err = errWrongElementType("map key", typeOfString, keyType)
				} else {
					inj, err = newMapInjector(destValue)
				}
			}
		case reflect.Slice, reflect.Array:
			if !wasNull {
				inj, err = newSliceInjector(destValue)
			}
		default:
			err = errDestinationTypeNotSupported
		}
		if inj != nil {
			inj.ensureSize(len(c.fieldCodecs))
		}
	}
	return
}

func writeUdt(ext extractor, fieldNames []string, fieldCodecs []Codec, version primitive.ProtocolVersion) ([]byte, error) {
	buf := &bytes.Buffer{}
	for i, fieldCodec := range fieldCodecs {
		name := fieldNames[i]
		if value, err := ext.getElem(i, name); err != nil {
			return nil, errCannotExtractUdtField(i, name, err)
		} else if encodedField, err := fieldCodec.Encode(value, version); err != nil {
			return nil, errCannotEncodeUdtField(i, name, err)
		} else {
			_ = primitive.WriteBytes(encodedField, buf)
		}
	}
	return buf.Bytes(), nil
}

func readUdt(source []byte, inj injector, fieldNames []string, fieldCodecs []Codec, version primitive.ProtocolVersion) error {
	reader := bytes.NewReader(source)
	total := reader.Len()
	for i, fieldCodec := range fieldCodecs {
		name := fieldNames[i]
		if encodedField, err := primitive.ReadBytes(reader); err != nil {
			return errCannotReadUdtField(i, name, err)
		} else if decodedField, err := inj.zeroElem(i, name, fieldCodec.DataType()); err != nil {
			return errCannotCreateUdtField(i, name, err)
		} else if fieldWasNull, err := fieldCodec.Decode(encodedField, decodedField, version); err != nil {
			return errCannotDecodeUdtField(i, name, err)
		} else if err = inj.setElem(i, name, decodedField, false, fieldWasNull, nil, fieldCodec.DataType()); err != nil {
			return errCannotInjectUdtField(i, name, err)
		}
	}
	if remaining := reader.Len(); remaining != 0 {
		return errBytesRemaining(total, remaining)
	}
	return nil
}
