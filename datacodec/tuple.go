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

func NewTuple(tupleType datatype.TupleType) (Codec, error) {
	if tupleType == nil {
		return nil, errNilDataType
	}
	elementCodecs := make([]Codec, len(tupleType.GetFieldTypes()))
	for i, elementType := range tupleType.GetFieldTypes() {
		if elementCodec, err := NewCodec(elementType); err != nil {
			return nil, fmt.Errorf("cannot create codec for tuple element %d: %w", i, err)
		} else {
			elementCodecs[i] = elementCodec
		}
	}
	return &tupleCodec{tupleType, elementCodecs}, nil
}

type tupleCodec struct {
	dataType      datatype.TupleType
	elementCodecs []Codec
}

func (c *tupleCodec) DataType() datatype.DataType {
	return c.dataType
}

func (c *tupleCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var ext extractor
		if ext, err = c.createExtractor(source); err == nil && ext != nil {
			dest, err = writeTuple(ext, c.elementCodecs, version)
		}
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *tupleCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	wasNull = len(source) == 0
	if !version.SupportsDataType(c.DataType().GetDataTypeCode()) {
		err = errDataTypeNotSupported(c.DataType(), version)
	} else {
		var inj injector
		if inj, err = c.createInjector(dest, wasNull); err == nil && inj != nil {
			err = readTuple(source, inj, c.elementCodecs, version)
		}
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func (c *tupleCodec) createExtractor(source interface{}) (ext extractor, err error) {
	sourceValue, sourceType, wasNil := reflectSource(source)
	if sourceType != nil {
		switch sourceType.Kind() {
		case reflect.Struct:
			if !wasNil {
				ext, err = newStructExtractor(sourceValue)
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

func (c *tupleCodec) createInjector(dest interface{}, wasNull bool) (inj injector, err error) {
	destValue, err := reflectDest(dest, wasNull)
	if err == nil {
		switch destValue.Kind() {
		case reflect.Struct:
			if !wasNull {
				inj, err = newStructInjector(destValue)
			}
		case reflect.Slice:
			if !wasNull {
				inj, err = newSliceInjector(destValue)
			}
		case reflect.Array:
			if !wasNull {
				inj, err = newSliceInjector(destValue)
			}
		default:
			err = errDestinationTypeNotSupported
		}
		if inj != nil {
			inj.ensureSize(len(c.elementCodecs))
		}
	}
	return
}

func writeTuple(ext extractor, elementCodecs []Codec, version primitive.ProtocolVersion) ([]byte, error) {
	buf := &bytes.Buffer{}
	for i, elementCodec := range elementCodecs {
		if value, err := ext.getElem(i, i); err != nil {
			return nil, errCannotExtractElement(i, err)
		} else if encodedElement, err := elementCodec.Encode(value, version); err != nil {
			return nil, errCannotEncodeElement(i, err)
		} else {
			_ = primitive.WriteBytes(encodedElement, buf)
		}
	}
	return buf.Bytes(), nil
}

func readTuple(source []byte, inj injector, elementCodecs []Codec, version primitive.ProtocolVersion) error {
	reader := bytes.NewReader(source)
	total := reader.Len()
	for i, elementCodec := range elementCodecs {
		if encodedElement, err := primitive.ReadBytes(reader); err != nil {
			return errCannotReadElement(i, err)
		} else if decodedElement, err := inj.zeroElem(i, i, elementCodec.DataType()); err != nil {
			return errCannotCreateElement(i, err)
		} else if elementWasNull, err := elementCodec.Decode(encodedElement, decodedElement, version); err != nil {
			return errCannotDecodeElement(i, err)
		} else if err = inj.setElem(i, i, decodedElement, false, elementWasNull, nil, elementCodec.DataType()); err != nil {
			return errCannotInjectElement(i, err)
		}
	}
	if remaining := reader.Len(); remaining != 0 {
		return errBytesRemaining(total, remaining)
	}
	return nil
}
