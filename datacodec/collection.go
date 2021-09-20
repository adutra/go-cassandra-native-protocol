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
	"io"
	"math"
	"reflect"
)

func NewList(dataType datatype.ListType) (Codec, error) {
	if dataType == nil {
		return nil, errNilDataType
	}
	codec, err := NewCodec(dataType.GetElementType())
	if err != nil {
		return nil, fmt.Errorf("cannot create codec for list elements: %w", err)
	}
	return &collectionCodec{dataType, codec}, nil
}

func NewSet(dataType datatype.SetType) (Codec, error) {
	if dataType == nil {
		return nil, errNilDataType
	}
	codec, err := NewCodec(dataType.GetElementType())
	if err != nil {
		return nil, fmt.Errorf("cannot create codec for set elements: %w", err)
	}
	return &collectionCodec{dataType, codec}, nil
}

type collectionCodec struct {
	dataType     datatype.DataType
	elementCodec Codec
}

func (c *collectionCodec) DataType() datatype.DataType {
	return c.dataType
}

func (c *collectionCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	ext, size, err := c.createExtractor(source)
	if err == nil && ext != nil {
		dest, err = writeCollection(ext, c.elementCodec, size, version)
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *collectionCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	wasNull = len(source) == 0
	var inj injector
	if inj, err = c.createInjector(dest, wasNull); err == nil && inj != nil {
		err = readCollection(source, inj, c.elementCodec, version)
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func (c *collectionCodec) createExtractor(source interface{}) (ext extractor, size int, err error) {
	sourceValue, sourceType, wasNil := reflectSource(source)
	if sourceType != nil {
		switch sourceType.Kind() {
		case reflect.Slice, reflect.Array:
			if !wasNil {
				ext, err = newSliceExtractor(sourceValue)
				size = sourceValue.Len()
			}
		default:
			err = errSourceTypeNotSupported
		}
	}
	return
}

func (c *collectionCodec) createInjector(dest interface{}, wasNull bool) (inj injector, err error) {
	destValue, err := reflectDest(dest, wasNull)
	if err == nil {
		switch destValue.Kind() {
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
	}
	return
}

func writeCollection(ext extractor, elementCodec Codec, size int, version primitive.ProtocolVersion) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := writeCollectionSize(size, buf, version); err != nil {
		return nil, err
	}
	for i := 0; i < size; i++ {
		if elem, err := ext.getElem(i, i); err != nil {
			return nil, errCannotExtractElement(i, err)
		} else if elem == nil {
			return nil, errNilElement(i)
		} else if encodedElem, err := elementCodec.Encode(elem, version); err != nil {
			return nil, errCannotEncodeElement(i, err)
		} else if encodedElem == nil {
			return nil, errElementEncodedToNil(i)
		} else {
			_ = primitive.WriteBytes(encodedElem, buf)
		}
	}
	return buf.Bytes(), nil
}

func readCollection(source []byte, inj injector, elementCodec Codec, version primitive.ProtocolVersion) error {
	reader := bytes.NewReader(source)
	total := reader.Len()
	if size, err := readCollectionSize(reader, version); err != nil {
		return err
	} else {
		inj.ensureSize(size)
		for i := 0; i < size; i++ {
			if encodedElem, err := primitive.ReadBytes(reader); err != nil {
				return errCannotReadElement(i, err)
			} else if decodedElem, err := inj.zeroElem(i, i, elementCodec.DataType()); err != nil {
				return errCannotCreateElement(i, err)
			} else if _, err = elementCodec.Decode(encodedElem, decodedElem, version); err != nil {
				return errCannotDecodeElement(i, err)
			} else if err = inj.setElem(i, i, decodedElem, false, false, nil, elementCodec.DataType()); err != nil {
				return errCannotInjectElement(i, err)
			}
		}
		if remaining := reader.Len(); remaining != 0 {
			return errBytesRemaining(total, remaining)
		}
	}
	return nil
}

func writeCollectionSize(size int, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if version.Uses4BytesCollectionLength() {
		if size > math.MaxInt32 {
			err = collectionSizeTooLarge(size, math.MaxInt32)
		} else if size < 0 {
			err = collectionSizeNegative(size)
		} else {
			err = primitive.WriteInt(int32(size), dest)
		}
	} else {
		if size > math.MaxUint16 {
			err = collectionSizeTooLarge(size, math.MaxUint16)
		} else if size < 0 {
			err = collectionSizeNegative(size)
		} else {
			err = primitive.WriteShort(uint16(size), dest)
		}
	}
	if err != nil {
		err = cannotWriteCollectionSize(err)
	}
	return
}

func readCollectionSize(source io.Reader, version primitive.ProtocolVersion) (size int, err error) {
	if version.Uses4BytesCollectionLength() {
		var sizeInt32 int32
		sizeInt32, err = primitive.ReadInt(source)
		size = int(sizeInt32)
	} else {
		var sizeInt16 uint16
		sizeInt16, err = primitive.ReadShort(source)
		size = int(sizeInt16)
	}
	if err != nil {
		err = fmt.Errorf("cannot read collection size: %w", err)
	}
	return
}
