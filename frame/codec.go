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

package frame

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type Encoder interface {

	// EncodeFrame encodes the entire frame, compressing the body if needed.
	EncodeFrame(frame *Frame, dest io.Writer) error
}

type RawEncoder interface {

	// EncodeRawFrame encodes the given RawFrame.
	EncodeRawFrame(frame *RawFrame, dest io.Writer) error

	// EncodeHeader encodes the given frame Header. This is a partial operation; after calling this method, one must
	// call EncodeBody to fully encode the entire frame.
	EncodeHeader(header *Header, dest io.Writer) error

	// EncodeBody encodes the given frame Body. The body will be compressed depending on whether the Compressed flag is
	// set in the given Header. This is a partial operation; it is illegal to call this method before calling
	// EncodeHeader.
	EncodeBody(header *Header, body *Body, dest io.Writer) error
}

type Decoder interface {

	// DecodeFrame decodes the entire frame, decompressing the body if needed.
	DecodeFrame(source io.Reader) (*Frame, error)
}

type RawDecoder interface {

	// DecodeRawFrame decodes a RawFrame from the given source.
	DecodeRawFrame(source io.Reader) (*RawFrame, error)

	// DecodeHeader decodes a frame Header from the given source, leaving the body contents unread. This is a partial
	// operation; after calling this method, one must either call DecodeBody, DecodeRawBody or DiscardBody to fully
	// read or discard the body contents.
	DecodeHeader(source io.Reader) (*Header, error)

	// DecodeBody decodes a frame Body from the given source, decompressing it if required. This is a partial
	// operation; It is illegal to call this method before calling DecodeHeader.
	DecodeBody(header *Header, source io.Reader) (*Body, error)

	// DecodeRawBody decodes a frame RawBody from the given source. This is a partial operation; it is illegal to call
	// this method before calling DecodeHeader.
	DecodeRawBody(header *Header, source io.Reader) ([]byte, error)

	// DiscardBody discards the contents of a frame body read from the given source. This is a partial operation; it is
	// illegal to call this method before calling DecodeHeader.
	DiscardBody(header *Header, source io.Reader) error
}

type RawConverter interface {

	// ConvertToRawFrame converts a Frame to a RawFrame, encoding the body and compressing it if necessary. The
	// returned RawFrame will share the same header with the initial Frame.
	ConvertToRawFrame(frame *Frame) (*RawFrame, error)

	// ConvertFromRawFrame converts a RawFrame to a Frame, decoding the body and decompressing it if necessary. The
	// returned Frame will share the same header with the initial RawFrame.
	ConvertFromRawFrame(frame *RawFrame) (*Frame, error)
}

// Codec exposes basic encoding and decoding operations for Frame instances. It should be the preferred interface to
// use in typical client applications such as drivers.
type Codec interface {
	Encoder
	Decoder
}

// RawCodec exposes advanced encoding and decoding operations for both Frame and RawFrame instances. It should be used
// only by applications that need to access the frame header without necessarily accessing the frame body, such as
// proxies or gateways.
type RawCodec interface {
	Codec
	RawEncoder
	RawDecoder
	RawConverter
}

type codec struct {
	messageCodecs map[primitive.OpCode]message.Codec
	compressor    BodyCompressor
}

func NewCodec(messageCodecs ...message.Codec) Codec {
	return NewCodecWithCompression(nil, messageCodecs...)
}

func NewCodecWithCompression(compressor BodyCompressor, messageCodecs ...message.Codec) Codec {
	return NewRawCodecWithCompression(compressor, messageCodecs...)
}

func NewRawCodec(messageCodecs ...message.Codec) RawCodec {
	return NewRawCodecWithCompression(nil, messageCodecs...)
}

func NewRawCodecWithCompression(compressor BodyCompressor, messageCodecs ...message.Codec) RawCodec {
	frameCodec := &codec{
		compressor:    compressor,
		messageCodecs: message.DefaultMessageCodecs(),
	}
	for _, messageCodec := range messageCodecs {
		frameCodec.messageCodecs[messageCodec.GetOpCode()] = messageCodec
	}
	return frameCodec
}

func (c *codec) GetBodyCompressor() BodyCompressor {
	return c.compressor
}

func (c *codec) SetBodyCompressor(compressor BodyCompressor) {
	c.compressor = compressor
}

func (c *codec) findMessageCodec(opCode primitive.OpCode) (message.Codec, error) {
	if encoder, found := c.messageCodecs[opCode]; !found {
		return nil, fmt.Errorf("unsupported opcode %d", opCode)
	} else {
		return encoder, nil
	}
}

type ProtocolVersionErr struct {
	Err     string
	Version primitive.ProtocolVersion
	UseBeta bool
}

func NewProtocolVersionErr(err string, version primitive.ProtocolVersion, useBeta bool) *ProtocolVersionErr {
	return &ProtocolVersionErr{
		Err:     err,
		Version: version,
		UseBeta: useBeta,
	}
}

func (e *ProtocolVersionErr) Error() string {
	return fmt.Sprintf("unsupported protocol version (version=%s, useBeta=%v): %s", e.Version, e.UseBeta, e.Err)
}
