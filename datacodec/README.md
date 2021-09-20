# Package `datacodec` overview

Package datacodec contains functionality to encode and decode CQL data.

`datacodec.Codec` is its main interface, and it has two main methods:

```go
// Encode encodes the given source into dest. The parameter source must be a value of a supported Go type for the
// CQL type being encoded, or a pointer thereto; a nil value is encoded as a CQL NULL.
Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error)

// Decode decodes the given source into dest. The parameter dest must be a pointer to a supported Go type for
// the CQL type being decoded; it cannot be nil. If return parameter wasNull is true, then the decoded value was a
// NULL, in which case the actual value stored in dest will be set to its zero value.
Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error)
```

## Obtaining a codec

TODO NewCodec

Simple CQL types have a global codec available. Codecs for complex types can be obtained through constructor functions:

* NewList
* NewSet
* NewMap
* NewTuple
* NewUserDefined

## Using a codec

Both accept a wide variety of inputs.

TODO table of accepted inputs/outputs

### Encoding data

Sources can be passed by value or by reference, unless specified otherwise in the table above.

Accept untyped nil when encoding; encoding such a value should be a no-op.

* convert to pivot value
* encode

### Decoding data

Any nil input will result in an error.

* decode to pivot value
* convert

The method accepts a _pointer to a zero value_  of the desired target
type. The method implementation will then read the value and convert it to that target type, if a conversion path is
available.

A few remarks:

1. The destination value _must be a non-nil pointer_, including for slices and maps. This is the same
   for `json.Unmarshal` and is required for the value to be addressable (i.e. settable), and for the changes applied to
   the value to be visible once the method returns.
2. One of the challenges of this design is how to handle NULLs. The rules should be as follows:
   a. When a null is decoded, the passed `dest` variable should be set to its zero value. b. Depending on the variable
   type, the zero value may or may not be distinguishable from the CQL zero value for that type. For example, if a CQL
   bigint was null and `dest` is `int64`, then `dest` will be set to `0`, effectively making this indistinguishable from
   a bigint value `0`. c. This is why a new return parameter is required: `wasNull`. If `wasNull` is true then we know
   for sure that the decoded value was a CQL NULL.

A typical invocation of `Codec.Decode` is then as follows:

```
source := []byte{...}
var value int64
wasNull, err := datacodec.Bigint.Decode(source, &value, primitive.ProtocolVersion5)
if err != nil {
	fmt.Println("Decoding failed: ", err)
} else if wasNull {
	fmt.Println("CQL value was NULL")
} else {
	fmt.Println("CQL value was:", value)
}
```

Decoding a complex value is not very different:

```
source := []byte{...}
var value []int
listOfInt := datatype.NewListType(datatype.Int)
listOfIntCodec, _ := datacodec.NewList(listOfInt)
wasNull, err := listOfIntCodec.Decode(source, &value, primitive.ProtocolVersion5)
if err != nil {
	fmt.Println("Decoding failed: ", err)
} else if wasNull {
	fmt.Println("CQL value was NULL")
} else {
	fmt.Println("CQL value was:", value)
}
```

## Developer guide

### Avoid reflection

Reflection can and should be avoided for simple CQL types. Using a `switch value.(type)` block one can handle different
source and destination types without resorting to reflection.

Complex types however will need reflection as Go does not have a way to handle slices, structs and maps in a generic way
without reflection.

### Generating mocks

    mockery --dir=./datacodec --name=Codec --output=./datacodec --outpkg=datacodec --filename=mock_codec_test.go --structname=mockCodec
    mockery --dir=./datacodec --name=extractor --output=./datacodec --outpkg=datacodec --filename=mock_extractor_test.go --structname=mockExtractor
    mockery --dir=./datacodec --name=injector --output=./datacodec --outpkg=datacodec --filename=mock_injector_test.go --structname=mockInjector
    mockery --dir=./datacodec --name=keyValueExtractor --output=./datacodec --outpkg=datacodec --filename=mock_key_value_extractor_test.go --structname=mockKeyValueExtractor
    mockery --dir=./datacodec --name=keyValueInjector --output=./datacodec --outpkg=datacodec --filename=mock_key_value_injector_test.go --structname=mockKeyValueInjector
