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
	"errors"
	"math"
	"math/big"
	"strconv"
	"time"
)

// Platform-dependent constants
const (
	MaxUint = ^uint(0)
	MaxInt  = int(MaxUint >> 1)
	MinInt  = -MaxInt - 1
)

func int64ToInt(val int64) (int, error) {
	if val < int64(MinInt) || val > int64(MaxInt) {
		return 0, errValueOutOfRange(val)
	} else {
		return int(val), nil
	}
}

func int64ToInt32(val int64) (int32, error) {
	if val < math.MinInt32 || val > math.MaxInt32 {
		return 0, errValueOutOfRange(val)
	} else {
		return int32(val), nil
	}
}

func int64ToInt16(val int64) (int16, error) {
	if val < math.MinInt16 || val > math.MaxInt16 {
		return 0, errValueOutOfRange(val)
	} else {
		return int16(val), nil
	}
}

func int64ToInt8(val int64) (int8, error) {
	if val < math.MinInt8 || val > math.MaxInt8 {
		return 0, errValueOutOfRange(val)
	} else {
		return int8(val), nil
	}
}

func int64ToUint64(val int64) (uint64, error) {
	if val < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint64(val), nil
	}
}

func int64ToUint(val int64) (uint, error) {
	if val < 0 || uint64(val) > uint64(MaxUint) {
		return 0, errValueOutOfRange(val)
	} else {
		return uint(val), nil
	}
}

func int64ToUint32(val int64) (uint32, error) {
	if val < 0 || val > math.MaxUint32 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint32(val), nil
	}
}

func int64ToUint16(val int64) (uint16, error) {
	if val < 0 || val > math.MaxUint16 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint16(val), nil
	}
}

func int64ToUint8(val int64) (uint8, error) {
	if val < 0 || val > math.MaxUint8 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint8(val), nil
	}
}

func intToInt32(val int) (int32, error) {
	if val < math.MinInt32 || val > math.MaxInt32 {
		return 0, errValueOutOfRange(val)
	} else {
		return int32(val), nil
	}
}

func intToInt16(val int) (int16, error) {
	if val < math.MinInt16 || val > math.MaxInt16 {
		return 0, errValueOutOfRange(val)
	} else {
		return int16(val), nil
	}
}

func intToInt8(val int) (int8, error) {
	if val < math.MinInt8 || val > math.MaxInt8 {
		return 0, errValueOutOfRange(val)
	} else {
		return int8(val), nil
	}
}

func int32ToInt16(val int32) (int16, error) {
	if val < math.MinInt16 || val > math.MaxInt16 {
		return 0, errValueOutOfRange(val)
	} else {
		return int16(val), nil
	}
}

func int32ToInt8(val int32) (int8, error) {
	if val < math.MinInt8 || val > math.MaxInt8 {
		return 0, errValueOutOfRange(val)
	} else {
		return int8(val), nil
	}
}

func int32ToUint64(val int32) (uint64, error) {
	if val < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint64(val), nil
	}
}

func int32ToUint(val int32) (uint, error) {
	if val < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint(val), nil
	}
}

func int32ToUint32(val int32) (uint32, error) {
	if val < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint32(val), nil
	}
}

func int32ToUint16(val int32) (uint16, error) {
	if val < 0 || val > math.MaxUint16 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint16(val), nil
	}
}

func int32ToUint8(val int32) (uint8, error) {
	if val < 0 || val > math.MaxUint8 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint8(val), nil
	}
}

func int16ToInt8(val int16) (int8, error) {
	if val < math.MinInt8 || val > math.MaxInt8 {
		return 0, errValueOutOfRange(val)
	} else {
		return int8(val), nil
	}
}

func int16ToUint64(val int16) (uint64, error) {
	if val < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint64(val), nil
	}
}

func int16ToUint(val int16) (uint, error) {
	if val < 0 || uint64(val) > uint64(MaxUint) {
		return 0, errValueOutOfRange(val)
	} else {
		return uint(val), nil
	}
}

func int16ToUint32(val int16) (uint32, error) {
	if val < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint32(val), nil
	}
}

func int16ToUint16(val int16) (uint16, error) {
	if val < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint16(val), nil
	}
}

func int16ToUint8(val int16) (uint8, error) {
	if val < 0 || val > math.MaxUint8 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint8(val), nil
	}
}
func int8ToUint64(val int8) (uint64, error) {
	if val < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint64(val), nil
	}
}

func int8ToUint(val int8) (uint, error) {
	if val < 0 || uint64(val) > uint64(MaxUint) {
		return 0, errValueOutOfRange(val)
	} else {
		return uint(val), nil
	}
}

func int8ToUint32(val int8) (uint32, error) {
	if val < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint32(val), nil
	}
}

func int8ToUint16(val int8) (uint16, error) {
	if val < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint16(val), nil
	}
}

func int8ToUint8(val int8) (uint8, error) {
	if val < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint8(val), nil
	}
}

func uint64ToInt64(val uint64) (int64, error) {
	if val > math.MaxInt64 {
		return 0, errValueOutOfRange(val)
	} else {
		return int64(val), nil
	}
}

func uint64ToInt32(val uint64) (int32, error) {
	if val > math.MaxInt32 {
		return 0, errValueOutOfRange(val)
	} else {
		return int32(val), nil
	}
}

func uint64ToInt16(val uint64) (int16, error) {
	if val > math.MaxInt16 {
		return 0, errValueOutOfRange(val)
	} else {
		return int16(val), nil
	}
}

func uint64ToInt8(val uint64) (int8, error) {
	if val > math.MaxInt8 {
		return 0, errValueOutOfRange(val)
	} else {
		return int8(val), nil
	}
}

func uintToInt64(val uint) (int64, error) {
	if uint64(val) > math.MaxInt64 {
		return 0, errValueOutOfRange(val)
	} else {
		return int64(val), nil
	}
}

func uintToInt32(val uint) (int32, error) {
	if val > math.MaxInt32 {
		return 0, errValueOutOfRange(val)
	} else {
		return int32(val), nil
	}
}

func uintToInt16(val uint) (int16, error) {
	if val > math.MaxInt16 {
		return 0, errValueOutOfRange(val)
	} else {
		return int16(val), nil
	}
}

func uintToInt8(val uint) (int8, error) {
	if val > math.MaxInt8 {
		return 0, errValueOutOfRange(val)
	} else {
		return int8(val), nil
	}
}

func uint32ToInt32(val uint32) (int32, error) {
	if val > math.MaxInt32 {
		return 0, errValueOutOfRange(val)
	} else {
		return int32(val), nil
	}
}

func uint32ToInt16(val uint32) (int16, error) {
	if val > math.MaxInt16 {
		return 0, errValueOutOfRange(val)
	} else {
		return int16(val), nil
	}
}

func uint32ToInt8(val uint32) (int8, error) {
	if val > math.MaxInt8 {
		return 0, errValueOutOfRange(val)
	} else {
		return int8(val), nil
	}
}

func uint16ToInt16(val uint16) (int16, error) {
	if val > math.MaxInt16 {
		return 0, errValueOutOfRange(val)
	} else {
		return int16(val), nil
	}
}

func uint16ToInt8(val uint16) (int8, error) {
	if val > math.MaxInt8 {
		return 0, errValueOutOfRange(val)
	} else {
		return int8(val), nil
	}
}

func uint8ToInt8(val uint8) (int8, error) {
	if val > math.MaxInt8 {
		return 0, errValueOutOfRange(val)
	} else {
		return int8(val), nil
	}
}

func stringToInt64(val string) (int64, error) {
	if parsed, err := strconv.ParseInt(val, 10, 64); err != nil {
		return 0, errCannotParseString(val, err)
	} else {
		return parsed, nil
	}
}

func stringToInt32(val string) (int32, error) {
	if parsed, err := strconv.ParseInt(val, 10, 32); err != nil {
		return 0, errCannotParseString(val, err)
	} else {
		return int32(parsed), nil
	}
}

func stringToInt16(val string) (int16, error) {
	if parsed, err := strconv.ParseInt(val, 10, 16); err != nil {
		return 0, errCannotParseString(val, err)
	} else {
		return int16(parsed), nil
	}
}

func stringToInt8(val string) (int8, error) {
	if parsed, err := strconv.ParseInt(val, 10, 8); err != nil {
		return 0, errCannotParseString(val, err)
	} else {
		return int8(parsed), nil
	}
}

func stringToBigInt(val string) (*big.Int, error) {
	if i, ok := new(big.Int).SetString(val, 10); !ok {
		return nil, errCannotParseString(val, errors.New("big.Int.SetString(text, 10) failed"))
	} else {
		return i, nil
	}
}

func stringToEpochMillis(val string, layout string, location *time.Location) (int64, error) {
	if parsed, err := time.ParseInLocation(layout, val, location); err != nil {
		return 0, err
	} else {
		return ConvertTimeToEpochMillis(parsed)
	}
}

func stringToNanosOfDay(val string, layout string) (int64, error) {
	if parsed, err := time.Parse(layout, val); err != nil {
		return 0, err
	} else {
		return ConvertTimeToNanosOfDay(parsed), nil
	}
}

func stringToEpochDays(val string, layout string) (int32, error) {
	if parsed, err := time.Parse(layout, val); err != nil {
		return 0, err
	} else {
		return ConvertTimeToEpochDays(parsed)
	}
}

func bigIntToInt64(val *big.Int) (int64, error) {
	if !val.IsInt64() {
		return 0, errValueOutOfRange((*val).String())
	} else {
		return val.Int64(), nil
	}
}

func bigIntToInt(val *big.Int) (int, error) {
	if !val.IsInt64() || val.Int64() < int64(MinInt) || val.Int64() > int64(MaxInt) {
		return 0, errValueOutOfRange(val)
	} else {
		return int(val.Int64()), nil
	}
}

func bigIntToInt32(val *big.Int) (int32, error) {
	if !val.IsInt64() || val.Int64() < math.MinInt32 || val.Int64() > math.MaxInt32 {
		return 0, errValueOutOfRange(val)
	} else {
		return int32(val.Int64()), nil
	}
}

func bigIntToInt16(val *big.Int) (int16, error) {
	if !val.IsInt64() || val.Int64() < math.MinInt16 || val.Int64() > math.MaxInt16 {
		return 0, errValueOutOfRange(val)
	} else {
		return int16(val.Int64()), nil
	}
}

func bigIntToInt8(val *big.Int) (int8, error) {
	if !val.IsInt64() || val.Int64() < math.MinInt8 || val.Int64() > math.MaxInt8 {
		return 0, errValueOutOfRange(val)
	} else {
		return int8(val.Int64()), nil
	}
}

func bigIntToUint64(val *big.Int) (uint64, error) {
	if !val.IsInt64() || val.Int64() < 0 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint64(val.Int64()), nil
	}
}

func bigIntToUint(val *big.Int) (uint, error) {
	if !val.IsInt64() || val.Int64() < 0 || uint64(val.Int64()) > uint64(MaxUint) {
		return 0, errValueOutOfRange(val)
	} else {
		return uint(val.Int64()), nil
	}
}

func bigIntToUint32(val *big.Int) (uint32, error) {
	if !val.IsInt64() || val.Int64() < 0 || val.Int64() > math.MaxUint32 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint32(val.Int64()), nil
	}
}

func bigIntToUint16(val *big.Int) (uint16, error) {
	if !val.IsInt64() || val.Int64() < 0 || val.Int64() > math.MaxUint16 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint16(val.Int64()), nil
	}
}

func bigIntToUint8(val *big.Int) (uint8, error) {
	if !val.IsInt64() || val.Int64() < 0 || val.Int64() > math.MaxUint8 {
		return 0, errValueOutOfRange(val)
	} else {
		return uint8(val.Int64()), nil
	}
}

func float64ToFloat32(val float64) (float32, error) {
	// this is the best we can do: convert from float64 to float32 is inherently lossy
	if float64(float32(val)) != val {
		return 0, errValueOutOfRange(val)
	} else {
		return float32(val), nil
	}
}
