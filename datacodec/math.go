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
	"math"
)

// Adapted from java.lang.Math#addExact(long, long).
// Returns the sum of its arguments, and a boolean indicating whether the sum overflowed.
func addExact(x, y int64) (int64, bool) {
	r := x + y
	if ((x ^ r) & (y ^ r)) < 0 {
		return 0, true
	}
	return r, false
}

// Adapted from:
// https://stackoverflow.com/questions/50744681/testing-overflow-in-integer-multiplication.
// Returns the product of its arguments, and a boolean indicating whether the product overflowed.
// Another interesting implementation can be found in java.lang.Math#multiplyExact(long, long).
func multiplyExact(x, y int64) (int64, bool) {
	if x == 0 || y == 0 || x == 1 || y == 1 {
		return x * y, false
	} else if x == math.MinInt64 || y == math.MinInt64 {
		return 0, true
	} else {
		r := x * y
		if r/y != x {
			return 0, true
		}
		return r, false
	}
}

// Adapted from java.lang.Math#floorDiv(int, int).
// Returns the largest (closest to positive infinity) long value that is less than or equal to the algebraic quotient.
// There is one special case, if the dividend is the Long.MIN_VALUE and the divisor is -1, then integer overflow occurs
// and the result is equal to the Long.MIN_VALUE.
// Normal integer division operates under the round to zero rounding mode (truncation). This operation instead acts
// under the round toward negative infinity (floor) rounding mode. The floor rounding mode gives different results than
// truncation when the exact result is negative.
func floorDiv(x, y int64) int64 {
	r := x / y
	// if the signs are different and modulo not zero, round down
	if (x^y) < 0 && (r*y != x) {
		r--
	}
	return r
}

// Adapted from java.lang.Math#floorMod(int, int).
// Returns the floor modulus of the long arguments.
// The floor modulus is x - (floorDiv(x, y) * y), has the same sign as the divisor y, and is in the range of
// -abs(y) < r < +abs(y).
func floorMod(x, y int64) int64 {
	return x - floorDiv(x, y)*y
}
