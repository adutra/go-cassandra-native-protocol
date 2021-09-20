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
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func Test_addExact(t *testing.T) {
	tests := []struct {
		name     string
		x        int64
		y        int64
		expected int64
		overflow bool
	}{
		{"zero", 0, 0, 0, false},
		{"positive", 1, 1, 2, false},
		{"negative", -1, -1, -2, false},

		{"max + zero", math.MaxInt64, 0, math.MaxInt64, false},
		{"zero + max", 0, math.MaxInt64, math.MaxInt64, false},
		{"max-1 + 1", math.MaxInt64 - 1, 1, math.MaxInt64, false},
		{"1 + max-1", 1, math.MaxInt64 - 1, math.MaxInt64, false},

		{"max + 1", math.MaxInt64, 1, 0, true},
		{"1 + max", 1, math.MaxInt64, 0, true},
		{"max-1 + 2", math.MaxInt64 - 1, 2, 0, true},
		{"2 + max-1", 2, math.MaxInt64 - 1, 0, true},

		{"min + zero", math.MinInt64, 0, math.MinInt64, false},
		{"zero + min", 0, math.MinInt64, math.MinInt64, false},
		{"min+1 + -1", math.MinInt64 + 1, -1, math.MinInt64, false},
		{"-1 + min+1", -1, math.MinInt64 + 1, math.MinInt64, false},

		{"min - 1", math.MinInt64, -1, 0, true},
		{"-1 + min", -1, math.MinInt64, 0, true},
		{"min+1 + -2", math.MinInt64 + 1, -2, 0, true},
		{"-2 + min+1", -2, math.MinInt64 + 1, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, overflow := addExact(tt.x, tt.y)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.overflow, overflow)
		})
	}
}

func Test_multiplyExact(t *testing.T) {
	tests := []struct {
		name     string
		x        int64
		y        int64
		expected int64
		overflow bool
	}{
		{"0 * 0", 0, 0, 0, false},
		{"1 * 0", 1, 0, 0, false},
		{"0 * 1", 0, 1, 0, false},
		{"1 * 1", 1, 1, 1, false},
		{"-1 * -1", -1, -1, 1, false},
		{"-1 * 1", -1, 1, -1, false},
		{"1 * -1", 1, -1, -1, false},

		{"max/2 * 2", math.MaxInt64 / 2, 2, math.MaxInt64 - 1, false},
		{"2 * max/2", 2, math.MaxInt64 / 2, math.MaxInt64 - 1, false},
		{"max/2+1 * 2", math.MaxInt64/2 + 1, 2, 0, true},
		{"2 * max/2+1", 2, math.MaxInt64/2 + 1, 0, true},

		{"max * 0", math.MaxInt64, 0, 0, false},
		{"0 * max", 0, math.MaxInt64, 0, false},
		{"max * 1", math.MaxInt64, 1, math.MaxInt64, false},
		{"1 * max", 1, math.MaxInt64, math.MaxInt64, false},
		{"max * -1", math.MaxInt64, -1, -math.MaxInt64, false},
		{"-1 * max", -1, math.MaxInt64, -math.MaxInt64, false},
		{"max * 2", math.MaxInt64, 2, 0, true},
		{"2 * max", 2, math.MaxInt64, 0, true},
		{"max * -2", math.MaxInt64, -2, 0, true},
		{"-2 * max", -2, math.MaxInt64, 0, true},

		{"min/2 * 2", math.MinInt64 / 2, 2, math.MinInt64, false},
		{"2 * min/2", 2, math.MinInt64 / 2, math.MinInt64, false},
		{"min/2-1 * 2", math.MinInt64/2 - 1, 2, 0, true},
		{"2 * min/2-1", 2, math.MinInt64/2 - 1, 0, true},

		{"min * 0", math.MinInt64, 0, 0, false},
		{"0 * min", 0, math.MinInt64, 0, false},
		{"min * 1", math.MinInt64, 1, math.MinInt64, false},
		{"1 * min", 1, math.MinInt64, math.MinInt64, false},
		{"min * -1", math.MinInt64, -1, 0, true},
		{"-1 * min", -1, math.MinInt64, 0, true},
		{"min * 2", math.MinInt64, 2, 0, true},
		{"2 * min", 2, math.MinInt64, 0, true},
		{"min * -2", math.MinInt64, -2, 0, true},
		{"-2 * min", -2, math.MinInt64, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, overflow := multiplyExact(tt.x, tt.y)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.overflow, overflow)
		})
	}
}

func Test_floorDiv(t *testing.T) {
	tests := []struct {
		name     string
		x        int64
		y        int64
		expected int64
	}{
		{"0/1", 0, 1, 0},
		{"0/-1", 0, -1, 0},
		{"1/1", 1, 1, 1},
		{"1/-1", 1, -1, -1},
		{"2/2", 2, 2, 1},
		{"2/-2", 2, -2, -1},
		{"3/2", 3, 2, 1},
		{"3/-2", 3, -2, -2},
		{"-1/1", -1, 1, -1},
		{"-1/-1", -1, -1, 1},
		{"-2/2", -2, 2, -1},
		{"-2/-2", -2, -2, 1},
		{"-3/2", -3, 2, -2},
		{"-3/-2", -3, -2, 1},

		{"5/2", 5, 2, 2},
		{"5/-2", 5, -2, -3},
		{"-5/2", -5, 2, -3},
		{"-5/-2", -5, -2, 2},

		{"1/min", 1, math.MinInt64, -1},
		{"-1/min", -1, math.MinInt64, 0},
		{"1/max", 1, math.MaxInt64, 0},
		{"-1/max", -1, math.MaxInt64, -1},

		{"min/1", math.MinInt64, 1, math.MinInt64},
		{"min/-1", math.MinInt64, -1, math.MinInt64}, // overflow: math.MaxInt64+1
		{"max/1", math.MaxInt64, 1, math.MaxInt64},
		{"max/-1", math.MaxInt64, -1, -math.MaxInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := floorDiv(tt.x, tt.y)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func Test_floorMod(t *testing.T) {
	tests := []struct {
		name     string
		x        int64
		y        int64
		expected int64
	}{
		{"0/1", 0, 1, 0},
		{"0/-1", 0, -1, 0},
		{"1/1", 1, 1, 0},
		{"1/-1", 1, -1, 0},
		{"2/2", 2, 2, 0},
		{"2/-2", 2, -2, 0},
		{"3/2", 3, 2, 1},
		{"3/-2", 3, -2, -1},
		{"-1/1", -1, 1, 0},
		{"-1/-1", -1, -1, 0},
		{"-2/2", -2, 2, -0},
		{"-2/-2", -2, -2, 0},
		{"-3/2", -3, 2, 1},
		{"-3/-2", -3, -2, -1},

		{"5/2", 5, 2, 1},
		{"5/-2", 5, -2, -1},
		{"-5/2", -5, 2, 1},
		{"-5/-2", -5, -2, -1},

		{"1/min", 1, math.MinInt64, 1 + math.MinInt64}, // overflow
		{"-1/min", -1, math.MinInt64, -1},
		{"1/max", 1, math.MaxInt64, 1},
		{"-1/max", -1, math.MaxInt64, -1 + math.MaxInt64},

		{"min/1", math.MinInt64, 1, 0},
		{"min/-1", math.MinInt64, -1, 0},
		{"max/1", math.MaxInt64, 1, 0},
		{"max/-1", math.MaxInt64, -1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := floorMod(tt.x, tt.y)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
