package storage

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ============================================
// Tests for InferType
// ============================================

func TestInferType_Integer(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  DataType
	}{
		{"int", 42, TypeBigInt},
		{"int64", int64(42), TypeBigInt},
		{"int32", int32(42), TypeBigInt},
		{"string integer", "123", TypeBigInt},
		{"string negative", "-456", TypeBigInt},
		{"string zero", "0", TypeBigInt},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InferType(tt.value)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInferType_Float(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  DataType
	}{
		{"float64", 3.14, TypeDouble},
		{"float32", float32(3.14), TypeDouble},
		{"string float", "3.14", TypeDouble},
		{"string negative float", "-2.5", TypeDouble},
		{"string scientific positive", "1.5e10", TypeDouble},
		{"string scientific negative", "1.5e-10", TypeDouble},
		{"string scientific uppercase", "2.5E+5", TypeDouble},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InferType(tt.value)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInferType_Boolean(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  DataType
	}{
		{"bool true", true, TypeBoolean},
		{"bool false", false, TypeBoolean},
		{"string true lowercase", "true", TypeBoolean},
		{"string false lowercase", "false", TypeBoolean},
		{"string TRUE uppercase", "TRUE", TypeBoolean},
		{"string FALSE uppercase", "FALSE", TypeBoolean},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InferType(tt.value)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInferType_Varchar(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  DataType
	}{
		{"string text", "hello", TypeVarchar},
		{"string empty", "", TypeVarchar},
		{"nil", nil, TypeVarchar},
		{"string with spaces", "  ", TypeVarchar},
		{"mixed alphanumeric", "abc123", TypeVarchar},
		{"invalid number", "12.34.56", TypeVarchar},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InferType(tt.value)
			assert.Equal(t, tt.want, got)
		})
	}
}

// ============================================
// Tests for TryConvertValue
// ============================================

func TestTryConvertValue_ToBigInt(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		want    any
		success bool
	}{
		{"int to int64", 42, int64(42), true},
		{"int64 passthrough", int64(100), int64(100), true},
		{"float64 truncate", 3.7, int64(3), true},
		{"string integer", "123", int64(123), true},
		{"string negative", "-456", int64(-456), true},
		{"string float truncate", "3.9", int64(3), true},
		{"string invalid", "abc", nil, false},
		{"string mixed", "12abc", nil, false},
		{"bool true", true, int64(1), true},
		{"bool false", false, int64(0), true},
		{"nil value", nil, nil, true},
		{"empty string", "", nil, true},
		{"whitespace string", "   ", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := TryConvertValue(tt.value, TypeBigInt)
			assert.Equal(t, tt.success, ok, "success mismatch")
			assert.Equal(t, tt.want, got, "value mismatch")
		})
	}
}

func TestTryConvertValue_ToDouble(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		want    any
		success bool
	}{
		{"float64 passthrough", 3.14, 3.14, true},
		{"float32 convert", float32(2.5), float64(float32(2.5)), true},
		{"int to float", 42, float64(42), true},
		{"int64 to float", int64(100), float64(100), true},
		{"string float", "3.14", 3.14, true},
		{"string integer", "42", float64(42), true},
		{"string scientific", "1.5e-10", 1.5e-10, true},
		{"string scientific uppercase", "2.5E+5", 2.5e5, true},
		{"string invalid", "abc", nil, false},
		{"bool true", true, float64(1), true},
		{"bool false", false, float64(0), true},
		{"nil value", nil, nil, true},
		{"empty string", "", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := TryConvertValue(tt.value, TypeDouble)
			assert.Equal(t, tt.success, ok, "success mismatch")
			if tt.success && tt.want != nil {
				assert.InDelta(t, tt.want.(float64), got.(float64), 0.0001, "value mismatch")
			} else {
				assert.Equal(t, tt.want, got, "value mismatch")
			}
		})
	}
}

func TestTryConvertValue_ToBoolean(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		want    any
		success bool
	}{
		{"bool true passthrough", true, true, true},
		{"bool false passthrough", false, false, true},
		{"string true", "true", true, true},
		{"string false", "false", false, true},
		{"string TRUE", "TRUE", true, true},
		{"string FALSE", "FALSE", false, true},
		{"string yes", "yes", true, true},
		{"string no", "no", false, true},
		{"string 1", "1", true, true},
		{"string 0", "0", false, true},
		{"string on", "on", true, true},
		{"string off", "off", false, true},
		{"string t", "t", true, true},
		{"string f", "f", false, true},
		{"string y", "y", true, true},
		{"string n", "n", false, true},
		{"string invalid", "maybe", nil, false},
		{"string invalid number", "2", nil, false},
		{"int 1", 1, true, true},
		{"int 0", 0, false, true},
		{"int negative", -1, true, true},
		{"float64 nonzero", 1.5, true, true},
		{"float64 zero", 0.0, false, true},
		{"nil value", nil, nil, true},
		{"empty string", "", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := TryConvertValue(tt.value, TypeBoolean)
			assert.Equal(t, tt.success, ok, "success mismatch")
			assert.Equal(t, tt.want, got, "value mismatch")
		})
	}
}

func TestTryConvertValue_ToVarchar(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		want    any
		success bool
	}{
		{"string passthrough", "hello", "hello", true},
		{"int to string", 42, "42", true},
		{"float to string", 3.14, "3.14", true},
		{"bool true to string", true, "true", true},
		{"bool false to string", false, "false", true},
		{"nil value", nil, nil, true},
		{"empty string", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := TryConvertValue(tt.value, TypeVarchar)
			assert.Equal(t, tt.success, ok, "success mismatch")
			assert.Equal(t, tt.want, got, "value mismatch")
		})
	}
}

// ============================================
// Tests for edge cases
// ============================================

func TestTryConvertValue_LargeNumbers(t *testing.T) {
	// Test near int64 max
	maxInt64 := int64(math.MaxInt64)
	got, ok := TryConvertValue(maxInt64, TypeBigInt)
	assert.True(t, ok)
	assert.Equal(t, maxInt64, got)

	// Test near int64 min
	minInt64 := int64(math.MinInt64)
	got, ok = TryConvertValue(minInt64, TypeBigInt)
	assert.True(t, ok)
	assert.Equal(t, minInt64, got)

	// Test string representation of large number
	got, ok = TryConvertValue("9223372036854775807", TypeBigInt)
	assert.True(t, ok)
	assert.Equal(t, int64(9223372036854775807), got)
}

func TestTryConvertValue_ScientificNotation(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  float64
	}{
		{"positive exponent", "1.5e10", 1.5e10},
		{"negative exponent", "1.5e-10", 1.5e-10},
		{"uppercase E", "2.5E+5", 2.5e5},
		{"no decimal", "1e10", 1e10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := TryConvertValue(tt.value, TypeDouble)
			assert.True(t, ok)
			assert.InDelta(t, tt.want, got.(float64), 0.0001)
		})
	}
}

func TestTryConvertValue_WhitespaceHandling(t *testing.T) {
	// Whitespace should be trimmed for parsing
	got, ok := TryConvertValue("  123  ", TypeBigInt)
	assert.True(t, ok)
	assert.Equal(t, int64(123), got)

	got, ok = TryConvertValue("  3.14  ", TypeDouble)
	assert.True(t, ok)
	assert.InDelta(t, 3.14, got.(float64), 0.0001)

	got, ok = TryConvertValue("  true  ", TypeBoolean)
	assert.True(t, ok)
	assert.Equal(t, true, got)
}

// ============================================
// Tests for InferColumnTypes
// ============================================

func TestInferColumnTypes_MixedNumeric(t *testing.T) {
	columns := []string{"value"}
	sampleRows := [][]any{
		{"100"},
		{"200"},
		{"300.5"}, // Float value should promote column to DOUBLE
	}

	result := InferColumnTypes(columns, sampleRows)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, TypeDouble, result[0].Type)
}

func TestInferColumnTypes_MixedWithStrings(t *testing.T) {
	columns := []string{"value"}
	sampleRows := [][]any{
		{"100"},
		{"200"},
		{"not a number"}, // String should promote column to VARCHAR
	}

	result := InferColumnTypes(columns, sampleRows)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, TypeVarchar, result[0].Type)
}

func TestInferColumnTypes_BooleanMixed(t *testing.T) {
	columns := []string{"active"}
	sampleRows := [][]any{
		{"true"},
		{"false"},
		{"maybe"}, // Non-boolean should promote to VARCHAR
	}

	result := InferColumnTypes(columns, sampleRows)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, TypeVarchar, result[0].Type)
}

func TestInferColumnTypes_AllEmpty(t *testing.T) {
	columns := []string{"col1", "col2"}
	sampleRows := [][]any{
		{"", ""},
		{"", ""},
	}

	result := InferColumnTypes(columns, sampleRows)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, TypeVarchar, result[0].Type)
	assert.Equal(t, TypeVarchar, result[1].Type)
}

func TestInferColumnTypes_MultipleColumns(t *testing.T) {
	columns := []string{"id", "price", "active", "name"}
	sampleRows := [][]any{
		{"1", "99.99", "true", "Product A"},
		{"2", "149.50", "false", "Product B"},
		{"3", "200", "true", "Product C"},
	}

	result := InferColumnTypes(columns, sampleRows)
	assert.Equal(t, 4, len(result))
	assert.Equal(t, TypeBigInt, result[0].Type)  // id - all integers
	assert.Equal(t, TypeDouble, result[1].Type)  // price - mixed int/float
	assert.Equal(t, TypeBoolean, result[2].Type) // active - all boolean
	assert.Equal(t, TypeVarchar, result[3].Type) // name - strings
}
