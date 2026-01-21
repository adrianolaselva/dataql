package storage

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// DataType represents the detected type of a column
type DataType string

const (
	TypeVarchar DataType = "VARCHAR"
	TypeBigInt  DataType = "BIGINT"
	TypeDouble  DataType = "DOUBLE"
	TypeBoolean DataType = "BOOLEAN"
)

// ColumnDef defines a column with its name and inferred type
type ColumnDef struct {
	Name string
	Type DataType
}

// Storage is the main interface for data storage operations
type Storage interface {
	BuildStructure(string, []string) error
	InsertRow(string, []string, []any) error
	Query(cmd string) (*sql.Rows, error)
	ShowTables() (*sql.Rows, error)
	Close() error
}

// TypedStorage is an optional interface for storage implementations
// that support typed columns for better query compatibility
type TypedStorage interface {
	Storage
	BuildStructureWithTypes(tableName string, columns []ColumnDef) error
	InsertRowWithCoercion(tableName string, columns []string, values []any, columnDefs []ColumnDef) error
}

// InferType detects the most appropriate data type for a value
func InferType(value any) DataType {
	if value == nil {
		return TypeVarchar
	}

	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return TypeBigInt
	case float32, float64:
		return TypeDouble
	case bool:
		return TypeBoolean
	case string:
		return inferTypeFromString(v)
	default:
		return TypeVarchar
	}
}

// inferTypeFromString tries to detect the type from a string value
func inferTypeFromString(s string) DataType {
	s = strings.TrimSpace(s)

	if s == "" {
		return TypeVarchar
	}

	// Check for boolean
	lower := strings.ToLower(s)
	if lower == "true" || lower == "false" {
		return TypeBoolean
	}

	// Check for integer
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return TypeBigInt
	}

	// Check for float
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return TypeDouble
	}

	return TypeVarchar
}

// InferColumnTypes analyzes sample data to infer the best type for each column
// It uses the most restrictive type that can represent all values:
// BIGINT -> DOUBLE -> VARCHAR (BOOLEAN is special-cased)
func InferColumnTypes(columns []string, sampleRows [][]any) []ColumnDef {
	if len(sampleRows) == 0 {
		// No data to analyze, default to VARCHAR
		result := make([]ColumnDef, len(columns))
		for i, col := range columns {
			result[i] = ColumnDef{Name: col, Type: TypeVarchar}
		}
		return result
	}

	// Initialize with the most restrictive type (BIGINT)
	// We'll relax it as needed
	colTypes := make([]DataType, len(columns))
	for i := range colTypes {
		colTypes[i] = TypeBigInt // Start with most restrictive numeric type
	}

	// Track if we've seen any non-null values
	hasValues := make([]bool, len(columns))

	// Analyze each row
	for _, row := range sampleRows {
		for i, val := range row {
			if i >= len(columns) {
				continue
			}

			inferredType := InferType(val)

			// Skip null/empty values
			if inferredType == TypeVarchar {
				if str, ok := val.(string); ok && strings.TrimSpace(str) == "" {
					continue
				}
			}

			hasValues[i] = true

			// Handle boolean separately - if any value is boolean-like, keep checking
			// but don't override other types with it
			if inferredType == TypeBoolean {
				if colTypes[i] == TypeBigInt {
					// First non-null value is boolean
					colTypes[i] = TypeBoolean
				} else if colTypes[i] != TypeBoolean {
					// Mixed types, fall back to VARCHAR
					colTypes[i] = TypeVarchar
				}
				continue
			}

			// Type precedence: BIGINT -> DOUBLE -> VARCHAR
			switch colTypes[i] {
			case TypeBigInt:
				if inferredType == TypeDouble {
					colTypes[i] = TypeDouble
				} else if inferredType == TypeVarchar {
					colTypes[i] = TypeVarchar
				}
			case TypeDouble:
				if inferredType == TypeVarchar {
					colTypes[i] = TypeVarchar
				}
			case TypeBoolean:
				if inferredType != TypeBoolean {
					colTypes[i] = TypeVarchar
				}
			}
			// VARCHAR stays VARCHAR
		}
	}

	// Build result
	result := make([]ColumnDef, len(columns))
	for i, col := range columns {
		t := colTypes[i]
		if !hasValues[i] {
			t = TypeVarchar // No data seen, default to VARCHAR
		}
		result[i] = ColumnDef{Name: col, Type: t}
	}

	return result
}

// TryConvertValue attempts to convert a value to the expected type
// Returns (convertedValue, success). If conversion fails, returns (nil, false)
func TryConvertValue(value any, expectedType DataType) (any, bool) {
	if value == nil {
		return nil, true
	}

	// Handle empty strings as NULL for numeric/boolean types
	if str, ok := value.(string); ok && strings.TrimSpace(str) == "" {
		if expectedType == TypeBigInt || expectedType == TypeDouble || expectedType == TypeBoolean {
			return nil, true
		}
		return str, true
	}

	switch expectedType {
	case TypeBigInt:
		return tryConvertToBigInt(value)
	case TypeDouble:
		return tryConvertToDouble(value)
	case TypeBoolean:
		return tryConvertToBoolean(value)
	default:
		// VARCHAR - convert anything to string
		return fmt.Sprintf("%v", value), true
	}
}

// tryConvertToBigInt attempts to convert a value to int64
func tryConvertToBigInt(value any) (any, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v <= ^uint64(0)>>1 { // Check if fits in int64
			return int64(v), true
		}
		return nil, false
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	case string:
		s := strings.TrimSpace(v)
		// Try parsing as integer first
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i, true
		}
		// Try parsing as float and truncate
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f), true
		}
		return nil, false
	case bool:
		if v {
			return int64(1), true
		}
		return int64(0), true
	}
	return nil, false
}

// tryConvertToDouble attempts to convert a value to float64
func tryConvertToDouble(value any) (any, bool) {
	switch v := value.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case string:
		s := strings.TrimSpace(v)
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f, true
		}
		return nil, false
	case bool:
		if v {
			return float64(1), true
		}
		return float64(0), true
	}
	return nil, false
}

// tryConvertToBoolean attempts to convert a value to bool
func tryConvertToBoolean(value any) (any, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		s := strings.TrimSpace(strings.ToLower(v))
		switch s {
		case "true", "1", "yes", "on", "t", "y":
			return true, true
		case "false", "0", "no", "off", "f", "n":
			return false, true
		}
		return nil, false
	case int, int8, int16, int32, int64:
		// Use type assertion to get int64 value
		var i int64
		switch val := v.(type) {
		case int:
			i = int64(val)
		case int8:
			i = int64(val)
		case int16:
			i = int64(val)
		case int32:
			i = int64(val)
		case int64:
			i = val
		}
		return i != 0, true
	case uint, uint8, uint16, uint32, uint64:
		var u uint64
		switch val := v.(type) {
		case uint:
			u = uint64(val)
		case uint8:
			u = uint64(val)
		case uint16:
			u = uint64(val)
		case uint32:
			u = uint64(val)
		case uint64:
			u = val
		}
		return u != 0, true
	case float32:
		return v != 0, true
	case float64:
		return v != 0, true
	}
	return nil, false
}
