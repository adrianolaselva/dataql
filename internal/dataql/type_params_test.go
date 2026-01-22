package dataql

import (
	"testing"
)

func TestParseQueryParams_Basic(t *testing.T) {
	params := []string{"name=Alice", "age=25"}
	result, err := ParseQueryParams(params)
	if err != nil {
		t.Fatalf("ParseQueryParams failed: %v", err)
	}

	if result["name"] != "Alice" {
		t.Errorf("Expected name=Alice, got name=%s", result["name"])
	}
	if result["age"] != "25" {
		t.Errorf("Expected age=25, got age=%s", result["age"])
	}
}

func TestParseQueryParams_EmptyValue(t *testing.T) {
	params := []string{"empty="}
	result, err := ParseQueryParams(params)
	if err != nil {
		t.Fatalf("ParseQueryParams failed: %v", err)
	}

	if result["empty"] != "" {
		t.Errorf("Expected empty='', got empty=%s", result["empty"])
	}
}

func TestParseQueryParams_ValueWithEquals(t *testing.T) {
	params := []string{"expr=a=b"}
	result, err := ParseQueryParams(params)
	if err != nil {
		t.Fatalf("ParseQueryParams failed: %v", err)
	}

	if result["expr"] != "a=b" {
		t.Errorf("Expected expr='a=b', got expr=%s", result["expr"])
	}
}

func TestParseQueryParams_InvalidFormat(t *testing.T) {
	params := []string{"invalid"}
	_, err := ParseQueryParams(params)
	if err == nil {
		t.Error("Expected error for invalid format")
	}
}

func TestParseQueryParams_EmptyName(t *testing.T) {
	params := []string{"=value"}
	_, err := ParseQueryParams(params)
	if err == nil {
		t.Error("Expected error for empty name")
	}
}

func TestApplyQueryParams_ColonSyntax(t *testing.T) {
	params := map[string]string{"name": "Alice"}
	query := "SELECT * FROM users WHERE name = :name"
	result := ApplyQueryParams(query, params)
	expected := "SELECT * FROM users WHERE name = 'Alice'"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestApplyQueryParams_DollarSyntax(t *testing.T) {
	params := map[string]string{"name": "Bob"}
	query := "SELECT * FROM users WHERE name = $name"
	result := ApplyQueryParams(query, params)
	expected := "SELECT * FROM users WHERE name = 'Bob'"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestApplyQueryParams_MultipleParams(t *testing.T) {
	params := map[string]string{"min": "10", "max": "100"}
	query := "SELECT * FROM data WHERE value > :min AND value < :max"
	result := ApplyQueryParams(query, params)
	expected := "SELECT * FROM data WHERE value > 10 AND value < 100"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestApplyQueryParams_NumericValue(t *testing.T) {
	params := map[string]string{"id": "42", "price": "19.99"}
	query := "SELECT * FROM items WHERE id = :id AND price = :price"
	result := ApplyQueryParams(query, params)
	expected := "SELECT * FROM items WHERE id = 42 AND price = 19.99"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestApplyQueryParams_BooleanValue(t *testing.T) {
	params := map[string]string{"active": "true", "deleted": "false"}
	query := "SELECT * FROM users WHERE active = :active AND deleted = :deleted"
	result := ApplyQueryParams(query, params)
	expected := "SELECT * FROM users WHERE active = true AND deleted = false"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestApplyQueryParams_NullValue(t *testing.T) {
	params := map[string]string{"value": "null"}
	query := "SELECT * FROM data WHERE value = :value"
	result := ApplyQueryParams(query, params)
	expected := "SELECT * FROM data WHERE value = null"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestApplyQueryParams_StringWithQuotes(t *testing.T) {
	params := map[string]string{"name": "O'Brien"}
	query := "SELECT * FROM users WHERE name = :name"
	result := ApplyQueryParams(query, params)
	expected := "SELECT * FROM users WHERE name = 'O''Brien'"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestApplyQueryParams_NoParams(t *testing.T) {
	query := "SELECT * FROM users"
	result := ApplyQueryParams(query, nil)
	if result != query {
		t.Errorf("Expected unchanged query %q, got %q", query, result)
	}
}

func TestApplyQueryParams_EmptyParams(t *testing.T) {
	params := map[string]string{}
	query := "SELECT * FROM users"
	result := ApplyQueryParams(query, params)
	if result != query {
		t.Errorf("Expected unchanged query %q, got %q", query, result)
	}
}

func TestApplyQueryParams_PartialMatch(t *testing.T) {
	// Should not replace :username when only :user is defined
	params := map[string]string{"user": "test"}
	query := "SELECT * FROM users WHERE username = :username AND user = :user"
	result := ApplyQueryParams(query, params)
	expected := "SELECT * FROM users WHERE username = :username AND user = 'test'"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestApplyQueryParams_ScientificNotation(t *testing.T) {
	params := map[string]string{"value": "1.5e-10"}
	query := "SELECT * FROM data WHERE value = :value"
	result := ApplyQueryParams(query, params)
	expected := "SELECT * FROM data WHERE value = 1.5e-10"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestApplyQueryParams_NegativeNumber(t *testing.T) {
	params := map[string]string{"value": "-42"}
	query := "SELECT * FROM data WHERE value = :value"
	result := ApplyQueryParams(query, params)
	expected := "SELECT * FROM data WHERE value = -42"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestIsNumber(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"42", true},
		{"-42", true},
		{"3.14", true},
		{"-3.14", true},
		{"1e10", true},
		{"1.5e-10", true},
		{"1E+10", true},
		{"", false},
		{"abc", false},
		{"12abc", false},
		{"abc12", false},
		{"-", false},
		{".", false},
		{"1.2.3", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := isNumber(tt.input)
			if result != tt.expected {
				t.Errorf("isNumber(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestQuoteValue(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"42", "42"},
		{"3.14", "3.14"},
		{"true", "true"},
		{"false", "false"},
		{"null", "null"},
		{"NULL", "NULL"},
		{"hello", "'hello'"},
		{"O'Brien", "'O''Brien'"},
		{"", "''"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := quoteValue(tt.input)
			if result != tt.expected {
				t.Errorf("quoteValue(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
