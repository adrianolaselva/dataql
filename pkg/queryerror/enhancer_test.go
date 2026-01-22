package queryerror

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnhanceError_StrftimeWrongOrder(t *testing.T) {
	tests := []struct {
		name         string
		inputError   string
		expectHint   bool
		containsMsg  string
		containsHint string
	}{
		{
			name:         "strftime with VARCHAR and STRING_LITERAL",
			inputError:   `Binder Error: Could not choose a best candidate function for the function call "strftime(VARCHAR, STRING_LITERAL)". In order to select one, please add explicit type casts.`,
			expectHint:   true,
			containsMsg:  "strftime function received arguments in wrong order",
			containsHint: "strftime(format_string, date_value)",
		},
		{
			name:         "strftime generic type mismatch",
			inputError:   `Binder Error: Could not choose a best candidate function for the function call "strftime(INTEGER, VARCHAR)"`,
			expectHint:   true,
			containsMsg:  "strftime function type mismatch",
			containsHint: "Format string must be first",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := EnhanceError(errors.New(tt.inputError))

			if tt.expectHint {
				assert.True(t, IsEnhancedError(err), "expected enhanced error")
				hint, ok := err.(*ErrorHint)
				assert.True(t, ok)
				assert.Contains(t, hint.Message, tt.containsMsg)
				assert.Contains(t, hint.Hint, tt.containsHint)
				assert.NotEmpty(t, hint.Example)
			} else {
				assert.False(t, IsEnhancedError(err))
			}
		})
	}
}

func TestEnhanceError_ColumnNotFound(t *testing.T) {
	tests := []struct {
		name       string
		inputError string
		expectCol  string
	}{
		{
			name:       "column not found simple",
			inputError: `Binder Error: Referenced column "date_col" not found in FROM clause`,
			expectCol:  "date_col",
		},
		{
			name:       "column not found with quotes",
			inputError: `column "my_column" not found`,
			expectCol:  "my_column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := EnhanceError(errors.New(tt.inputError))
			assert.True(t, IsEnhancedError(err))
			hint := err.(*ErrorHint)
			assert.Contains(t, hint.Message, tt.expectCol)
			assert.Contains(t, hint.Hint, "column name")
		})
	}
}

func TestEnhanceError_TableNotFound(t *testing.T) {
	err := EnhanceError(errors.New(`Catalog Error: Table with name "users" does not exist`))

	assert.True(t, IsEnhancedError(err))
	hint := err.(*ErrorHint)
	assert.Contains(t, hint.Message, "users")
	assert.Contains(t, hint.Hint, ".tables")
}

func TestEnhanceError_SyntaxError(t *testing.T) {
	err := EnhanceError(errors.New(`Parser Error: syntax error at or near "FORM"`))

	assert.True(t, IsEnhancedError(err))
	hint := err.(*ErrorHint)
	assert.Contains(t, hint.Message, "FORM")
	assert.Contains(t, hint.Hint, "SQL syntax")
}

func TestEnhanceError_TypeConversion(t *testing.T) {
	err := EnhanceError(errors.New(`Conversion Error: Could not convert string "abc" to INT64`))

	assert.True(t, IsEnhancedError(err))
	hint := err.(*ErrorHint)
	assert.Contains(t, hint.Message, "abc")
	assert.Contains(t, hint.Message, "INT64")
	assert.Contains(t, hint.Hint, "TRY_CAST")
}

func TestEnhanceError_DivisionByZero(t *testing.T) {
	err := EnhanceError(errors.New(`division by zero`))

	assert.True(t, IsEnhancedError(err))
	hint := err.(*ErrorHint)
	assert.Contains(t, hint.Hint, "NULLIF")
}

func TestEnhanceError_AmbiguousColumn(t *testing.T) {
	err := EnhanceError(errors.New(`Binder Error: column "id" is ambiguous`))

	assert.True(t, IsEnhancedError(err))
	hint := err.(*ErrorHint)
	assert.Contains(t, hint.Message, "id")
	assert.Contains(t, hint.Hint, "qualify the column")
}

func TestEnhanceError_GroupByError(t *testing.T) {
	err := EnhanceError(errors.New(`Binder Error: column "name" must appear in the GROUP BY clause or be used in an aggregate function`))

	assert.True(t, IsEnhancedError(err))
	hint := err.(*ErrorHint)
	assert.Contains(t, hint.Message, "name")
	assert.Contains(t, hint.Hint, "GROUP BY")
}

func TestEnhanceError_MemoryError(t *testing.T) {
	tests := []string{
		"memory allocation failed",
		"Out of memory error",
		"OutOfMemoryException",
	}

	for _, errStr := range tests {
		err := EnhanceError(errors.New(errStr))
		assert.True(t, IsEnhancedError(err), "expected hint for: %s", errStr)
		hint := err.(*ErrorHint)
		assert.Contains(t, hint.Hint, "LIMIT")
	}
}

func TestEnhanceError_DateParseError(t *testing.T) {
	err := EnhanceError(errors.New(`Conversion Error: Could not parse string "2024/01/22" according to format specifier`))

	assert.True(t, IsEnhancedError(err))
	hint := err.(*ErrorHint)
	assert.Contains(t, hint.Message, "2024/01/22")
	assert.Contains(t, hint.Hint, "strptime")
}

func TestEnhanceError_NoMatch(t *testing.T) {
	original := errors.New("some random error that doesn't match any pattern")
	err := EnhanceError(original)

	assert.False(t, IsEnhancedError(err))
	assert.Equal(t, original, err)
}

func TestEnhanceError_NilError(t *testing.T) {
	err := EnhanceError(nil)
	assert.Nil(t, err)
}

func TestErrorHint_Error(t *testing.T) {
	hint := &ErrorHint{
		Original: "original error",
		Message:  "User-friendly message",
		Hint:     "This is a hint",
		Example:  "SELECT * FROM table",
	}

	errStr := hint.Error()
	assert.Contains(t, errStr, "User-friendly message")
	assert.Contains(t, errStr, "Hint: This is a hint")
	assert.Contains(t, errStr, "Example:")
	assert.Contains(t, errStr, "SELECT * FROM table")
}

func TestErrorHint_Unwrap(t *testing.T) {
	hint := &ErrorHint{
		Original: "original error message",
		Message:  "enhanced message",
	}

	unwrapped := hint.Unwrap()
	assert.Contains(t, unwrapped.Error(), "original error message")
}
