package queryerror

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrorHint represents an enhanced error with user-friendly hints
type ErrorHint struct {
	Original string
	Message  string
	Hint     string
	Example  string
}

// Error implements the error interface
func (e *ErrorHint) Error() string {
	var sb strings.Builder
	sb.WriteString(e.Message)
	if e.Hint != "" {
		sb.WriteString("\n\nHint: ")
		sb.WriteString(e.Hint)
	}
	if e.Example != "" {
		sb.WriteString("\n\nExample:\n  ")
		sb.WriteString(e.Example)
	}
	return sb.String()
}

// Unwrap returns the original error message
func (e *ErrorHint) Unwrap() error {
	return errors.New(e.Original)
}

// errorPattern represents a pattern to match and its enhancement
type errorPattern struct {
	pattern *regexp.Regexp
	enhance func(matches []string, original string) *ErrorHint
}

// patterns holds all error enhancement patterns
var patterns = []errorPattern{
	// strftime type mismatch - wrong argument order or VARCHAR column
	{
		pattern: regexp.MustCompile(`(?i)Could not choose a best candidate function for the function call "strftime\(VARCHAR, STRING_LITERAL\)"`),
		enhance: func(matches []string, original string) *ErrorHint {
			return &ErrorHint{
				Original: original,
				Message:  "strftime function received arguments in wrong order or with incompatible types",
				Hint:     "The strftime function expects: strftime(format_string, date_value)\nIf your date column is stored as VARCHAR, you need to cast it to DATE first.",
				Example:  `strftime('%Y-%m', CAST(date_column AS DATE))`,
			}
		},
	},
	// strftime with wrong argument order (any type mismatch)
	{
		pattern: regexp.MustCompile(`(?i)Could not choose a best candidate function for the function call "strftime\([^)]+\)"`),
		enhance: func(matches []string, original string) *ErrorHint {
			return &ErrorHint{
				Original: original,
				Message:  "strftime function type mismatch",
				Hint:     "The strftime function expects: strftime(format_string, date_value)\nFormat string must be first, followed by a DATE, TIMESTAMP, or TIMESTAMP_NS value.",
				Example:  `strftime('%Y-%m-%d', date_column)` + "\n  " + `strftime('%Y-%m', CAST(varchar_date AS DATE))`,
			}
		},
	},
	// Column not found
	{
		pattern: regexp.MustCompile(`(?i)column\s+"?([^"]+)"?\s+not found|Binder Error:.*Referenced column "([^"]+)" not found`),
		enhance: func(matches []string, original string) *ErrorHint {
			colName := matches[1]
			if colName == "" && len(matches) > 2 {
				colName = matches[2]
			}
			return &ErrorHint{
				Original: original,
				Message:  fmt.Sprintf("Column '%s' not found in table", colName),
				Hint:     "Check the column name for typos. Column names are case-sensitive.\nUse .tables or \\d to list available tables, and \\dt <table> to see column names.",
				Example:  `\\dt my_table`,
			}
		},
	},
	// Table not found
	{
		pattern: regexp.MustCompile(`(?i)Table with name\s+"?([^"]+)"?\s+does not exist|Catalog Error:.*Table.*"([^"]+)".*does not exist`),
		enhance: func(matches []string, original string) *ErrorHint {
			tableName := matches[1]
			if tableName == "" && len(matches) > 2 {
				tableName = matches[2]
			}
			return &ErrorHint{
				Original: original,
				Message:  fmt.Sprintf("Table '%s' does not exist", tableName),
				Hint:     "Check the table name for typos. Table names are derived from file names.\nUse .tables or \\d to list available tables.",
				Example:  `.tables`,
			}
		},
	},
	// Syntax error
	{
		pattern: regexp.MustCompile(`(?i)syntax error at or near "([^"]+)"|Parser Error:.*syntax error at or near "([^"]+)"`),
		enhance: func(matches []string, original string) *ErrorHint {
			token := matches[1]
			if token == "" && len(matches) > 2 {
				token = matches[2]
			}
			return &ErrorHint{
				Original: original,
				Message:  fmt.Sprintf("SQL syntax error near '%s'", token),
				Hint:     "Check your SQL syntax. Common issues:\n- Missing quotes around string values\n- Missing commas between columns\n- Typos in SQL keywords (SELECT, FROM, WHERE, etc.)",
				Example:  `SELECT * FROM table WHERE column = 'value'`,
			}
		},
	},
	// Type conversion error
	{
		pattern: regexp.MustCompile(`(?i)Conversion Error:.*Could not convert string "([^"]+)" to (\w+)`),
		enhance: func(matches []string, original string) *ErrorHint {
			value := matches[1]
			targetType := matches[2]
			return &ErrorHint{
				Original: original,
				Message:  fmt.Sprintf("Cannot convert '%s' to %s", value, targetType),
				Hint:     "The value cannot be automatically converted to the expected type.\nUse TRY_CAST for safe conversion that returns NULL on failure.",
				Example:  fmt.Sprintf(`TRY_CAST('%s' AS %s)`, value, targetType),
			}
		},
	},
	// Division by zero
	{
		pattern: regexp.MustCompile(`(?i)division by zero|Divide by zero`),
		enhance: func(matches []string, original string) *ErrorHint {
			return &ErrorHint{
				Original: original,
				Message:  "Division by zero error",
				Hint:     "Use NULLIF to handle potential zero divisors, which returns NULL instead of error.",
				Example:  `SELECT a / NULLIF(b, 0) FROM table`,
			}
		},
	},
	// Ambiguous column reference
	{
		pattern: regexp.MustCompile(`(?i)Binder Error:.*column "([^"]+)" is ambiguous`),
		enhance: func(matches []string, original string) *ErrorHint {
			colName := matches[1]
			return &ErrorHint{
				Original: original,
				Message:  fmt.Sprintf("Column '%s' is ambiguous (exists in multiple tables)", colName),
				Hint:     "When joining tables with same column names, qualify the column with the table name.",
				Example:  fmt.Sprintf(`SELECT table1.%s, table2.%s FROM table1 JOIN table2 ON ...`, colName, colName),
			}
		},
	},
	// GROUP BY error
	{
		pattern: regexp.MustCompile(`(?i)Binder Error:.*column "([^"]+)" must appear in the GROUP BY clause`),
		enhance: func(matches []string, original string) *ErrorHint {
			colName := matches[1]
			return &ErrorHint{
				Original: original,
				Message:  fmt.Sprintf("Column '%s' must be in GROUP BY or used with an aggregate function", colName),
				Hint:     "When using GROUP BY, all selected columns must either:\n1. Be in the GROUP BY clause, or\n2. Be used with an aggregate function (SUM, COUNT, AVG, etc.)",
				Example:  fmt.Sprintf(`SELECT %s, COUNT(*) FROM table GROUP BY %s`, colName, colName),
			}
		},
	},
	// Memory allocation error
	{
		pattern: regexp.MustCompile(`(?i)memory allocation failed|Out of memory|OutOfMemoryException`),
		enhance: func(matches []string, original string) *ErrorHint {
			return &ErrorHint{
				Original: original,
				Message:  "Memory allocation failed - data too large",
				Hint:     "The query requires more memory than available. Try:\n1. Add LIMIT to reduce result size\n2. Filter data with WHERE clause\n3. Use --lines flag to limit imported rows",
				Example:  `SELECT * FROM large_table LIMIT 1000`,
			}
		},
	},
	// Date/timestamp format error
	{
		pattern: regexp.MustCompile(`(?i)Conversion Error:.*Could not parse string "([^"]+)" according to format`),
		enhance: func(matches []string, original string) *ErrorHint {
			value := matches[1]
			return &ErrorHint{
				Original: original,
				Message:  fmt.Sprintf("Cannot parse date/time value '%s'", value),
				Hint:     "The string doesn't match the expected date/time format.\nUse strptime with the correct format specifier for your data.",
				Example:  `strptime('2024-01-22', '%Y-%m-%d')` + "\n  " + `strptime('22/01/2024', '%d/%m/%Y')`,
			}
		},
	},
}

// EnhanceError checks if the error matches known patterns and returns an enhanced error
// with user-friendly hints. If no pattern matches, returns the original error unchanged.
func EnhanceError(err error) error {
	if err == nil {
		return nil
	}

	errStr := err.Error()

	for _, p := range patterns {
		if matches := p.pattern.FindStringSubmatch(errStr); matches != nil {
			return p.enhance(matches, errStr)
		}
	}

	return err
}

// IsEnhancedError checks if the error is an enhanced ErrorHint
func IsEnhancedError(err error) bool {
	var hint *ErrorHint
	return errors.As(err, &hint)
}
