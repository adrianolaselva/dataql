package e2e_test

import (
	"strings"
	"testing"
)

// ============================================
// Enhanced Error Message Tests (Issue #23, #30)
// These tests verify that error messages include helpful hints
// ============================================

// TestEnhancedError_StrftimeWrongOrder tests that strftime errors have helpful hints
func TestEnhancedError_StrftimeWrongOrder(t *testing.T) {
	// This query uses the wrong argument order: strftime(date_col, format)
	// Correct is: strftime(format, date_col)
	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT strftime(event_date, '%Y-%m') FROM dates_data")

	// Expect error
	if err == nil {
		t.Fatal("expected error for strftime with wrong argument order")
	}

	// Check for helpful hint
	combined := stderr
	if !strings.Contains(combined, "strftime") {
		t.Errorf("error should mention strftime, got: %s", combined)
	}
	if !strings.Contains(combined, "Hint:") {
		t.Errorf("error should include Hint:, got: %s", combined)
	}
	if !strings.Contains(combined, "format_string") || !strings.Contains(combined, "date_value") {
		t.Errorf("error should explain correct argument order, got: %s", combined)
	}
}

// TestEnhancedError_StrftimeCorrectSyntax tests that strftime works correctly when used properly
func TestEnhancedError_StrftimeCorrectSyntax(t *testing.T) {
	// Correct usage: strftime(format, CAST(varchar_date AS DATE))
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT strftime('%Y-%m', CAST(event_date AS DATE)) as month FROM dates_data LIMIT 3")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2024-01")
	assertContains(t, stdout, "2024-02")
}

// TestEnhancedError_ColumnNotFoundWithHint tests that column not found errors have helpful hints
func TestEnhancedError_ColumnNotFoundWithHint(t *testing.T) {
	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT nonexistent_column FROM simple")

	if err == nil {
		t.Fatal("expected error for nonexistent column")
	}

	combined := stderr
	if !strings.Contains(combined, "Hint:") {
		t.Errorf("error should include Hint:, got: %s", combined)
	}
	if !strings.Contains(combined, "column") {
		t.Errorf("error should mention column, got: %s", combined)
	}
}

// TestEnhancedError_TableNotFoundWithHint tests that table not found errors have helpful hints
func TestEnhancedError_TableNotFoundWithHint(t *testing.T) {
	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM nonexistent_table")

	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}

	combined := stderr
	if !strings.Contains(combined, "nonexistent_table") || !strings.Contains(combined, "does not exist") {
		t.Errorf("error should mention the missing table, got: %s", combined)
	}
	if !strings.Contains(combined, "Hint:") {
		t.Errorf("error should include Hint:, got: %s", combined)
	}
	if !strings.Contains(combined, ".tables") {
		t.Errorf("error hint should suggest .tables command, got: %s", combined)
	}
}

// TestEnhancedError_GroupByMissingWithHint tests that GROUP BY errors have helpful hints
func TestEnhancedError_GroupByMissingWithHint(t *testing.T) {
	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, COUNT(*) FROM simple")

	if err == nil {
		t.Fatal("expected error for missing GROUP BY")
	}

	combined := stderr
	if !strings.Contains(combined, "GROUP BY") {
		t.Errorf("error should mention GROUP BY, got: %s", combined)
	}
	if !strings.Contains(combined, "Hint:") {
		t.Errorf("error should include Hint:, got: %s", combined)
	}
}

// TestEnhancedError_TypeConversionWithHint tests that type conversion errors have helpful hints
func TestEnhancedError_TypeConversionWithHint(t *testing.T) {
	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT CAST(name AS INTEGER) FROM simple")

	if err == nil {
		t.Fatal("expected error for invalid type conversion")
	}

	combined := stderr
	if !strings.Contains(combined, "Conversion") || !strings.Contains(combined, "convert") {
		t.Errorf("error should mention conversion, got: %s", combined)
	}
}

// TestEnhancedError_DateFunctions_WorkCorrectly tests that date functions work when used correctly
func TestEnhancedError_DateFunctions_WorkCorrectly(t *testing.T) {
	tests := []struct {
		name  string
		query string
		check string
	}{
		{
			name:  "year extraction",
			query: "SELECT YEAR(CAST(event_date AS DATE)) as yr FROM dates_data LIMIT 1",
			check: "2024",
		},
		{
			name:  "month extraction",
			query: "SELECT MONTH(CAST(event_date AS DATE)) as mo FROM dates_data LIMIT 1",
			check: "1",
		},
		{
			name:  "date formatting with strftime",
			query: "SELECT strftime('%Y-%m-%d', CAST(event_date AS DATE)) as formatted FROM dates_data LIMIT 1",
			check: "2024-01-01",
		},
		{
			name:  "substring workaround for varchar dates",
			query: "SELECT substr(event_date, 1, 7) as month FROM dates_data LIMIT 1",
			check: "2024-01",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout, stderr, err := runDataQL(t, "run",
				"-f", fixture("csv/dates_data.csv"),
				"-q", tt.query)

			assertNoError(t, err, stderr)
			assertContains(t, stdout, tt.check)
		})
	}
}
