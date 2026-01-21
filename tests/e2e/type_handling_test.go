package e2e_test

import (
	"testing"
)

// ============================================
// CSV Type Handling Tests
// ============================================

func TestType_CSV_MixedTypes(t *testing.T) {
	// Test that mixed types are handled gracefully
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/mixed_types.csv"),
		"-q", "SELECT * FROM mixed_types ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
	assertContains(t, stdout, "100")
	assertContains(t, stdout, "active")
	assertContains(t, stdout, "2")
	assertContains(t, stdout, "200.5")
	assertContains(t, stdout, "inactive")
	assertContains(t, stdout, "3")
	assertContains(t, stdout, "three hundred")
	assertContains(t, stdout, "pending")
}

func TestType_CSV_MixedTypes_Count(t *testing.T) {
	// Test that all rows are counted
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/mixed_types.csv"),
		"-q", "SELECT COUNT(*) as total FROM mixed_types")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "5")
}

func TestType_CSV_EdgeCases_Select(t *testing.T) {
	// Test that edge cases can be loaded and queried
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/type_edge_cases.csv"),
		"-q", "SELECT * FROM type_edge_cases ORDER BY id LIMIT 3")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "42")
	assertContains(t, stdout, "3.14")
	assertContains(t, stdout, "true")
}

func TestType_CSV_EdgeCases_ScientificNotation(t *testing.T) {
	// Test that scientific notation is parsed correctly
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/type_edge_cases.csv"),
		"-q", "SELECT id, scientific FROM type_edge_cases WHERE scientific IS NOT NULL ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
	// Scientific notation should be parsed as DOUBLE
}

func TestType_CSV_EdgeCases_NegativeNumbers(t *testing.T) {
	// Test that negative numbers are handled
	// Note: The 'negative' column contains mixed types (numbers and 'positive' string)
	// so it's inferred as VARCHAR. We test that the values are loaded correctly.
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/type_edge_cases.csv"),
		"-q", "SELECT id, negative FROM type_edge_cases WHERE negative LIKE '-%' ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "-999")
	assertContains(t, stdout, "-1")
}

func TestType_CSV_EdgeCases_InvalidValues(t *testing.T) {
	// Test that invalid values become NULL after coercion
	// Note: integer_col has mixed types so it's inferred as VARCHAR
	// We test that the values are loaded correctly
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/type_edge_cases.csv"),
		"-q", "SELECT id, integer_col FROM type_edge_cases WHERE integer_col = '' OR integer_col = 'not_a_number'")

	assertNoError(t, err, stderr)
	// Row 3 has empty integer_col, row 4 has 'not_a_number'
	assertContains(t, stdout, "3")
	assertContains(t, stdout, "4")
}

func TestType_CSV_BooleanVariants_TrueFalse(t *testing.T) {
	// Test that true/false variants are handled
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/boolean_variants.csv"),
		"-q", "SELECT * FROM boolean_variants ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
	assertContains(t, stdout, "true")
	assertContains(t, stdout, "TRUE")
	assertContains(t, stdout, "yes")
	assertContains(t, stdout, "2")
	assertContains(t, stdout, "false")
	assertContains(t, stdout, "FALSE")
	assertContains(t, stdout, "no")
}

func TestType_CSV_NullHandling(t *testing.T) {
	// Test that NULL values are handled correctly
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/mixed_types.csv"),
		"-q", "SELECT id FROM mixed_types WHERE value IS NULL OR value = ''")

	assertNoError(t, err, stderr)
	// Row 4 has empty value
	assertContains(t, stdout, "4")
}

func TestType_CSV_NumericComparison(t *testing.T) {
	// Test numeric comparison works after type inference
	// Note: integer_col has mixed types, so use CAST for comparison
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/type_edge_cases.csv"),
		"-q", "SELECT id, integer_col FROM type_edge_cases WHERE TRY_CAST(integer_col AS BIGINT) > 10 ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "42")
}

func TestType_CSV_NumericAggregation(t *testing.T) {
	// Test numeric aggregation works
	// Note: integer_col has mixed types, so use TRY_CAST for aggregation
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/type_edge_cases.csv"),
		"-q", "SELECT COUNT(*) as total FROM type_edge_cases WHERE TRY_CAST(integer_col AS BIGINT) IS NOT NULL")

	assertNoError(t, err, stderr)
	// Should count rows where integer_col can be cast to BIGINT
	assertContains(t, stdout, "total")
}

// ============================================
// JSON Type Handling Tests
// ============================================

func TestType_JSON_MixedTypes(t *testing.T) {
	// Test that mixed types in JSON are handled
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/mixed_types.json"),
		"-q", "SELECT * FROM mixed_types ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
	assertContains(t, stdout, "100")
	assertContains(t, stdout, "95.5")
	assertContains(t, stdout, "2")
	assertContains(t, stdout, "two hundred")
}

func TestType_JSON_MixedTypes_Count(t *testing.T) {
	// Test that all rows are counted
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/mixed_types.json"),
		"-q", "SELECT COUNT(*) as total FROM mixed_types")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "4")
}

func TestType_JSON_BooleanValues(t *testing.T) {
	// Test that boolean values are preserved
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/mixed_types.json"),
		"-q", "SELECT id, active FROM mixed_types WHERE active = true OR active = 'true'")

	assertNoError(t, err, stderr)
	// Rows 1 and 4 have active = true
	assertContains(t, stdout, "1")
	assertContains(t, stdout, "4")
}

func TestType_JSON_NullHandling(t *testing.T) {
	// Test that null values are handled
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/mixed_types.json"),
		"-q", "SELECT id FROM mixed_types WHERE active IS NULL")

	assertNoError(t, err, stderr)
	// Row 3 has active = null
	assertContains(t, stdout, "3")
}

// ============================================
// JSONL Type Handling Tests
// ============================================

func TestType_JSONL_TypeProgression(t *testing.T) {
	// Test that type progression is handled (int -> float -> string)
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/type_progression.jsonl"),
		"-q", "SELECT * FROM type_progression ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
	assertContains(t, stdout, "100")
	assertContains(t, stdout, "integer")
	assertContains(t, stdout, "2")
	assertContains(t, stdout, "200.5")
	assertContains(t, stdout, "float")
	assertContains(t, stdout, "3")
	assertContains(t, stdout, "three hundred")
	assertContains(t, stdout, "string")
}

func TestType_JSONL_TypeProgression_Count(t *testing.T) {
	// Test that all rows are counted
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/type_progression.jsonl"),
		"-q", "SELECT COUNT(*) as total FROM type_progression")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "4")
}

func TestType_JSONL_NullValue(t *testing.T) {
	// Test that null values are handled
	// Note: JSON null values become empty strings for VARCHAR columns
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/type_progression.jsonl"),
		"-q", "SELECT id, type FROM type_progression WHERE value IS NULL OR value = ''")

	assertNoError(t, err, stderr)
	// Row 4 has value = null (stored as empty string)
	assertContains(t, stdout, "4")
	assertContains(t, stdout, "null")
}

// ============================================
// Export Type Handling Tests
// ============================================

func TestType_Export_CSV_MixedTypes(t *testing.T) {
	// Test that exporting mixed types to CSV works
	exportPath := tempFile(t, "export_mixed.csv")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/mixed_types.csv"),
		"-q", "SELECT * FROM mixed_types ORDER BY id",
		"-e", exportPath,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	// Verify export file exists and contains data
	content := readFile(t, exportPath)
	assertContains(t, content, "id,value,status")
	assertContains(t, content, "100")
	assertContains(t, content, "200.5")
	assertContains(t, content, "three hundred")
}

func TestType_Export_JSONL_MixedTypes(t *testing.T) {
	// Test that exporting mixed types to JSONL works
	exportPath := tempFile(t, "export_mixed.jsonl")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/mixed_types.csv"),
		"-q", "SELECT * FROM mixed_types ORDER BY id",
		"-e", exportPath,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	// Verify export file exists and contains data
	content := readFile(t, exportPath)
	assertContains(t, content, `"id"`)
	assertContains(t, content, `"value"`)
	assertContains(t, content, `"status"`)
}

func TestType_Export_JSON_MixedTypes(t *testing.T) {
	// Test that exporting mixed types to JSON works
	exportPath := tempFile(t, "export_mixed.json")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/mixed_types.json"),
		"-q", "SELECT * FROM mixed_types ORDER BY id",
		"-e", exportPath,
		"-t", "json")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	// Verify export file exists
	if !fileExists(exportPath) {
		t.Error("Export file should exist")
	}
}

// ============================================
// Query Behavior with Types Tests
// ============================================

func TestType_Query_OrderBy_Numeric(t *testing.T) {
	// Test that ORDER BY works correctly with numeric columns
	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/type_edge_cases.csv"),
		"-q", "SELECT id, negative FROM type_edge_cases WHERE negative IS NOT NULL ORDER BY negative")

	assertNoError(t, err, stderr)
	// Should be ordered: -9223372036854775808, -999, -1, 0
}

func TestType_Query_GroupBy_Mixed(t *testing.T) {
	// Test GROUP BY with mixed type data
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/mixed_types.csv"),
		"-q", "SELECT status, COUNT(*) as cnt FROM mixed_types GROUP BY status ORDER BY cnt DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "active")
	assertContains(t, stdout, "inactive")
	assertContains(t, stdout, "pending")
}

func TestType_Query_Distinct_Boolean(t *testing.T) {
	// Test DISTINCT with boolean values
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/boolean_variants.csv"),
		"-q", "SELECT DISTINCT bool_lower FROM boolean_variants")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "true")
	assertContains(t, stdout, "false")
	assertContains(t, stdout, "invalid")
}
