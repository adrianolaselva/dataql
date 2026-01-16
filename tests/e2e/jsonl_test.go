package e2e_test

import (
	"testing"
)

func TestJSONL_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT COUNT(*) as count FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestJSONL_WithEmptyLines(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/with_empty_lines.jsonl"),
		"-q", "SELECT COUNT(*) as count FROM with_empty_lines")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestJSONL_NdjsonExtension(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/data.ndjson"),
		"-q", "SELECT COUNT(*) as count FROM data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

func TestJSONL_WithLineLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-l", "2",
		"-q", "SELECT COUNT(*) as count FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

func TestJSONL_MixedSchema(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/mixed_schema.jsonl"),
		"-q", "SELECT COUNT(*) as count FROM mixed_schema")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestJSONL_WithCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-c", "logs",
		"-q", "SELECT COUNT(*) as count FROM logs")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestJSONL_SelectSpecificFields(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT event FROM simple WHERE id = '1'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "login")
}

// Regression tests - these test bugs that were fixed

// TestJSONL_DataIntegrity_RegressionBug ensures that JSONL data values are actually
// stored and retrievable (not empty). This was a bug where column names were
// being modified in-place causing value lookups to fail.
func TestJSONL_DataIntegrity_RegressionBug(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT id, event, timestamp FROM simple ORDER BY id")

	assertNoError(t, err, stderr)
	// Verify actual data values are present, not empty strings
	assertContains(t, stdout, "login")
	assertContains(t, stdout, "logout")
	assertContains(t, stdout, "purchase")
	assertContains(t, stdout, "2024-01-01")
}

// TestJSONL_ExportDataIntegrity_RegressionBug ensures that when exporting JSONL
// to CSV/JSONL, the values are preserved (not empty).
func TestJSONL_ExportDataIntegrity_RegressionBug(t *testing.T) {
	outputFile := tempFile(t, "regression_jsonl_output.csv")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	// Verify actual data values are in the export, not empty
	assertContains(t, content, "login")
	assertContains(t, content, "logout")
	assertContains(t, content, "purchase")
}
