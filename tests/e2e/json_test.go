package e2e_test

import (
	"testing"
)

func TestJSON_Array(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT COUNT(*) as count FROM people")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestJSON_SingleObject(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/single_object.json"),
		"-q", "SELECT COUNT(*) as count FROM single_object")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
}

func TestJSON_NestedFlatten(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/nested.json"),
		"-q", "SELECT user_name FROM nested WHERE id = '1'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
}

func TestJSON_EmptyArray(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/empty_array.json"),
		"-q", "SELECT COUNT(*) as count FROM empty_array")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "0")
}

func TestJSON_WithLineLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-l", "2",
		"-q", "SELECT COUNT(*) as count FROM people")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

func TestJSON_WithCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-c", "custom_data",
		"-q", "SELECT COUNT(*) as count FROM custom_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestJSON_InvalidJSON(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("json/invalid.json"),
		"-q", "SELECT * FROM invalid")

	assertError(t, err)
}

func TestJSON_SelectAllColumns(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people WHERE id = '1'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "30")
}

// Regression tests - these test bugs that were fixed

// TestJSON_DataIntegrity_RegressionBug ensures that JSON data values are actually
// stored and retrievable (not empty). This was a bug where column names were
// being modified in-place causing value lookups to fail.
func TestJSON_DataIntegrity_RegressionBug(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT id, name, age FROM people ORDER BY id")

	assertNoError(t, err, stderr)
	// Verify actual data values are present, not empty strings
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
	assertContains(t, stdout, "30")
	assertContains(t, stdout, "25")
	assertContains(t, stdout, "35")
}

// TestJSON_ExportDataIntegrity_RegressionBug ensures that when exporting JSON
// to CSV/JSONL, the values are preserved (not empty). This was caused by the
// same bug where column name modification broke value lookups.
func TestJSON_ExportDataIntegrity_RegressionBug(t *testing.T) {
	outputFile := tempFile(t, "regression_output.csv")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	// Verify actual data values are in the export, not empty
	assertContains(t, content, "Alice")
	assertContains(t, content, "Bob")
	assertContains(t, content, "Charlie")
}
