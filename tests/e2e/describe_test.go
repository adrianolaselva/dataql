package e2e_test

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDescribe_CSV(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "describe",
		"-f", "tests/fixtures/csv/users.csv",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Table: users")
	assertContains(t, stdout, "Total rows: 3")
	assertContains(t, stdout, "column_name")
	assertContains(t, stdout, "column_type")
	assertContains(t, stdout, "id")
	assertContains(t, stdout, "name")
	assertContains(t, stdout, "BIGINT")
	assertContains(t, stdout, "VARCHAR")
}

func TestDescribe_JSON(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "describe",
		"-f", "tests/fixtures/json/people.json",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Table: people")
	assertContains(t, stdout, "Total rows: 3")
	assertContains(t, stdout, "age")
	assertContains(t, stdout, "BIGINT")
}

func TestDescribe_JSONL(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "describe",
		"-f", "tests/fixtures/jsonl/simple.jsonl",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Table: simple")
	assertContains(t, stdout, "VARCHAR")
}

func TestDescribe_WithCustomCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "describe",
		"-f", "tests/fixtures/csv/users.csv",
		"-c", "my_custom_table",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Table: my_custom_table")
	assertContains(t, stdout, "Total rows: 3")
}

func TestDescribe_WithLineLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "describe",
		"-f", "tests/fixtures/csv/users.csv",
		"-l", "2",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Table: users")
	assertContains(t, stdout, "Total rows: 2")
}

func TestDescribe_MultipleFiles(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "describe",
		"-f", "tests/fixtures/csv/users.csv",
		"-f", "tests/fixtures/csv/departments.csv",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Table: users")
	assertContains(t, stdout, "Table: departments")
}

func TestDescribe_NumericStatistics(t *testing.T) {
	// Create a CSV file with numeric data for statistics testing
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "numbers.csv")

	content := "id,value,score\n1,100,85.5\n2,200,92.3\n3,300,78.9\n4,400,88.1\n5,500,95.0\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}

	stdout, stderr, err := runDataQL(t, "describe",
		"-f", csvPath,
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Table: numbers")
	assertContains(t, stdout, "Total rows: 5")
	// Check for min/max values
	assertContains(t, stdout, "1")   // min id
	assertContains(t, stdout, "5")   // max id
	assertContains(t, stdout, "100") // min value
	assertContains(t, stdout, "500") // max value
}

func TestDescribe_WithNulls(t *testing.T) {
	// Create a CSV file with null values
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "nulls.csv")

	content := "id,name,value\n1,Alice,100\n2,,200\n3,Charlie,\n4,Diana,400\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}

	stdout, stderr, err := runDataQL(t, "describe",
		"-f", csvPath,
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Table: nulls")
	assertContains(t, stdout, "Total rows: 4")
}

func TestDescribe_CompressedFile(t *testing.T) {
	// Create a temp gzip file
	tmpDir := t.TempDir()
	gzPath := filepath.Join(tmpDir, "data.csv.gz")

	content := "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n"
	createGzipFile(t, gzPath, content)

	stdout, stderr, err := runDataQL(t, "describe",
		"-f", gzPath,
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Table: data")
	assertContains(t, stdout, "Total rows: 3")
}

func TestDescribe_NoFileError(t *testing.T) {
	_, _, err := runDataQL(t, "describe")

	assertError(t, err)
}

func TestDescribe_NonExistentFile(t *testing.T) {
	_, _, err := runDataQL(t, "describe",
		"-f", "/nonexistent/file.csv")

	assertError(t, err)
}

func TestDescribe_ExistingStorage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	// First, create a database with some data
	_, stderr, err := runDataQL(t, "run",
		"-f", "tests/fixtures/csv/users.csv",
		"-s", dbPath,
		"-q", "SELECT 1",
		"-Q")
	assertNoError(t, err, stderr)

	// Now describe using the existing storage
	stdout, stderr, err := runDataQL(t, "describe",
		"-s", dbPath,
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Table: users")
}
