package e2e_test

import (
	"testing"
)

func TestParquet_BasicQuery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT COUNT(*) as count FROM users")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestParquet_SelectSpecificColumns(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT name, email FROM users WHERE id = '1'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "alice@example.com")
}

func TestParquet_SelectAllData(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT * FROM users")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestParquet_WhereFilter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT name FROM users WHERE age = '30'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertNotContains(t, stdout, "Bob")
	assertNotContains(t, stdout, "Charlie")
}

func TestParquet_OrderBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT name FROM users ORDER BY name ASC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestParquet_EmptyFile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/empty.parquet"),
		"-q", "SELECT COUNT(*) as count FROM empty")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "0")
}

func TestParquet_WithLineLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-l", "2",
		"-q", "SELECT COUNT(*) as count FROM users")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

func TestParquet_WithCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-c", "people",
		"-q", "SELECT COUNT(*) as count FROM people")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestParquet_ExportToCSV(t *testing.T) {
	outputFile := tempFile(t, "parquet_export.csv")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	assertContains(t, content, "Alice")
	assertContains(t, content, "Bob")
	assertContains(t, content, "Charlie")
}

func TestParquet_ExportToJSONL(t *testing.T) {
	outputFile := tempFile(t, "parquet_export.jsonl")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/products.parquet"),
		"-q", "SELECT * FROM products",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	assertContains(t, content, "Laptop")
	assertContains(t, content, "Phone")
	assertContains(t, content, "T-Shirt")
}

func TestParquet_Products(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/products.parquet"),
		"-q", "SELECT name, price FROM products WHERE category = 'electronics'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Laptop")
	assertContains(t, stdout, "Phone")
	assertNotContains(t, stdout, "T-Shirt")
}

// Regression test - ensure Parquet data integrity
func TestParquet_DataIntegrity_RegressionBug(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT id, name, email, age FROM users ORDER BY id")

	assertNoError(t, err, stderr)
	// Verify actual data values are present, not empty strings
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
	assertContains(t, stdout, "alice@example.com")
	assertContains(t, stdout, "bob@example.com")
	assertContains(t, stdout, "charlie@example.com")
}
