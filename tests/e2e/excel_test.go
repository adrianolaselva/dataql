package e2e_test

import (
	"testing"
)

func TestExcel_BasicQuery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT COUNT(*) as count FROM users")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestExcel_SelectSpecificColumns(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT name, email FROM users WHERE id = '1'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "alice@example.com")
}

func TestExcel_SelectAllData(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT * FROM users")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestExcel_WhereFilter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT name FROM users WHERE age = '30'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertNotContains(t, stdout, "Bob")
	assertNotContains(t, stdout, "Charlie")
}

func TestExcel_OrderBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT name FROM users ORDER BY name ASC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestExcel_EmptyFile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/empty.xlsx"),
		"-q", "SELECT COUNT(*) as count FROM empty")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "0")
}

func TestExcel_WithLineLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-l", "2",
		"-q", "SELECT COUNT(*) as count FROM users")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

func TestExcel_WithCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-c", "people",
		"-q", "SELECT COUNT(*) as count FROM people")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestExcel_ExportToCSV(t *testing.T) {
	outputFile := tempFile(t, "excel_export.csv")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	assertContains(t, content, "Alice")
	assertContains(t, content, "Bob")
	assertContains(t, content, "Charlie")
}

func TestExcel_ExportToJSONL(t *testing.T) {
	outputFile := tempFile(t, "excel_export.jsonl")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/products.xlsx"),
		"-q", "SELECT * FROM products",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	assertContains(t, content, "Laptop")
	assertContains(t, content, "Phone")
	assertContains(t, content, "T-Shirt")
}

func TestExcel_Products(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/products.xlsx"),
		"-q", "SELECT name, price FROM products WHERE category = 'electronics'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Laptop")
	assertContains(t, stdout, "Phone")
	assertNotContains(t, stdout, "T-Shirt")
}

// Regression test - ensure Excel data integrity
func TestExcel_DataIntegrity_RegressionBug(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
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
