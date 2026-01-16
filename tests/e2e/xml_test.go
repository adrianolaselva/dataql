package e2e_test

import (
	"testing"
)

func TestXML_BasicQuery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT COUNT(*) as count FROM users")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestXML_SelectSpecificColumns(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT name, email FROM users WHERE id = '1'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "alice@example.com")
}

func TestXML_WithAttributes(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/products.xml"),
		"-q", "SELECT id, category, name, price FROM products WHERE category = 'electronics'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Laptop")
	assertContains(t, stdout, "Phone")
	assertContains(t, stdout, "electronics")
}

func TestXML_NestedElements(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/nested.xml"),
		"-q", "SELECT * FROM nested WHERE id = '1'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "john@test.com")
}

func TestXML_EmptyFile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/empty.xml"),
		"-q", "SELECT COUNT(*) as count FROM empty")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "0")
}

func TestXML_WithLineLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-l", "2",
		"-q", "SELECT COUNT(*) as count FROM users")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

func TestXML_WithCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-c", "people",
		"-q", "SELECT COUNT(*) as count FROM people")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestXML_WhereFilter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT name FROM users WHERE age = '30'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertNotContains(t, stdout, "Bob")
	assertNotContains(t, stdout, "Charlie")
}

func TestXML_OrderBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT name FROM users ORDER BY name ASC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestXML_ExportToCSV(t *testing.T) {
	outputFile := tempFile(t, "xml_export.csv")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	assertContains(t, content, "Alice")
	assertContains(t, content, "Bob")
	assertContains(t, content, "Charlie")
}

func TestXML_ExportToJSONL(t *testing.T) {
	outputFile := tempFile(t, "xml_export.jsonl")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/products.xml"),
		"-q", "SELECT * FROM products",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	assertContains(t, content, "Laptop")
	assertContains(t, content, "Phone")
	assertContains(t, content, "T-Shirt")
}

// Regression test - ensure XML data integrity
func TestXML_DataIntegrity_RegressionBug(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
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
