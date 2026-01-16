package e2e_test

import (
	"os"
	"strings"
	"testing"
)

func TestORC_BasicQuery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("orc/users.orc"), "-q", "SELECT COUNT(*) as cnt FROM users")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestORC_SelectSpecificColumns(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("orc/users.orc"), "-q", "SELECT name, email FROM users")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "alice@example.com")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestORC_SelectAll(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("orc/users.orc"), "-q", "SELECT * FROM users")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
	assertContains(t, stdout, "name")
	assertContains(t, stdout, "email")
	assertContains(t, stdout, "age")
}

func TestORC_Products(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("orc/products.orc"), "-q", "SELECT name, price FROM products")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Laptop")
	assertContains(t, stdout, "999.99")
	assertContains(t, stdout, "Mouse")
	assertContains(t, stdout, "Desk")
}

func TestORC_EmptyFile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("orc/empty.orc"), "-q", "SELECT COUNT(*) as cnt FROM empty")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "0")
}

func TestORC_WithLineLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("orc/users.orc"), "-l", "2", "-q", "SELECT COUNT(*) as cnt FROM users")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

func TestORC_WithCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("orc/users.orc"), "-c", "mydata", "-q", "SELECT COUNT(*) as cnt FROM mydata")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestORC_WhereFilter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("orc/users.orc"), "-q", "SELECT name FROM users WHERE age = '30'")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertNotContains(t, stdout, "Bob")
	assertNotContains(t, stdout, "Charlie")
}

func TestORC_OrderBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("orc/users.orc"), "-q", "SELECT name FROM users ORDER BY name ASC")
	assertNoError(t, err, stderr)
	aliceIdx := strings.Index(stdout, "Alice")
	bobIdx := strings.Index(stdout, "Bob")
	charlieIdx := strings.Index(stdout, "Charlie")
	if aliceIdx > bobIdx || bobIdx > charlieIdx {
		t.Errorf("Expected names in alphabetical order")
	}
}

func TestORC_ExportToCSV(t *testing.T) {
	tmpFile := tempFile(t, "export.csv")

	_, stderr, err := runDataQL(t, "run", "-f", fixture("orc/users.orc"), "-q", "SELECT * FROM users", "-e", tmpFile, "-t", "csv")
	assertNoError(t, err, stderr)

	content, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 4 { // header + 3 data rows
		t.Errorf("Expected 4 lines (header + 3 data), got %d", len(lines))
	}
}

func TestORC_ExportToJSONL(t *testing.T) {
	tmpFile := tempFile(t, "export.jsonl")

	_, stderr, err := runDataQL(t, "run", "-f", fixture("orc/users.orc"), "-q", "SELECT name, email FROM users", "-e", tmpFile, "-t", "jsonl")
	assertNoError(t, err, stderr)

	content, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 3 {
		t.Errorf("Expected 3 JSONL lines, got %d", len(lines))
	}
	assertContains(t, string(content), "Alice")
}
