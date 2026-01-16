package e2e_test

import (
	"os"
	"strings"
	"testing"
)

func TestAVRO_BasicQuery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("avro/users.avro"), "-q", "SELECT COUNT(*) as cnt FROM users")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestAVRO_SelectSpecificColumns(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("avro/users.avro"), "-q", "SELECT name, email FROM users")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "alice@example.com")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestAVRO_SelectAll(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("avro/users.avro"), "-q", "SELECT * FROM users")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
	assertContains(t, stdout, "name")
	assertContains(t, stdout, "email")
	assertContains(t, stdout, "age")
}

func TestAVRO_Products(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("avro/products.avro"), "-q", "SELECT name, price FROM products")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Laptop")
	assertContains(t, stdout, "999.99")
	assertContains(t, stdout, "Mouse")
	assertContains(t, stdout, "Desk")
}

func TestAVRO_EmptyFile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("avro/empty.avro"), "-q", "SELECT COUNT(*) as cnt FROM empty")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "0")
}

func TestAVRO_WithLineLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("avro/users.avro"), "-l", "2", "-q", "SELECT COUNT(*) as cnt FROM users")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

func TestAVRO_WithCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("avro/users.avro"), "-c", "mydata", "-q", "SELECT COUNT(*) as cnt FROM mydata")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestAVRO_WhereFilter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("avro/users.avro"), "-q", "SELECT name FROM users WHERE age = '30'")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertNotContains(t, stdout, "Bob")
	assertNotContains(t, stdout, "Charlie")
}

func TestAVRO_OrderBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("avro/users.avro"), "-q", "SELECT name FROM users ORDER BY name ASC")
	assertNoError(t, err, stderr)
	aliceIdx := strings.Index(stdout, "Alice")
	bobIdx := strings.Index(stdout, "Bob")
	charlieIdx := strings.Index(stdout, "Charlie")
	if aliceIdx > bobIdx || bobIdx > charlieIdx {
		t.Errorf("Expected names in alphabetical order")
	}
}

func TestAVRO_ExportToCSV(t *testing.T) {
	tmpFile := tempFile(t, "export.csv")

	_, stderr, err := runDataQL(t, "run", "-f", fixture("avro/users.avro"), "-q", "SELECT * FROM users", "-e", tmpFile, "-t", "csv")
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

func TestAVRO_ExportToJSONL(t *testing.T) {
	tmpFile := tempFile(t, "export.jsonl")

	_, stderr, err := runDataQL(t, "run", "-f", fixture("avro/users.avro"), "-q", "SELECT name, email FROM users", "-e", tmpFile, "-t", "jsonl")
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
