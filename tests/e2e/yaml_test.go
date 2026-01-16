package e2e_test

import (
	"os"
	"strings"
	"testing"
)

func TestYAML_BasicQuery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/users.yaml"), "-q", "SELECT COUNT(*) as cnt FROM users")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestYAML_SelectSpecificColumns(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/users.yaml"), "-q", "SELECT name, email FROM users")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "alice@example.com")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestYAML_YMLExtension(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/products.yml"), "-q", "SELECT COUNT(*) as cnt FROM products")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestYAML_NestedObjects(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/nested.yaml"), "-q", "SELECT user_name, address_city FROM nested")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "New York")
	assertContains(t, stdout, "Jane")
	assertContains(t, stdout, "London")
}

func TestYAML_SingleObject(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/single.yaml"), "-q", "SELECT COUNT(*) as cnt FROM single")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
}

func TestYAML_EmptyFile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/empty.yaml"), "-q", "SELECT COUNT(*) as cnt FROM empty")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "0")
}

func TestYAML_WithLineLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/users.yaml"), "-l", "2", "-q", "SELECT COUNT(*) as cnt FROM users")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

func TestYAML_WithCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/users.yaml"), "-c", "mydata", "-q", "SELECT COUNT(*) as cnt FROM mydata")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestYAML_WhereFilter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/users.yaml"), "-q", "SELECT name FROM users WHERE age = '30'")
	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertNotContains(t, stdout, "Bob")
	assertNotContains(t, stdout, "Charlie")
}

func TestYAML_OrderBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/users.yaml"), "-q", "SELECT name FROM users ORDER BY name ASC")
	assertNoError(t, err, stderr)
	// Check that Alice comes before Bob and Charlie
	aliceIdx := strings.Index(stdout, "Alice")
	bobIdx := strings.Index(stdout, "Bob")
	charlieIdx := strings.Index(stdout, "Charlie")
	if aliceIdx > bobIdx || bobIdx > charlieIdx {
		t.Errorf("Expected names in alphabetical order, got Alice at %d, Bob at %d, Charlie at %d", aliceIdx, bobIdx, charlieIdx)
	}
}

func TestYAML_ExportToCSV(t *testing.T) {
	tmpFile := tempFile(t, "export.csv")

	_, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/users.yaml"), "-q", "SELECT * FROM users", "-e", tmpFile, "-t", "csv")
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

func TestYAML_ExportToJSONL(t *testing.T) {
	tmpFile := tempFile(t, "export.jsonl")

	_, stderr, err := runDataQL(t, "run", "-f", fixture("yaml/users.yaml"), "-q", "SELECT name, email FROM users", "-e", tmpFile, "-t", "jsonl")
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
	assertContains(t, string(content), "alice@example.com")
}
