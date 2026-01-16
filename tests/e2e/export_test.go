package e2e_test

import (
	"strings"
	"testing"
)

func TestExport_ToCSV(t *testing.T) {
	outputFile := tempFile(t, "output.csv")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)

	if !fileExists(outputFile) {
		t.Fatal("Expected output file to be created")
	}

	content := readFile(t, outputFile)
	assertContains(t, content, "id")
	assertContains(t, content, "John")
}

func TestExport_ToJSONL(t *testing.T) {
	outputFile := tempFile(t, "output.jsonl")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)

	if !fileExists(outputFile) {
		t.Fatal("Expected output file to be created")
	}

	content := readFile(t, outputFile)
	// JSONL should have JSON objects
	assertContains(t, content, "John")
}

func TestExport_WithFilter(t *testing.T) {
	outputFile := tempFile(t, "filtered.csv")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id = '1'",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	assertContains(t, content, "John")
	assertNotContains(t, content, "Jane")
	assertNotContains(t, content, "Bob")
}

func TestExport_Aggregation(t *testing.T) {
	outputFile := tempFile(t, "aggregation.jsonl")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT COUNT(*) as total FROM simple",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	assertContains(t, content, "3")
}

func TestExport_FromJSON(t *testing.T) {
	outputFile := tempFile(t, "from_json.csv")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/array.json"),
		"-q", "SELECT * FROM array",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	assertContains(t, content, "Alice")
	assertContains(t, content, "Bob")
}

func TestExport_FromJSONL(t *testing.T) {
	outputFile := tempFile(t, "from_jsonl.csv")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	assertContains(t, content, "login")
	assertContains(t, content, "logout")
}

func TestExport_CSVToJSONL(t *testing.T) {
	outputFile := tempFile(t, "csv_to_jsonl.jsonl")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, name FROM simple",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)

	content := readFile(t, outputFile)
	// Each line should be valid JSON
	lines := strings.Split(strings.TrimSpace(content), "\n")
	if len(lines) != 3 {
		t.Errorf("Expected 3 lines, got %d", len(lines))
	}
}
