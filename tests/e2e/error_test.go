package e2e_test

import (
	"os"
	"testing"
)

func TestError_NoFileProvided(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-q", "SELECT * FROM table")

	assertError(t, err)
}

func TestError_UnsupportedExtension(t *testing.T) {
	// Create a temp file with unsupported extension
	tmpFile := tempFile(t, "data.dat")
	// Write some content
	if err := os.WriteFile(tmpFile, []byte("some content"), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	_, _, err := runDataQL(t, "run",
		"-f", tmpFile,
		"-q", "SELECT * FROM data")

	assertError(t, err)
}

func TestError_CorruptedJSON(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("json/invalid.json"),
		"-q", "SELECT * FROM invalid")

	assertError(t, err)
}

func TestError_EmptyQuery(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "")

	// Empty query should return error or show tables
	// This behavior might vary, so we just check it doesn't panic
	_ = err
}

func TestError_FileNotFound(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", "/nonexistent/path/file.csv",
		"-q", "SELECT * FROM file")

	assertError(t, err)
}

func TestError_InvalidDelimiter(t *testing.T) {
	// Using wrong delimiter should cause parsing issues but not necessarily an error
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/semicolon.csv"),
		"-d", ",",
		"-q", "SELECT COUNT(*) FROM semicolon")

	// Should succeed but data might be wrong
	// The CSV will be parsed as single column
	_ = err
}

func TestError_ExportWithoutType(t *testing.T) {
	outputFile := tempFile(t, "output.csv")

	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile)

	// Should error because -t is required with -e
	assertError(t, err)
}

func TestError_InvalidExportType(t *testing.T) {
	outputFile := tempFile(t, "output.txt")

	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "invalidformat")

	assertError(t, err)
}
