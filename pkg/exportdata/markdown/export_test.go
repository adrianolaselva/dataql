package markdown

import (
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/schollz/progressbar/v3"
)

func TestMarkdownExport_BasicTable(t *testing.T) {
	// Create an in-memory DuckDB database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate a test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name VARCHAR, email VARCHAR)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Query the data
	rows, err := db.Query("SELECT * FROM test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	// Create temp file for export
	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "test.md")

	// Create exporter with a silent progress bar
	bar := progressbar.NewOptions(0, progressbar.OptionSetWriter(io.Discard))
	exporter := NewMarkdownExport(rows, exportPath, bar)

	// Export
	if err := exporter.Export(); err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	if err := exporter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read and verify content
	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	contentStr := string(content)

	// Check header row
	if !strings.Contains(contentStr, "| id | name | email |") {
		t.Errorf("Missing header row, got:\n%s", contentStr)
	}

	// Check separator row
	if !strings.Contains(contentStr, "| --- | --- | --- |") {
		t.Errorf("Missing separator row, got:\n%s", contentStr)
	}

	// Check data rows
	if !strings.Contains(contentStr, "| 1 | Alice | alice@example.com |") {
		t.Errorf("Missing first data row, got:\n%s", contentStr)
	}

	if !strings.Contains(contentStr, "| 2 | Bob | bob@example.com |") {
		t.Errorf("Missing second data row, got:\n%s", contentStr)
	}
}

func TestMarkdownExport_EscapePipeCharacters(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER, value VARCHAR)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with pipe character
	_, err = db.Exec("INSERT INTO test VALUES (1, 'value|with|pipes')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "test.md")

	bar := progressbar.NewOptions(0, progressbar.OptionSetWriter(io.Discard))
	exporter := NewMarkdownExport(rows, exportPath, bar)

	if err := exporter.Export(); err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	exporter.Close()

	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	// Pipe characters should be escaped
	if !strings.Contains(string(content), "value\\|with\\|pipes") {
		t.Errorf("Pipe characters not escaped properly, got:\n%s", string(content))
	}
}

func TestMarkdownExport_NullValues(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1, NULL)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "test.md")

	bar := progressbar.NewOptions(0, progressbar.OptionSetWriter(io.Discard))
	exporter := NewMarkdownExport(rows, exportPath, bar)

	if err := exporter.Export(); err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	exporter.Close()

	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	// NULL should be exported as empty string
	if !strings.Contains(string(content), "| 1 |  |") {
		t.Errorf("NULL not handled properly, got:\n%s", string(content))
	}
}
