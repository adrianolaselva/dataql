package html

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

func TestHTMLExport_BasicTable(t *testing.T) {
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
	exportPath := filepath.Join(tmpDir, "test.html")

	// Create exporter with a silent progress bar
	bar := progressbar.NewOptions(0, progressbar.OptionSetWriter(io.Discard))
	exporter := NewHTMLExport(rows, exportPath, bar)

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

	// Check HTML structure
	if !strings.Contains(contentStr, "<!DOCTYPE html>") {
		t.Errorf("Missing DOCTYPE declaration")
	}

	if !strings.Contains(contentStr, "<table>") {
		t.Errorf("Missing table element")
	}

	// Check header
	if !strings.Contains(contentStr, "<th>id</th>") {
		t.Errorf("Missing id header")
	}

	if !strings.Contains(contentStr, "<th>name</th>") {
		t.Errorf("Missing name header")
	}

	if !strings.Contains(contentStr, "<th>email</th>") {
		t.Errorf("Missing email header")
	}

	// Check data rows
	if !strings.Contains(contentStr, "<td>Alice</td>") {
		t.Errorf("Missing Alice data")
	}

	if !strings.Contains(contentStr, "<td>Bob</td>") {
		t.Errorf("Missing Bob data")
	}

	if !strings.Contains(contentStr, "<td>alice@example.com</td>") {
		t.Errorf("Missing Alice email data")
	}

	// Check closing tags
	if !strings.Contains(contentStr, "</table>") {
		t.Errorf("Missing closing table tag")
	}

	if !strings.Contains(contentStr, "</html>") {
		t.Errorf("Missing closing html tag")
	}
}

func TestHTMLExport_EscapeHTMLCharacters(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER, value VARCHAR)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with HTML special characters
	_, err = db.Exec("INSERT INTO test VALUES (1, '<script>alert(\"xss\")</script>')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "test.html")

	bar := progressbar.NewOptions(0, progressbar.OptionSetWriter(io.Discard))
	exporter := NewHTMLExport(rows, exportPath, bar)

	if err := exporter.Export(); err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	exporter.Close()

	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	// HTML special characters should be escaped
	contentStr := string(content)

	if strings.Contains(contentStr, "<script>") {
		t.Errorf("Script tags not escaped - XSS vulnerability!")
	}

	if !strings.Contains(contentStr, "&lt;script&gt;") {
		t.Errorf("HTML not properly escaped, got:\n%s", contentStr)
	}
}

func TestHTMLExport_NullValues(t *testing.T) {
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
	exportPath := filepath.Join(tmpDir, "test.html")

	bar := progressbar.NewOptions(0, progressbar.OptionSetWriter(io.Discard))
	exporter := NewHTMLExport(rows, exportPath, bar)

	if err := exporter.Export(); err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	exporter.Close()

	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	// NULL should be exported as empty cell
	if !strings.Contains(string(content), "<td></td>") {
		t.Errorf("NULL not handled properly, got:\n%s", string(content))
	}
}

func TestHTMLExport_HasStyles(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "test.html")

	bar := progressbar.NewOptions(0, progressbar.OptionSetWriter(io.Discard))
	exporter := NewHTMLExport(rows, exportPath, bar)

	if err := exporter.Export(); err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	exporter.Close()

	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	// Check that CSS styles are included
	contentStr := string(content)

	if !strings.Contains(contentStr, "<style>") {
		t.Errorf("Missing style element")
	}

	if !strings.Contains(contentStr, "border-collapse") {
		t.Errorf("Missing table styling")
	}
}
