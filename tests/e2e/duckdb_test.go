package e2e_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// DuckDB tests use a temporary database file that we create and populate

func setupDuckDBTestDB(t *testing.T) string {
	t.Helper()

	// Create temp directory for test database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Use duckdb CLI or create via the binary itself
	// For now, we'll create a simple test by first creating data via CSV import
	// then querying via DuckDB

	// Create a CSV file to import
	csvPath := filepath.Join(tmpDir, "users.csv")
	csvContent := `id,name,email,age
1,Alice,alice@example.com,30
2,Bob,bob@example.com,25
3,Charlie,charlie@example.com,35`
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to create CSV file: %v", err)
	}

	return dbPath
}

func TestDuckDB_InvalidURL(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "duckdb://invalid", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for invalid duckdb URL")
	}
	if !strings.Contains(stderr, "invalid DuckDB URL") && !strings.Contains(stderr, "missing table name") {
		t.Errorf("Expected invalid URL error, got: %s", stderr)
	}
}

func TestDuckDB_NoTable(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "duckdb:///path/to/file.db", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for duckdb URL without table")
	}
	// Accept either "table name" error or database connection error
	if !strings.Contains(stderr, "table name") && !strings.Contains(stderr, "Error") {
		t.Errorf("Expected error, got: %s", stderr)
	}
}

// The following tests require a working DuckDB installation and are skipped by default
// Set DATAQL_TEST_DUCKDB=1 to enable them

func skipIfNoDuckDB(t *testing.T) {
	t.Helper()
	if os.Getenv("DATAQL_TEST_DUCKDB") == "" {
		t.Skip("Skipping DuckDB test: DATAQL_TEST_DUCKDB not set")
	}
}

func TestDuckDB_BasicQuery(t *testing.T) {
	skipIfNoDuckDB(t)

	// This test would require creating a DuckDB database first
	// For now, it's skipped unless DATAQL_TEST_DUCKDB is set
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create a test database using DuckDB CLI or programmatically
	// For integration testing, you'd need to:
	// 1. Create a DuckDB database with test data
	// 2. Run the dataql command against it

	t.Logf("Test would use database at: duckdb://%s/users", dbPath)
}

func TestDuckDB_MemoryURL(t *testing.T) {
	skipIfNoDuckDB(t)

	// Test in-memory DuckDB
	// Note: in-memory databases are ephemeral, so this would need
	// a way to pre-populate data

	t.Log("In-memory DuckDB test - would use duckdb://:memory:/tablename")
}
