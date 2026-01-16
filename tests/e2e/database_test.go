package e2e_test

import (
	"os"
	"strings"
	"testing"
)

// Database tests require running database instances.
// Set environment variables to enable:
// - DATAQL_TEST_POSTGRES_URL=postgres://user:pass@localhost:5432/testdb/testtable
// - DATAQL_TEST_MYSQL_URL=mysql://user:pass@localhost:3306/testdb/testtable

func getPostgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATAQL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("Skipping PostgreSQL test: DATAQL_TEST_POSTGRES_URL not set")
	}
	return url
}

func getMySQLURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATAQL_TEST_MYSQL_URL")
	if url == "" {
		t.Skip("Skipping MySQL test: DATAQL_TEST_MYSQL_URL not set")
	}
	return url
}

func TestDatabase_PostgresBasicQuery(t *testing.T) {
	dbURL := getPostgresURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", dbURL, "-q", "SELECT COUNT(*) FROM data")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "COUNT") {
		t.Errorf("Expected COUNT in output, got: %s", stdout)
	}
}

func TestDatabase_PostgresSelectAll(t *testing.T) {
	dbURL := getPostgresURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", dbURL, "-q", "SELECT * FROM data LIMIT 5")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if stdout == "" {
		t.Error("Expected some output")
	}
}

func TestDatabase_PostgresWithCollection(t *testing.T) {
	dbURL := getPostgresURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", dbURL, "-c", "mydata", "-q", "SELECT COUNT(*) FROM mydata")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "COUNT") {
		t.Errorf("Expected COUNT in output, got: %s", stdout)
	}
}

func TestDatabase_PostgresWithLineLimit(t *testing.T) {
	dbURL := getPostgresURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", dbURL, "-l", "5", "-q", "SELECT COUNT(*) as cnt FROM data")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "5") {
		t.Errorf("Expected 5 rows, got: %s", stdout)
	}
}

func TestDatabase_PostgresExportCSV(t *testing.T) {
	dbURL := getPostgresURL(t)
	tmpFile := tempFile(t, "export.csv")
	defer os.Remove(tmpFile)

	_, stderr, err := runDataQL(t, "run", "-f", dbURL, "-q", "SELECT * FROM data LIMIT 3", "-e", tmpFile, "-t", "csv")
	if err != nil {
		t.Fatalf("Failed to export: %v\nstderr: %s", err, stderr)
	}

	content, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read export: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) < 2 {
		t.Errorf("Expected at least header + 1 data row, got %d lines", len(lines))
	}
}

func TestDatabase_PostgresExportJSONL(t *testing.T) {
	dbURL := getPostgresURL(t)
	tmpFile := tempFile(t, "export.jsonl")
	defer os.Remove(tmpFile)

	_, stderr, err := runDataQL(t, "run", "-f", dbURL, "-q", "SELECT * FROM data LIMIT 3", "-e", tmpFile, "-t", "jsonl")
	if err != nil {
		t.Fatalf("Failed to export: %v\nstderr: %s", err, stderr)
	}

	content, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read export: %v", err)
	}

	if len(content) == 0 {
		t.Error("Expected non-empty export")
	}
}

func TestDatabase_MySQLBasicQuery(t *testing.T) {
	dbURL := getMySQLURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", dbURL, "-q", "SELECT COUNT(*) FROM data")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "COUNT") {
		t.Errorf("Expected COUNT in output, got: %s", stdout)
	}
}

func TestDatabase_MySQLSelectAll(t *testing.T) {
	dbURL := getMySQLURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", dbURL, "-q", "SELECT * FROM data LIMIT 5")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if stdout == "" {
		t.Error("Expected some output")
	}
}

func TestDatabase_MySQLWithCollection(t *testing.T) {
	dbURL := getMySQLURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", dbURL, "-c", "mydata", "-q", "SELECT COUNT(*) FROM mydata")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "COUNT") {
		t.Errorf("Expected COUNT in output, got: %s", stdout)
	}
}

func TestDatabase_MySQLWithLineLimit(t *testing.T) {
	dbURL := getMySQLURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", dbURL, "-l", "5", "-q", "SELECT COUNT(*) as cnt FROM data")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "5") {
		t.Errorf("Expected 5 rows, got: %s", stdout)
	}
}

func TestDatabase_MySQLExportCSV(t *testing.T) {
	dbURL := getMySQLURL(t)
	tmpFile := tempFile(t, "export.csv")
	defer os.Remove(tmpFile)

	_, stderr, err := runDataQL(t, "run", "-f", dbURL, "-q", "SELECT * FROM data LIMIT 3", "-e", tmpFile, "-t", "csv")
	if err != nil {
		t.Fatalf("Failed to export: %v\nstderr: %s", err, stderr)
	}

	content, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read export: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) < 2 {
		t.Errorf("Expected at least header + 1 data row, got %d lines", len(lines))
	}
}

func TestDatabase_MySQLExportJSONL(t *testing.T) {
	dbURL := getMySQLURL(t)
	tmpFile := tempFile(t, "export.jsonl")
	defer os.Remove(tmpFile)

	_, stderr, err := runDataQL(t, "run", "-f", dbURL, "-q", "SELECT * FROM data LIMIT 3", "-e", tmpFile, "-t", "jsonl")
	if err != nil {
		t.Fatalf("Failed to export: %v\nstderr: %s", err, stderr)
	}

	content, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read export: %v", err)
	}

	if len(content) == 0 {
		t.Error("Expected non-empty export")
	}
}

// Error cases

func TestDatabase_InvalidPostgresURL(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "postgres://invalid", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for invalid postgres URL")
	}
	if !strings.Contains(stderr, "missing database name") && !strings.Contains(stderr, "invalid") {
		t.Errorf("Expected invalid URL error, got: %s", stderr)
	}
}

func TestDatabase_InvalidMySQLURL(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "mysql://invalid", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for invalid mysql URL")
	}
	if !strings.Contains(stderr, "missing database name") && !strings.Contains(stderr, "invalid") {
		t.Errorf("Expected invalid URL error, got: %s", stderr)
	}
}

func TestDatabase_PostgresNoTable(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "postgres://user:pass@localhost:5432/testdb", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for postgres URL without table")
	}
	if !strings.Contains(stderr, "table name") {
		t.Errorf("Expected table name required error, got: %s", stderr)
	}
}

func TestDatabase_MySQLNoTable(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "mysql://user:pass@localhost:3306/testdb", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for mysql URL without table")
	}
	if !strings.Contains(stderr, "table name") {
		t.Errorf("Expected table name required error, got: %s", stderr)
	}
}
