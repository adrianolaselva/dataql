package e2e_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// createTestSQLiteDB creates a test SQLite database with sample data
func createTestSQLiteDB(t *testing.T, filename string) string {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, filename)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to create test SQLite database: %v", err)
	}
	defer db.Close()

	// Create users table
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			email TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO users (id, name, email, age) VALUES
		(1, 'Alice', 'alice@example.com', 30),
		(2, 'Bob', 'bob@example.com', 25),
		(3, 'Charlie', 'charlie@example.com', 35),
		(4, 'Diana', 'diana@example.com', 28),
		(5, 'Eve', 'eve@example.com', 32)
	`)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	return dbPath
}

// createMultiTableSQLiteDB creates a SQLite database with multiple tables
func createMultiTableSQLiteDB(t *testing.T, filename string) string {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, filename)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to create test SQLite database: %v", err)
	}
	defer db.Close()

	// Create users table
	_, err = db.Exec(`CREATE TABLE users (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}
	_, err = db.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')`)
	if err != nil {
		t.Fatalf("failed to insert users: %v", err)
	}

	// Create orders table
	_, err = db.Exec(`CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL)`)
	if err != nil {
		t.Fatalf("failed to create orders table: %v", err)
	}
	_, err = db.Exec(`INSERT INTO orders VALUES (1, 1, 99.99), (2, 1, 50.00), (3, 2, 75.50)`)
	if err != nil {
		t.Fatalf("failed to insert orders: %v", err)
	}

	return dbPath
}

func TestSQLite_BasicQuery(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "test.db")

	stdout, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q", "SELECT * FROM users")
	if err != nil {
		t.Errorf("SQLite query failed: %v\nstderr: %s", err, stderr)
	}

	// Check that expected columns are present
	if !strings.Contains(stdout, "id") || !strings.Contains(stdout, "name") || !strings.Contains(stdout, "email") {
		t.Errorf("Expected columns not found in output: %s", stdout)
	}

	// Check that expected data is present
	if !strings.Contains(stdout, "Alice") || !strings.Contains(stdout, "Bob") {
		t.Errorf("Expected data not found in output: %s", stdout)
	}
}

func TestSQLite_SelectSpecificColumns(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "test.db")

	stdout, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q", "SELECT name, email FROM users")
	if err != nil {
		t.Errorf("SQLite query failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "name") || !strings.Contains(stdout, "email") {
		t.Errorf("Expected columns not found in output: %s", stdout)
	}
}

func TestSQLite_WithWhereClause(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "test.db")

	stdout, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q", "SELECT * FROM users WHERE age > 30")
	if err != nil {
		t.Errorf("SQLite query failed: %v\nstderr: %s", err, stderr)
	}

	// Should include Charlie (35) and Eve (32)
	if !strings.Contains(stdout, "Charlie") || !strings.Contains(stdout, "Eve") {
		t.Errorf("Expected filtered data not found in output: %s", stdout)
	}
}

func TestSQLite_WithOrderBy(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "test.db")

	stdout, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q", "SELECT name FROM users ORDER BY age DESC LIMIT 2")
	if err != nil {
		t.Errorf("SQLite query failed: %v\nstderr: %s", err, stderr)
	}

	// Should show Charlie (35) first, then Eve (32)
	if !strings.Contains(stdout, "Charlie") {
		t.Errorf("Expected ordered data not found in output: %s", stdout)
	}
}

func TestSQLite_WithLineLimit(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "test.db")

	stdout, stderr, err := runDataQL(t, "run", "-f", dbPath, "-l", "2", "-q", "SELECT * FROM users")
	if err != nil {
		t.Errorf("SQLite query with limit failed: %v\nstderr: %s", err, stderr)
	}

	// Should only have 2 data rows (plus header)
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	// Filter out non-data lines (tables info, etc)
	dataLines := 0
	for _, line := range lines {
		if strings.Contains(line, "@example.com") {
			dataLines++
		}
	}
	if dataLines > 2 {
		t.Errorf("Expected at most 2 data rows, got %d", dataLines)
	}
}

func TestSQLite_MultipleTablesImport(t *testing.T) {
	dbPath := createMultiTableSQLiteDB(t, "multi.db")

	stdout, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q", "SELECT * FROM users")
	if err != nil {
		t.Errorf("SQLite multi-table query failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "Alice") || !strings.Contains(stdout, "Bob") {
		t.Errorf("Expected users data not found: %s", stdout)
	}
}

func TestSQLite_QueryOrdersTable(t *testing.T) {
	dbPath := createMultiTableSQLiteDB(t, "multi.db")

	stdout, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q", "SELECT * FROM orders")
	if err != nil {
		t.Errorf("SQLite orders query failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "99.99") || !strings.Contains(stdout, "75.5") {
		t.Errorf("Expected orders data not found: %s", stdout)
	}
}

func TestSQLite_JoinTables(t *testing.T) {
	dbPath := createMultiTableSQLiteDB(t, "multi.db")

	stdout, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q",
		"SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id")
	if err != nil {
		t.Errorf("SQLite join query failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "Alice") || !strings.Contains(stdout, "99.99") {
		t.Errorf("Expected joined data not found: %s", stdout)
	}
}

func TestSQLite_Aggregation(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "test.db")

	stdout, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q", "SELECT COUNT(*) as count FROM users")
	if err != nil {
		t.Errorf("SQLite aggregation failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "5") {
		t.Errorf("Expected count of 5, got: %s", stdout)
	}
}

func TestSQLite_ExportToCSV(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "test.db")
	outputFile := tempFile(t, "sqlite_export.csv")

	_, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q", "SELECT * FROM users", "-e", outputFile, "-t", "csv")
	if err != nil {
		t.Errorf("SQLite export to CSV failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("Export file was not created")
	}
}

func TestSQLite_ExportToJSON(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "test.db")
	outputFile := tempFile(t, "sqlite_export.json")

	_, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q", "SELECT * FROM users LIMIT 3", "-e", outputFile, "-t", "json")
	if err != nil {
		t.Errorf("SQLite export to JSON failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("Export file was not created")
	}
}

func TestSQLite_FileNotFound(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "/nonexistent/path/test.db", "-q", "SELECT * FROM users")
	if err == nil {
		t.Error("Expected error for non-existent SQLite file")
	}
	if !strings.Contains(stderr, "Error") && !strings.Contains(stderr, "unable to open") {
		t.Errorf("Expected file not found error, got: %s", stderr)
	}
}

func TestSQLite_Sqlite3Extension(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "test.sqlite3")

	stdout, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q", "SELECT * FROM users LIMIT 2")
	if err != nil {
		t.Errorf("SQLite3 extension query failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "Alice") {
		t.Errorf("Expected data not found: %s", stdout)
	}
}

func TestSQLite_SqliteExtension(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "test.sqlite")

	stdout, stderr, err := runDataQL(t, "run", "-f", dbPath, "-q", "SELECT * FROM users LIMIT 2")
	if err != nil {
		t.Errorf("SQLite extension query failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "Alice") {
		t.Errorf("Expected data not found: %s", stdout)
	}
}
