package e2e_test

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStorageOnly_QueryExistingDatabase(t *testing.T) {
	// Create test data
	csvData := `id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35`

	csvFile := tempFileWithContent(t, "test_users.csv", csvData)
	dbFile := tempFile(t, "test.duckdb")

	// First, create the database with data
	stdout, stderr, err := runDataQL(t, "run",
		"-f", csvFile,
		"-s", dbFile,
		"-q", "SELECT COUNT(*) as total FROM test_users")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")

	// Now query the existing database without --file flag
	stdout, stderr, err = runDataQL(t, "run",
		"-s", dbFile,
		"-q", "SELECT * FROM test_users ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestStorageOnly_QueryWithAggregation(t *testing.T) {
	// Create test data
	csvData := `product,price,quantity
Laptop,999.99,5
Mouse,29.99,20
Keyboard,89.99,15`

	csvFile := tempFileWithContent(t, "products.csv", csvData)
	dbFile := tempFile(t, "products.duckdb")

	// Create the database
	_, stderr, err := runDataQL(t, "run",
		"-f", csvFile,
		"-s", dbFile,
		"-q", "SELECT 1")

	assertNoError(t, err, stderr)

	// Query with aggregation
	stdout, stderr, err := runDataQL(t, "run",
		"-s", dbFile,
		"-q", "SELECT SUM(price * quantity) as total_value FROM products")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total_value")
}

func TestStorageOnly_MultipleTables(t *testing.T) {
	// Create test data files
	usersCSV := `id,name
1,Alice
2,Bob`
	ordersCSV := `id,user_id,total
1,1,100
2,1,200
3,2,150`

	usersFile := tempFileWithContent(t, "users.csv", usersCSV)
	ordersFile := tempFileWithContent(t, "orders.csv", ordersCSV)
	dbFile := tempFile(t, "multi.duckdb")

	// Load first table
	_, stderr, err := runDataQL(t, "run",
		"-f", usersFile,
		"-s", dbFile,
		"-q", "SELECT 1")

	assertNoError(t, err, stderr)

	// Load second table
	_, stderr, err = runDataQL(t, "run",
		"-f", ordersFile,
		"-s", dbFile,
		"-q", "SELECT 1")

	assertNoError(t, err, stderr)

	// Query both tables with JOIN
	stdout, stderr, err := runDataQL(t, "run",
		"-s", dbFile,
		"-q", "SELECT u.name, SUM(o.total) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
}

func TestStorageOnly_ErrorNoFileOrStorage(t *testing.T) {
	_, stderr, err := runDataQL(t, "run",
		"-q", "SELECT 1")

	assertError(t, err)
	assertContains(t, stderr, "either --file or --storage")
}

func TestStorageOnly_ErrorNonExistentStorage(t *testing.T) {
	nonExistentPath := filepath.Join(os.TempDir(), "nonexistent_db_"+randomString(8)+".duckdb")

	_, stderr, err := runDataQL(t, "run",
		"-s", nonExistentPath,
		"-q", "SELECT 1")

	assertError(t, err)
	assertContains(t, stderr, "storage file does not exist")
}

func TestStorageOnly_ListTables(t *testing.T) {
	// Create test data
	csvData := `col1,col2
a,b`

	csvFile := tempFileWithContent(t, "mytable.csv", csvData)
	dbFile := tempFile(t, "list.duckdb")

	// Create database
	_, stderr, err := runDataQL(t, "run",
		"-f", csvFile,
		"-s", dbFile,
		"-q", "SELECT 1")

	assertNoError(t, err, stderr)

	// Query .tables command (shows tables when running in storage-only mode)
	stdout, stderr, err := runDataQL(t, "run",
		"-s", dbFile,
		"-q", ".tables")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "mytable")
}

// Helper function to create temp file with content
func tempFileWithContent(t *testing.T, name, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, name)
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	return filePath
}

// Helper function to generate random string
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[i%len(letters)]
	}
	return string(b)
}
