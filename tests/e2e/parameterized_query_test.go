package e2e_test

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParam_BasicString(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", "tests/fixtures/csv/users.csv",
		"-q", "SELECT * FROM users WHERE name = :name",
		"-p", "name=Alice",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertNotContains(t, stdout, "Bob")
	assertNotContains(t, stdout, "Charlie")
	assertContains(t, stdout, "(1 row")
}

func TestParam_BasicNumber(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", "tests/fixtures/csv/users.csv",
		"-q", "SELECT * FROM users WHERE id = :id",
		"-p", "id=2",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "(1 row")
}

func TestParam_MultipleParams(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", "tests/fixtures/csv/users.csv",
		"-q", "SELECT * FROM users WHERE id >= :min_id AND id <= :max_id",
		"-p", "min_id=1",
		"-p", "max_id=2",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertNotContains(t, stdout, "Charlie")
	assertContains(t, stdout, "(2 rows)")
}

func TestParam_DollarSyntax(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", "tests/fixtures/csv/users.csv",
		"-q", "SELECT * FROM users WHERE name = $name",
		"-p", "name=Charlie",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Charlie")
	assertContains(t, stdout, "(1 row")
}

func TestParam_MixedSyntax(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", "tests/fixtures/csv/users.csv",
		"-q", "SELECT * FROM users WHERE id = :id OR name = $name",
		"-p", "id=1",
		"-p", "name=Bob",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "(2 rows)")
}

func TestParam_StringWithSpaces(t *testing.T) {
	// Create a temp CSV file with spaces in values
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "names.csv")
	content := "id,full_name\n1,John Doe\n2,Jane Smith\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}

	stdout, stderr, err := runDataQL(t, "run",
		"-f", csvPath,
		"-q", "SELECT * FROM names WHERE full_name = :name",
		"-p", "name=John Doe",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John Doe")
	assertContains(t, stdout, "(1 row")
}

func TestParam_FloatValue(t *testing.T) {
	// Create a temp CSV file with float values
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "prices.csv")
	content := "id,price\n1,19.99\n2,29.99\n3,39.99\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}

	stdout, stderr, err := runDataQL(t, "run",
		"-f", csvPath,
		"-q", "SELECT * FROM prices WHERE price < :max_price",
		"-p", "max_price=30.00",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "19.99")
	assertContains(t, stdout, "29.99")
	assertNotContains(t, stdout, "39.99")
	assertContains(t, stdout, "(2 rows)")
}

func TestParam_BooleanValue(t *testing.T) {
	// Create a temp CSV file with boolean values
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "flags.csv")
	content := "id,active\n1,true\n2,false\n3,true\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}

	stdout, stderr, err := runDataQL(t, "run",
		"-f", csvPath,
		"-q", "SELECT * FROM flags WHERE active = :active",
		"-p", "active=true",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "(2 rows)")
}

func TestParam_WithExport(t *testing.T) {
	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "exported.csv")

	_, stderr, err := runDataQL(t, "run",
		"-f", "tests/fixtures/csv/users.csv",
		"-q", "SELECT * FROM users WHERE name = :name",
		"-p", "name=Alice",
		"-e", exportPath,
		"-t", "csv",
		"-Q")

	assertNoError(t, err, stderr)

	// Verify exported file
	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}
	assertContains(t, string(content), "Alice")
	assertNotContains(t, string(content), "Bob")
}

func TestParam_WithVerbose(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", "tests/fixtures/csv/users.csv",
		"-q", "SELECT * FROM users WHERE id = :id",
		"-p", "id=1",
		"-v",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	// Verbose output goes to stdout
	assertContains(t, stdout, "Parsed query parameters")
}

func TestParam_InvalidFormat(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", "tests/fixtures/csv/users.csv",
		"-q", "SELECT * FROM users",
		"-p", "invalid_param")

	assertError(t, err)
}

func TestParam_LikeQuery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", "tests/fixtures/csv/users.csv",
		"-q", "SELECT * FROM users WHERE name LIKE :pattern",
		"-p", "pattern=%li%",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Charlie")
	assertNotContains(t, stdout, "Bob")
}

func TestParam_InSubquery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", "tests/fixtures/csv/users.csv",
		"-q", "SELECT * FROM users WHERE department_id = :dept ORDER BY name",
		"-p", "dept=10",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Charlie")
}

func TestParam_NegativeNumber(t *testing.T) {
	// Create a temp CSV file with negative values
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "numbers.csv")
	content := "id,value\n1,-10\n2,0\n3,10\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}

	stdout, stderr, err := runDataQL(t, "run",
		"-f", csvPath,
		"-q", "SELECT * FROM numbers WHERE value > :min",
		"-p", "min=-5",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "0")
	assertContains(t, stdout, "10")
	assertContains(t, stdout, "(2 rows)")
}

func TestParam_WithJSON(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", "tests/fixtures/json/people.json",
		"-q", "SELECT * FROM people WHERE name = :name",
		"-p", "name=Alice",
		"-Q")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "(1 row")
}
