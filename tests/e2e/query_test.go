package e2e_test

import (
	"testing"
)

func TestQuery_SelectAll(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
	assertContains(t, stdout, "name")
	assertContains(t, stdout, "email")
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "Jane")
	assertContains(t, stdout, "Bob")
}

func TestQuery_SelectSpecificColumns(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, name FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
	assertContains(t, stdout, "name")
	assertContains(t, stdout, "John")
}

func TestQuery_WhereEquals(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id = '1'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertNotContains(t, stdout, "Jane")
	assertNotContains(t, stdout, "Bob")
}

func TestQuery_WhereLike(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE name LIKE 'J%'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "Jane")
	assertNotContains(t, stdout, "Bob")
}

func TestQuery_OrderByAsc(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name FROM simple ORDER BY name ASC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Jane")
	assertContains(t, stdout, "John")
}

func TestQuery_OrderByDesc(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name FROM simple ORDER BY name DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "Jane")
	assertContains(t, stdout, "Bob")
}

func TestQuery_Limit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/large.csv"),
		"-q", "SELECT COUNT(*) as count FROM (SELECT * FROM large LIMIT 5)")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "5")
}

func TestQuery_Count(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT COUNT(*) as total FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestQuery_Sum(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/semicolon.csv"),
		"-d", ";",
		"-q", "SELECT SUM(CAST(value AS REAL)) as total FROM semicolon")

	assertNoError(t, err, stderr)
	// 100.50 + 200.75 + 150.25 = 451.5
	assertContains(t, stdout, "451")
}

func TestQuery_GroupBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-q", "SELECT department_id, COUNT(*) as count FROM users GROUP BY department_id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "10")
	assertContains(t, stdout, "20")
}

func TestQuery_Having(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-q", "SELECT department_id, COUNT(*) as count FROM users GROUP BY department_id HAVING COUNT(*) > 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "10")
	// Note: department_id=20 has only 1 user, so it should not appear in filtered results
	// We check for "(1 rows)" to ensure only one group is returned
	assertContains(t, stdout, "(1 rows)")
}

func TestQuery_InvalidSyntax(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELEC * FORM simple")

	assertError(t, err)
}

func TestQuery_TableNotFound(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM nonexistent_table")

	assertError(t, err)
}

func TestQuery_ColumnNotFound(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT nonexistent_column FROM simple")

	assertError(t, err)
}

func TestQuery_WhereAnd(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id = '1' AND name = 'John'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
}

func TestQuery_WhereOr(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id = '1' OR id = '2'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "Jane")
	assertNotContains(t, stdout, "Bob")
}
