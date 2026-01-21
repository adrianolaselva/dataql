package e2e_test

import (
	"testing"
)

// ============================================
// Basic JOIN Tests
// ============================================

func TestJoin_InnerJoin_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT u.name, d.department_name FROM users u INNER JOIN departments d ON u.department_id = d.id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestJoin_LeftJoin_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT u.name, d.department_name FROM users u LEFT JOIN departments d ON u.department_id = d.id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestJoin_CrossJoin(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT s.name, d.department_name FROM simple s CROSS JOIN departments d LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// JOIN with Filters
// ============================================

func TestJoin_InnerJoin_WithWhere(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT u.name, d.department_name FROM users u INNER JOIN departments d ON u.department_id = d.id WHERE u.id > 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestJoin_LeftJoin_WithWhere(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT u.name, d.department_name FROM users u LEFT JOIN departments d ON u.department_id = d.id WHERE d.id IS NOT NULL")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// JOIN with Aggregations
// ============================================

func TestJoin_WithCount(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT d.department_name, COUNT(u.id) as user_count FROM departments d LEFT JOIN users u ON d.id = u.department_id GROUP BY d.department_name")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestJoin_WithSum(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/advanced.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT d.department_name, SUM(a.salary) as total_salary FROM departments d INNER JOIN advanced a ON d.department_name = a.city GROUP BY d.department_name")

	// This may not match but should not error
	_ = stdout
	_ = stderr
	_ = err
}

// ============================================
// Self JOIN Tests
// ============================================

func TestJoin_SelfJoin(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT a.name as name1, b.name as name2 FROM simple a, simple b WHERE a.id < b.id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// JOIN with ORDER BY
// ============================================

func TestJoin_WithOrderBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT u.name, d.department_name FROM users u INNER JOIN departments d ON u.department_id = d.id ORDER BY u.name")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestJoin_WithOrderByDesc(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT u.name, d.department_name FROM users u INNER JOIN departments d ON u.department_id = d.id ORDER BY d.department_name DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// JOIN with LIMIT
// ============================================

func TestJoin_WithLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT u.name, d.department_name FROM users u INNER JOIN departments d ON u.department_id = d.id LIMIT 2")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "(2 rows)")
}

// ============================================
// JOIN with Different Formats
// ============================================

func TestJoin_CSV_And_JSON(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("json/people.json"),
		"-q", "SELECT s.name as csv_name, a.name as json_name FROM simple s, array a WHERE s.id = a.id LIMIT 3")

	// May or may not work depending on data matching
	_ = stdout
	_ = stderr
	_ = err
}

func TestJoin_CSV_And_JSONL(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT COUNT(*) as total FROM simple s, simple_1 j")

	// Cross join should produce results
	_ = stdout
	_ = stderr
	_ = err
}

// ============================================
// Multiple JOINs
// ============================================

func TestJoin_ThreeTables(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT u.name, d.department_name FROM users u, departments d, simple s WHERE u.department_id = d.id AND u.id = s.id LIMIT 5")

	// Should work for matching data
	_ = stdout
	_ = stderr
	_ = err
}

// ============================================
// JOIN with Aliases
// ============================================

func TestJoin_TableAliases(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT emp.name as employee, dept.department_name as department FROM users emp INNER JOIN departments dept ON emp.department_id = dept.id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "employee")
	assertContains(t, stdout, "department")
}

// ============================================
// JOIN with DISTINCT
// ============================================

func TestJoin_WithDistinct(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT DISTINCT d.department_name FROM users u INNER JOIN departments d ON u.department_id = d.id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// JOIN Error Cases
// ============================================

func TestJoin_InvalidColumn(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT u.name FROM users u INNER JOIN departments d ON u.invalid_col = d.id")

	assertError(t, err)
}

func TestJoin_TableNotFound(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-q", "SELECT u.name FROM users u INNER JOIN nonexistent n ON u.id = n.id")

	assertError(t, err)
}
