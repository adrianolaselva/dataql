package e2e_test

import (
	"testing"
)

// ============================================
// Complex WHERE Clause Tests
// ============================================

func TestQuery_ComplexWhere_AND(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/advanced.csv"),
		"-q", "SELECT * FROM advanced WHERE age > 25 AND city = 'NYC'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_ComplexWhere_OR(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE name = 'John' OR name = 'Jane'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "Jane")
}

func TestQuery_ComplexWhere_AND_OR(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/advanced.csv"),
		"-q", "SELECT * FROM advanced WHERE (age > 30 AND city = 'NYC') OR (age < 25 AND city = 'LA')")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_NestedConditions(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/advanced.csv"),
		"-q", "SELECT * FROM advanced WHERE (age > 20 AND age < 40) AND (city = 'NYC' OR city = 'LA')")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// IN Clause Tests
// ============================================

func TestQuery_INClause(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE name IN ('John', 'Jane')")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "Jane")
}

func TestQuery_INClause_Numbers(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id IN (1, 2)")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_NotIN(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE name NOT IN ('John')")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Jane")
}

// ============================================
// BETWEEN Tests
// ============================================

func TestQuery_BETWEEN(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id BETWEEN 1 AND 2")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// LIKE Pattern Tests
// ============================================

func TestQuery_LIKE_StartsWith(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE name LIKE 'J%'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "Jane")
}

func TestQuery_LIKE_EndsWith(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE email LIKE '%example.com'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_LIKE_Contains(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE email LIKE '%@%'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_NOT_LIKE(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE name NOT LIKE 'J%'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Bob")
}

// ============================================
// NULL Handling Tests
// ============================================

func TestQuery_IS_NULL(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE email IS NOT NULL")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// Numeric Comparisons
// ============================================

func TestQuery_GreaterThan(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id > 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_LessThanOrEqual(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id <= 2")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_NotEqual(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id != 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// Aggregation Tests
// ============================================

func TestQuery_COUNT(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT COUNT(*) as total FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestQuery_COUNT_Column(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT COUNT(name) as name_count FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_SUM(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT SUM(id) as total_id FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "6") // 1+2+3
}

func TestQuery_AVG(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT AVG(id) as avg_id FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2") // (1+2+3)/3
}

func TestQuery_MIN(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT MIN(id) as min_id FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
}

func TestQuery_MAX(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT MAX(id) as max_id FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestQuery_MultipleAggregations(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT COUNT(*) as cnt, MIN(id) as min_id, MAX(id) as max_id FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
	assertContains(t, stdout, "1")
}

// ============================================
// GROUP BY Tests
// ============================================

func TestQuery_GroupBy_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/advanced.csv"),
		"-q", "SELECT city, COUNT(*) as count FROM advanced GROUP BY city")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_GroupByMultipleColumns(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/advanced.csv"),
		"-q", "SELECT city, age, COUNT(*) as count FROM advanced GROUP BY city, age")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// HAVING Tests
// ============================================

func TestQuery_Having_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/advanced.csv"),
		"-q", "SELECT city, COUNT(*) as count FROM advanced GROUP BY city HAVING COUNT(*) > 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// ORDER BY Tests
// ============================================

func TestQuery_OrderBy_ASC(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple ORDER BY name ASC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_OrderBy_DESC(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple ORDER BY id DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_OrderBy_Multiple(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/advanced.csv"),
		"-q", "SELECT * FROM advanced ORDER BY city ASC, age DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// LIMIT and OFFSET Tests
// ============================================

func TestQuery_Limit_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple LIMIT 2")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "(2 rows)")
}

func TestQuery_LimitOffset(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple LIMIT 2 OFFSET 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "(2 rows)")
}

// ============================================
// DISTINCT Tests
// ============================================

func TestQuery_Distinct(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/advanced.csv"),
		"-q", "SELECT DISTINCT city FROM advanced")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_DistinctMultiple(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/advanced.csv"),
		"-q", "SELECT DISTINCT city, age FROM advanced")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// UNION Tests
// ============================================

func TestQuery_UnionAll(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name FROM simple WHERE id = 1 UNION ALL SELECT name FROM simple WHERE id = 2")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "Jane")
}

// ============================================
// Subquery Tests
// ============================================

func TestQuery_Subquery_IN(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id IN (SELECT id FROM simple WHERE id < 3)")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_Subquery_Scalar(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id = (SELECT MAX(id) FROM simple)")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Bob")
}

// ============================================
// Alias Tests
// ============================================

func TestQuery_ColumnAlias(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name AS person_name, email AS contact FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "person_name")
	assertContains(t, stdout, "contact")
}

func TestQuery_TableAlias(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT s.name, s.email FROM simple s")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
}

// ============================================
// CASE Expression Tests
// ============================================

func TestQuery_CaseWhen(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, CASE WHEN id = 1 THEN 'First' ELSE 'Other' END as position FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "First")
	assertContains(t, stdout, "Other")
}

// ============================================
// String Functions Tests
// ============================================

func TestQuery_Upper(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT UPPER(name) as upper_name FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "JOHN")
}

func TestQuery_Lower(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT LOWER(name) as lower_name FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "john")
}

func TestQuery_Length(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, LENGTH(name) as name_len FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestQuery_Substr(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT SUBSTR(name, 1, 2) as short_name FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Jo")
}

func TestQuery_Concat(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name || ' (' || email || ')' as full_info FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
}
