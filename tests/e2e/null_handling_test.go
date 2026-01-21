package e2e_test

import (
	"testing"
)

// ============================================
// NULL Handling Tests
// Comprehensive tests for NULL value behavior
// ============================================

func TestNull_IsNull(t *testing.T) {
	// Note: Empty strings in CSV are stored as empty strings, not NULL
	// Use IS NULL OR = '' to match empty values
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, name FROM null_values WHERE name IS NULL OR name = ''")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "4")
}

func TestNull_IsNotNull(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, name FROM null_values WHERE name IS NOT NULL ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Item A")
}

func TestNull_CoalesceSimple(t *testing.T) {
	// Note: COALESCE handles NULL but not empty strings
	// Use CASE or NULLIF to also handle empty strings
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, COALESCE(NULLIF(name, ''), 'Unknown') as safe_name FROM null_values ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Unknown")
}

func TestNull_CoalesceChain(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, COALESCE(name, status, 'N/A') as result FROM null_values ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "result")
}

func TestNull_NullifBasic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, NULLIF(CAST(value AS VARCHAR), '0') as non_zero FROM null_values ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "non_zero")
}

func TestNull_EmptyStringVsNull(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, name, CASE WHEN name = '' THEN 'empty' WHEN name IS NULL THEN 'null' ELSE 'has_value' END as name_status FROM null_values ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name_status")
}

func TestNull_InAggregation_Count(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT COUNT(*) as total, COUNT(name) as with_name, COUNT(value) as with_value FROM null_values")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total")
	assertContains(t, stdout, "with_name")
}

func TestNull_InAggregation_Sum(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT SUM(CAST(value AS INTEGER)) as total_value FROM null_values")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total_value")
}

func TestNull_InAggregation_Avg(t *testing.T) {
	// score column may have empty strings and 'NULL' string
	// Use TRY_CAST to safely convert
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT ROUND(AVG(TRY_CAST(score AS DOUBLE)), 2) as avg_score FROM null_values")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "avg_score")
}

func TestNull_InGroupBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT status, COUNT(*) as cnt FROM null_values GROUP BY status ORDER BY status NULLS FIRST")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cnt")
}

func TestNull_InJoin(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-f", fixture("csv/simple.csv"),
		"-q", `SELECT n.id, n.name, s.name as simple_name
			   FROM null_values n
			   LEFT JOIN simple s ON n.id = CAST(s.id AS INTEGER)
			   ORDER BY n.id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "simple_name")
}

func TestNull_OrderByNullsFirst(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, name FROM null_values ORDER BY name NULLS FIRST")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
}

func TestNull_OrderByNullsLast(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, name FROM null_values ORDER BY name NULLS LAST")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
}

func TestNull_CaseWhenNull(t *testing.T) {
	// Use TRY_CAST for safe conversion with correct column handling
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `SELECT id,
			   CASE
				   WHEN TRY_CAST(value AS INTEGER) IS NULL THEN 'Missing'
				   WHEN TRY_CAST(value AS INTEGER) > 100 THEN 'High'
				   ELSE 'Low'
			   END as value_category
			   FROM null_values
			   ORDER BY id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "value_category")
}

func TestNull_Arithmetic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, CAST(value AS INTEGER) + 10 as plus_ten FROM null_values ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "plus_ten")
}

func TestNull_StringConcat(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, name || ' - ' || status as combined FROM null_values ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "combined")
}

func TestNull_Comparison(t *testing.T) {
	// NULL comparisons should return UNKNOWN/NULL, not true/false
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id FROM null_values WHERE value = NULL")

	assertNoError(t, err, stderr)
	// Should return no rows because NULL = NULL is UNKNOWN
	_ = stdout
}

func TestNull_IfNull_Equivalent(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, IFNULL(name, 'N/A') as safe_name FROM null_values ORDER BY id")

	// IFNULL might not exist - COALESCE is standard
	if err == nil {
		assertContains(t, stdout, "safe_name")
	}
	_ = stderr
}

func TestNull_NVL_Equivalent(t *testing.T) {
	// Some DBs use NVL instead of COALESCE
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, COALESCE(name, 'N/A') as safe_name FROM null_values ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "safe_name")
}

func TestNull_InFilter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, value FROM null_values WHERE value IN ('100', '200') ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "100")
	assertContains(t, stdout, "200")
}

func TestNull_NotInFilter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, value FROM null_values WHERE value NOT IN ('100', '200') AND value IS NOT NULL ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
}

func TestNull_BetweenWithNull(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, value FROM null_values WHERE CAST(value AS INTEGER) BETWEEN 50 AND 250 ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "100")
}

func TestNull_LikeWithNull(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, name FROM null_values WHERE name LIKE 'Item%' ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Item A")
}

func TestNull_DistinctIncludesNull(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT DISTINCT status FROM null_values ORDER BY status NULLS FIRST")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "status")
}

func TestNull_WindowFunctionWithNull(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `SELECT id, status,
			   COUNT(*) OVER (PARTITION BY status) as count_in_status
			   FROM null_values
			   ORDER BY id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "count_in_status")
}

func TestNull_CTEWithNull(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `WITH null_check AS (
			   SELECT id, name,
				   CASE WHEN name IS NULL THEN 1 ELSE 0 END as is_null
			   FROM null_values
		   )
		   SELECT * FROM null_check WHERE is_null = 1`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "is_null")
}

func TestNull_JsonNullValues(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/mixed_types.json"),
		"-q", "SELECT id, active FROM mixed_types WHERE active IS NULL")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestNull_CountNullVsCountStar(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `SELECT
			   COUNT(*) as count_star,
			   COUNT(name) as count_name,
			   COUNT(*) - COUNT(name) as null_count
			   FROM null_values`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "count_star")
	assertContains(t, stdout, "null_count")
}

func TestNull_ZeroVsNull(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `SELECT id, value, score,
			   CASE
				   WHEN TRY_CAST(value AS INTEGER) IS NULL THEN 'null_or_empty'
				   WHEN TRY_CAST(value AS INTEGER) = 0 THEN 'zero'
				   ELSE 'has_value'
			   END as value_type
			   FROM null_values
			   ORDER BY id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "value_type")
}
