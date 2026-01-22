package e2e_test

import (
	"testing"
)

// ============================================
// Mixed Format JOIN Tests
// Tests for Issue #17: Support mixed file formats in JOIN
// ============================================

// TestMixedFormats_CSV_JSON tests JOIN between CSV and JSON files
func TestMixedFormats_CSV_JSON(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers_csv.csv"),
		"-f", fixture("json/orders_json.json"),
		"-q", "SELECT c.name, c.department, o.total FROM customers_csv c JOIN orders_json o ON c.id = o.customer_id ORDER BY c.name")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "rows)")
}

// TestMixedFormats_CSV_JSON_LeftJoin tests LEFT JOIN between CSV and JSON
func TestMixedFormats_CSV_JSON_LeftJoin(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers_csv.csv"),
		"-f", fixture("json/orders_json.json"),
		"-q", "SELECT c.name, o.total FROM customers_csv c LEFT JOIN orders_json o ON c.id = o.customer_id ORDER BY c.name")

	assertNoError(t, err, stderr)
	// Diana has no orders, should still appear with NULL total
	assertContains(t, stdout, "Diana")
	// 5 rows: Alice (2 orders), Bob (1), Charlie (1), Diana (NULL)
	assertContains(t, stdout, "(5 rows)")
}

// TestMixedFormats_CSV_JSONL tests JOIN between CSV and JSONL files
func TestMixedFormats_CSV_JSONL(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers_csv.csv"),
		"-f", fixture("jsonl/salaries_jsonl.jsonl"),
		"-q", "SELECT c.name, s.salary FROM customers_csv c JOIN salaries_jsonl s ON c.id = s.customer_id ORDER BY s.salary DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Charlie")
	assertContains(t, stdout, "90000")
	assertContains(t, stdout, "rows)")
}

// TestMixedFormats_ThreeFormats tests JOIN across three different file formats
func TestMixedFormats_ThreeFormats(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers_csv.csv"),
		"-f", fixture("json/orders_json.json"),
		"-f", fixture("jsonl/salaries_jsonl.jsonl"),
		"-q", `SELECT c.name, SUM(o.total) as order_total, s.salary
			   FROM customers_csv c
			   JOIN orders_json o ON c.id = o.customer_id
			   JOIN salaries_jsonl s ON c.id = s.customer_id
			   GROUP BY c.name, s.salary
			   ORDER BY c.name`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "rows)")
}

// TestMixedFormats_Aggregation tests aggregation with mixed format JOIN
func TestMixedFormats_Aggregation(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers_csv.csv"),
		"-f", fixture("json/orders_json.json"),
		"-q", `SELECT c.department, COUNT(*) as order_count, SUM(o.total) as total_amount
			   FROM customers_csv c
			   JOIN orders_json o ON c.id = o.customer_id
			   GROUP BY c.department
			   ORDER BY c.department`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Engineering")
	assertContains(t, stdout, "Marketing")
	assertContains(t, stdout, "rows)")
}

// TestMixedFormats_JSON_JSONL tests JOIN between JSON and JSONL files
func TestMixedFormats_JSON_JSONL(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/orders_json.json"),
		"-f", fixture("jsonl/salaries_jsonl.jsonl"),
		"-q", "SELECT o.id, o.total, s.salary FROM orders_json o JOIN salaries_jsonl s ON o.customer_id = s.customer_id ORDER BY o.id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// TestMixedFormats_WithWhere tests mixed format JOIN with WHERE clause
func TestMixedFormats_WithWhere(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers_csv.csv"),
		"-f", fixture("json/orders_json.json"),
		"-q", "SELECT c.name, o.total FROM customers_csv c JOIN orders_json o ON c.id = o.customer_id WHERE o.total > 150 ORDER BY o.total")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "200")
	assertContains(t, stdout, "rows)")
}

// TestMixedFormats_WithLimit tests mixed format JOIN with LIMIT
func TestMixedFormats_WithLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers_csv.csv"),
		"-f", fixture("json/orders_json.json"),
		"-q", "SELECT c.name, o.total FROM customers_csv c JOIN orders_json o ON c.id = o.customer_id LIMIT 2")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "(2 rows)")
}

// TestMixedFormats_SameFormatMultipleFiles ensures same format files still work
func TestMixedFormats_SameFormatMultipleFiles(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers_csv.csv"),
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT c.name as customer, s.name as simple FROM customers_csv c, simple s WHERE c.id = s.id LIMIT 3")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// TestMixedFormats_SelectFromSingle tests that selecting from single table works in mixed context
func TestMixedFormats_SelectFromSingle(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers_csv.csv"),
		"-f", fixture("json/orders_json.json"),
		"-q", "SELECT * FROM customers_csv ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Diana")
	assertContains(t, stdout, "(4 rows)")
}

// TestMixedFormats_CrossJoin tests CROSS JOIN between different formats
func TestMixedFormats_CrossJoin(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers_csv.csv"),
		"-f", fixture("json/orders_json.json"),
		"-q", "SELECT c.name, o.total FROM customers_csv c CROSS JOIN orders_json o LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "(5 rows)")
}
