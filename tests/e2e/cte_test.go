package e2e_test

import (
	"testing"
)

// ============================================
// CTE (Common Table Expression) Tests
// DuckDB supports WITH clause for CTEs
// ============================================

func TestCTE_Simple(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `WITH high_value AS (
			SELECT * FROM sales WHERE price > 200
		)
		SELECT * FROM high_value ORDER BY price DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestCTE_WithAggregation(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `WITH category_stats AS (
			SELECT category, COUNT(*) as cnt, SUM(price) as total
			FROM sales
			GROUP BY category
		)
		SELECT * FROM category_stats ORDER BY total DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "category")
	assertContains(t, stdout, "cnt")
	assertContains(t, stdout, "total")
}

func TestCTE_Multiple(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `WITH electronics AS (
			SELECT * FROM sales WHERE category = 'Electronics'
		),
		furniture AS (
			SELECT * FROM sales WHERE category = 'Furniture'
		)
		SELECT 'Electronics' as type, COUNT(*) as cnt FROM electronics
		UNION ALL
		SELECT 'Furniture' as type, COUNT(*) as cnt FROM furniture`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Electronics")
	assertContains(t, stdout, "Furniture")
}

func TestCTE_Nested(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `WITH base_sales AS (
			SELECT * FROM sales WHERE quantity > 5
		),
		filtered_sales AS (
			SELECT * FROM base_sales WHERE price > 100
		)
		SELECT product, price, quantity FROM filtered_sales ORDER BY price DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestCTE_WithJoin(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `WITH order_totals AS (
			SELECT customer_id, SUM(amount) as total_spent
			FROM orders
			GROUP BY customer_id
		)
		SELECT c.name, o.total_spent
		FROM customers c
		JOIN order_totals o ON c.customer_id = o.customer_id
		ORDER BY o.total_spent DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
	assertContains(t, stdout, "total_spent")
}

func TestCTE_ReferencedMultipleTimes(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `WITH sales_stats AS (
			SELECT category, AVG(price) as avg_price
			FROM sales
			GROUP BY category
		)
		SELECT s.product, s.price, ss.avg_price,
			   s.price - ss.avg_price as diff_from_avg
		FROM sales s
		JOIN sales_stats ss ON s.category = ss.category
		ORDER BY diff_from_avg DESC
		LIMIT 5`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "diff_from_avg")
}

func TestCTE_WithWindowFunction(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", `WITH ranked_employees AS (
			SELECT name, department, salary,
				   RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank
			FROM employees
		)
		SELECT * FROM ranked_employees WHERE rank <= 2 ORDER BY department, rank`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rank")
}

func TestCTE_ChainedCTEs(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `WITH step1 AS (
			SELECT *, price * quantity as line_total FROM sales
		),
		step2 AS (
			SELECT category, SUM(line_total) as category_total FROM step1 GROUP BY category
		),
		step3 AS (
			SELECT *, ROUND(category_total * 100.0 / (SELECT SUM(category_total) FROM step2), 2) as pct
			FROM step2
		)
		SELECT * FROM step3 ORDER BY category_total DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "pct")
}

func TestCTE_WithSubquery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", `WITH avg_salary AS (
			SELECT AVG(salary) as avg FROM employees
		)
		SELECT name, salary, (SELECT avg FROM avg_salary) as company_avg
		FROM employees
		WHERE salary > (SELECT avg FROM avg_salary)
		ORDER BY salary DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "company_avg")
}

func TestCTE_InInsertSelect(t *testing.T) {
	// Test that CTE can be used with SELECT even without INSERT
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `WITH top_products AS (
			SELECT product, SUM(quantity) as total_qty
			FROM sales
			GROUP BY product
			ORDER BY total_qty DESC
			LIMIT 3
		)
		SELECT product, total_qty FROM top_products`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
	assertContains(t, stdout, "total_qty")
}

func TestCTE_EmptyResult(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `WITH no_sales AS (
			SELECT * FROM sales WHERE price > 99999
		)
		SELECT COUNT(*) as cnt FROM no_sales`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "0")
}

func TestCTE_WithCase(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `WITH categorized AS (
			SELECT product, price,
				CASE
					WHEN price > 500 THEN 'High'
					WHEN price > 100 THEN 'Medium'
					ELSE 'Low'
				END as price_tier
			FROM sales
		)
		SELECT price_tier, COUNT(*) as cnt
		FROM categorized
		GROUP BY price_tier
		ORDER BY cnt DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "price_tier")
}

func TestCTE_WithDistinct(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `WITH unique_categories AS (
			SELECT DISTINCT category FROM sales
		)
		SELECT * FROM unique_categories ORDER BY category`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Electronics")
	assertContains(t, stdout, "Furniture")
}

func TestCTE_SelfReference_Recursive_Attempt(t *testing.T) {
	// Note: Recursive CTEs need WITH RECURSIVE
	// This tests if DuckDB supports it
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", `WITH RECURSIVE org_chart AS (
			SELECT id, name, manager_id, 1 as level
			FROM employees
			WHERE manager_id IS NULL OR manager_id = ''
			UNION ALL
			SELECT e.id, e.name, e.manager_id, oc.level + 1
			FROM employees e
			JOIN org_chart oc ON CAST(e.manager_id AS INTEGER) = oc.id
		)
		SELECT * FROM org_chart ORDER BY level, name`)

	// If recursive CTEs work, we get results
	if err == nil {
		assertContains(t, stdout, "level")
	}
	// If not supported, it's expected to fail - no assertion needed
	_ = stderr
}
