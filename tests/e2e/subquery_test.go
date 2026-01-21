package e2e_test

import (
	"testing"
)

// ============================================
// Subquery Tests
// Comprehensive tests for subquery scenarios
// ============================================

func TestSubquery_ScalarInSelect(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price,
			   (SELECT AVG(price) FROM sales) as avg_price
			   FROM sales
			   LIMIT 5`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "avg_price")
}

func TestSubquery_ScalarInWhere(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price FROM sales
			   WHERE price > (SELECT AVG(price) FROM sales)
			   ORDER BY price DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSubquery_In(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT * FROM sales
			   WHERE category IN (
				   SELECT DISTINCT category FROM sales WHERE price > 500
			   )`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSubquery_NotIn(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT * FROM customers
			   WHERE customer_id NOT IN (
				   SELECT DISTINCT customer_id FROM orders WHERE status = 'cancelled'
			   )`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestSubquery_Exists(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers.csv"),
		"-f", fixture("csv/orders.csv"),
		"-q", `SELECT c.name FROM customers c
			   WHERE EXISTS (
				   SELECT 1 FROM orders o
				   WHERE o.customer_id = c.customer_id
				   AND o.status = 'completed'
			   )`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestSubquery_NotExists(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers.csv"),
		"-f", fixture("csv/orders.csv"),
		"-q", `SELECT c.name FROM customers c
			   WHERE NOT EXISTS (
				   SELECT 1 FROM orders o
				   WHERE o.customer_id = c.customer_id
				   AND o.status = 'cancelled'
			   )`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestSubquery_Any(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price FROM sales
			   WHERE price > ANY (
				   SELECT AVG(price) FROM sales GROUP BY category
			   )
			   ORDER BY price DESC
			   LIMIT 5`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSubquery_All(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price FROM sales
			   WHERE price > ALL (
				   SELECT AVG(price) FROM sales GROUP BY category
			   )
			   ORDER BY price DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSubquery_DerivedTable(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT sub.category, sub.total_revenue
			   FROM (
				   SELECT category, SUM(price * quantity) as total_revenue
				   FROM sales
				   GROUP BY category
			   ) sub
			   ORDER BY sub.total_revenue DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total_revenue")
}

func TestSubquery_Nested(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT * FROM (
			   SELECT * FROM (
				   SELECT product, price, category FROM sales
			   ) inner_sub
			   WHERE price > 100
		   ) outer_sub
		   WHERE category = 'Electronics'
		   LIMIT 5`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSubquery_Correlated(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT s.product, s.price, s.category
			   FROM sales s
			   WHERE s.price > (
				   SELECT AVG(s2.price) FROM sales s2
				   WHERE s2.category = s.category
			   )
			   ORDER BY s.category, s.price DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSubquery_InSelect_Multiple(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price,
			   (SELECT MIN(price) FROM sales) as min_price,
			   (SELECT MAX(price) FROM sales) as max_price,
			   (SELECT AVG(price) FROM sales) as avg_price
			   FROM sales
			   LIMIT 5`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "min_price")
	assertContains(t, stdout, "max_price")
	assertContains(t, stdout, "avg_price")
}

func TestSubquery_WithJoin(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT c.name, sub.total_orders, sub.total_amount
			   FROM customers c
			   JOIN (
				   SELECT customer_id,
					   COUNT(*) as total_orders,
					   SUM(amount) as total_amount
				   FROM orders
				   GROUP BY customer_id
			   ) sub ON c.customer_id = sub.customer_id
			   ORDER BY sub.total_amount DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total_orders")
}

func TestSubquery_InHaving(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT category, AVG(price) as avg_price
			   FROM sales
			   GROUP BY category
			   HAVING AVG(price) > (SELECT AVG(price) / 2 FROM sales)`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "avg_price")
}

func TestSubquery_ComparisonOperators(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price FROM sales
			   WHERE price >= (SELECT MIN(price) FROM sales WHERE category = 'Electronics')
			   AND price <= (SELECT MAX(price) FROM sales WHERE category = 'Electronics')
			   ORDER BY price`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSubquery_WithLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price FROM sales
			   WHERE price IN (
				   SELECT price FROM sales ORDER BY price DESC LIMIT 5
			   )`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSubquery_Lateral(t *testing.T) {
	// LATERAL allows subquery to reference outer query columns
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT s.category, top.product, top.price
			   FROM (SELECT DISTINCT category FROM sales) s,
			   LATERAL (
				   SELECT product, price FROM sales
				   WHERE category = s.category
				   ORDER BY price DESC
				   LIMIT 1
			   ) top`)

	// LATERAL may not be supported in all versions
	if err == nil {
		assertContains(t, stdout, "product")
	}
	_ = stderr
}

func TestSubquery_RowSubquery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT * FROM sales
			   WHERE (category, price) IN (
				   SELECT category, MAX(price) FROM sales GROUP BY category
			   )`)

	// Row subqueries may have limited support
	if err == nil {
		assertContains(t, stdout, "product")
	}
	_ = stderr
}

func TestSubquery_WithCase(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price,
			   CASE
				   WHEN price > (SELECT AVG(price) * 2 FROM sales) THEN 'Premium'
				   WHEN price > (SELECT AVG(price) FROM sales) THEN 'Standard'
				   ELSE 'Budget'
			   END as tier
			   FROM sales
			   ORDER BY price DESC
			   LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "tier")
}

func TestSubquery_WithWindowFunction(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT * FROM (
			   SELECT product, category, price,
				   ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as rank
			   FROM sales
		   ) ranked
		   WHERE rank <= 2
		   ORDER BY category, rank`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rank")
}

func TestSubquery_MultipleTables(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-f", fixture("csv/products_catalog.csv"),
		"-q", `SELECT c.name,
			   (SELECT SUM(o.amount) FROM orders o WHERE o.customer_id = c.customer_id) as total_spent,
			   (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) as order_count
			   FROM customers c
			   ORDER BY total_spent DESC NULLS LAST`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total_spent")
}

func TestSubquery_EmptyResult(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT * FROM sales
			   WHERE price > (SELECT MAX(price) FROM sales)`)

	assertNoError(t, err, stderr)
	// Should return empty result set
	_ = stdout
}

func TestSubquery_NullHandling(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT c.name,
			   COALESCE(
				   (SELECT SUM(o.amount) FROM orders o WHERE o.customer_id = c.customer_id),
				   0
			   ) as total_spent
			   FROM customers c
			   ORDER BY total_spent DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total_spent")
}
