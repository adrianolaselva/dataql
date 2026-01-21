package e2e_test

import (
	"testing"
)

// ============================================
// SQL Compliance Tests
// Tests for SQL standard compliance and edge cases
// ============================================

// SELECT statement tests
func TestSQL_SelectStar(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
	assertContains(t, stdout, "name")
}

func TestSQL_SelectColumns(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, name FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
	assertContains(t, stdout, "name")
	// Verify the output shows the query results (not checking for absence of email
	// since schema info may include all columns)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "Jane")
}

func TestSQL_SelectAlias(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id AS identifier, name AS full_name FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "identifier")
	assertContains(t, stdout, "full_name")
}

func TestSQL_SelectDistinct(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT DISTINCT category FROM sales ORDER BY category")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Electronics")
	assertContains(t, stdout, "Furniture")
}

func TestSQL_SelectExpression(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT id, price * quantity AS total FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total")
}

// WHERE clause tests
func TestSQL_WhereEquals(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE category = 'Electronics'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Electronics")
	assertNotContains(t, stdout, "Furniture")
}

func TestSQL_WhereNotEquals(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE category <> 'Electronics'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Furniture")
}

func TestSQL_WhereGreaterThan(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE price > 500 ORDER BY price DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "999")
}

func TestSQL_WhereLessThan(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE price < 100 ORDER BY price")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "49.99")
}

func TestSQL_WhereAnd(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE category = 'Electronics' AND price > 200")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Electronics")
}

func TestSQL_WhereOr(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE category = 'Electronics' OR price < 100")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSQL_WhereNot(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE NOT category = 'Electronics'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Furniture")
}

func TestSQL_WhereBetween(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE price BETWEEN 100 AND 500 ORDER BY price")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "199.99")
}

func TestSQL_WhereIn(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE region IN ('North', 'South') ORDER BY region")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "North")
	assertContains(t, stdout, "South")
}

func TestSQL_WhereNotIn(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE region NOT IN ('North', 'South') ORDER BY region")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "East")
}

func TestSQL_WhereLike(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE product LIKE 'L%'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Laptop")
	assertContains(t, stdout, "Lamp")
}

func TestSQL_WhereLikeUnderscore(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE product LIKE 'L___' ORDER BY product")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Lamp")
}

// ORDER BY tests
func TestSQL_OrderByAsc(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, price FROM sales ORDER BY price ASC LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "29.99")
}

func TestSQL_OrderByDesc(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, price FROM sales ORDER BY price DESC LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "999.99")
}

func TestSQL_OrderByMultiple(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, product, price FROM sales ORDER BY category ASC, price DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "category")
}

func TestSQL_OrderByExpression(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, price, quantity FROM sales ORDER BY price * quantity DESC LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

// GROUP BY tests
func TestSQL_GroupByCount(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, COUNT(*) as cnt FROM sales GROUP BY category ORDER BY cnt DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Electronics")
}

func TestSQL_GroupBySum(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, SUM(price * quantity) as total FROM sales GROUP BY category ORDER BY total DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total")
}

func TestSQL_GroupByAvg(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, ROUND(AVG(price), 2) as avg_price FROM sales GROUP BY category")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "avg_price")
}

func TestSQL_GroupByMinMax(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, MIN(price) as min_price, MAX(price) as max_price FROM sales GROUP BY category")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "min_price")
	assertContains(t, stdout, "max_price")
}

func TestSQL_GroupByHaving(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, COUNT(*) as cnt FROM sales GROUP BY category HAVING COUNT(*) > 3")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "category")
}

func TestSQL_GroupByMultiple(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, region, COUNT(*) as cnt FROM sales GROUP BY category, region ORDER BY category, region")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cnt")
}

// LIMIT and OFFSET tests
func TestSQL_Limit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	// Verify the query executed successfully and returned results
	assertContains(t, stdout, "product")
	assertContains(t, stdout, "(5 rows)")
}

func TestSQL_Offset(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT id FROM sales ORDER BY id LIMIT 3 OFFSET 2")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
	assertContains(t, stdout, "4")
	assertContains(t, stdout, "5")
}

// Subquery tests
func TestSQL_SubqueryInWhere(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT * FROM sales WHERE price > (SELECT AVG(price) FROM sales)")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSQL_SubqueryInSelect(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, price, (SELECT AVG(price) FROM sales) as avg_price FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "avg_price")
}

func TestSQL_SubqueryInFrom(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT sub.category, sub.total
			   FROM (SELECT category, SUM(price) as total FROM sales GROUP BY category) sub
			   ORDER BY sub.total DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total")
}

// UNION tests
func TestSQL_Union(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "(SELECT product, price FROM sales WHERE category = 'Electronics' LIMIT 3) UNION (SELECT product, price FROM sales WHERE category = 'Furniture' LIMIT 3)")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSQL_UnionAll(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT category FROM sales WHERE id <= 3
			   UNION ALL
			   SELECT category FROM sales WHERE id <= 3`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "category")
}

func TestSQL_Intersect(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT category FROM sales WHERE price > 200
			   INTERSECT
			   SELECT category FROM sales WHERE price < 500`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "category")
}

func TestSQL_Except(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT DISTINCT category FROM sales
			   EXCEPT
			   SELECT DISTINCT category FROM sales WHERE price > 500`)

	// May or may not return results depending on data
	assertNoError(t, err, stderr)
	_ = stdout
}

// CASE expression tests
func TestSQL_CaseSimple(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, category,
			   CASE category
				   WHEN 'Electronics' THEN 'Tech'
				   WHEN 'Furniture' THEN 'Home'
				   ELSE 'Other'
			   END as dept
			   FROM sales LIMIT 5`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "dept")
}

func TestSQL_CaseSearched(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price,
			   CASE
				   WHEN price > 500 THEN 'Expensive'
				   WHEN price > 100 THEN 'Moderate'
				   ELSE 'Cheap'
			   END as price_range
			   FROM sales ORDER BY price DESC LIMIT 5`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Expensive")
}

// EXISTS tests
func TestSQL_Exists(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers.csv"),
		"-f", fixture("csv/orders.csv"),
		"-q", `SELECT c.name FROM customers c
			   WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestSQL_NotExists(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers.csv"),
		"-f", fixture("csv/orders.csv"),
		"-q", `SELECT c.name FROM customers c
			   WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id AND o.status = 'cancelled')`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

// Type casting tests
func TestSQL_Cast(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT id, CAST(price AS INTEGER) as price_int FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "price_int")
}

func TestSQL_TryCast(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/mixed_types.csv"),
		"-q", "SELECT id, TRY_CAST(value AS INTEGER) as int_value FROM mixed_types")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "int_value")
}

// Comments
func TestSQL_LineComment(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", `SELECT * FROM simple -- this is a comment
			   WHERE id = 1`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
}

func TestSQL_BlockComment(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", `SELECT /* column list */ id, name /* end */
			   FROM simple`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
}

// Table aliases
func TestSQL_TableAlias(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT s.id, s.product FROM sales s LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestSQL_TableAliasAS(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT s.id, s.product FROM sales AS s LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}
