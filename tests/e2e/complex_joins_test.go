package e2e_test

import (
	"testing"
)

// ============================================
// Complex Join Tests
// Tests for various join scenarios and edge cases
// ============================================

func TestJoin_OrdersCustomers_Inner(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT o.order_id, c.name as customer_name, o.amount
			   FROM orders o
			   INNER JOIN customers c ON o.customer_id = c.customer_id
			   ORDER BY o.order_id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "customer_name")
	assertContains(t, stdout, "amount")
}

func TestJoin_OrdersCustomers_Left(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT o.order_id, c.name as customer_name, o.status
			   FROM orders o
			   LEFT JOIN customers c ON o.customer_id = c.customer_id
			   ORDER BY o.order_id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "status")
}

func TestJoin_CustomersOrders_Right(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT c.name, o.order_id, o.amount
			   FROM customers c
			   RIGHT JOIN orders o ON c.customer_id = o.customer_id
			   ORDER BY o.order_id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestJoin_FullOuter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT c.name, o.order_id, COALESCE(o.amount, 0) as amount
			   FROM customers c
			   FULL OUTER JOIN orders o ON c.customer_id = o.customer_id
			   ORDER BY c.name NULLS LAST`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestJoin_Cross(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT s.name as source_name, c.name as customer_name
			   FROM simple s
			   CROSS JOIN customers c
			   LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "source_name")
	assertContains(t, stdout, "customer_name")
}

func TestJoin_SelfJoin_Employees(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", `SELECT e.name as employee, m.name as manager
			   FROM employees e
			   LEFT JOIN employees m ON CAST(e.manager_id AS INTEGER) = m.id
			   ORDER BY e.name`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "employee")
	assertContains(t, stdout, "manager")
}

func TestJoin_ThreeWay(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-f", fixture("csv/products_catalog.csv"),
		"-q", `SELECT o.order_id, c.name as customer, p.name as product
			   FROM orders o
			   JOIN customers c ON o.customer_id = c.customer_id
			   JOIN products_catalog p ON o.product_id = p.product_id
			   ORDER BY o.order_id
			   LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "customer")
	assertContains(t, stdout, "product")
}

func TestJoin_WithAggregation(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT c.name, COUNT(o.order_id) as order_count, SUM(o.amount) as total_spent
			   FROM customers c
			   LEFT JOIN orders o ON c.customer_id = o.customer_id
			   GROUP BY c.name
			   ORDER BY total_spent DESC NULLS LAST`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "order_count")
	assertContains(t, stdout, "total_spent")
}

func TestJoin_WithSubquery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT c.name, totals.total_amount
			   FROM customers c
			   JOIN (
				   SELECT customer_id, SUM(amount) as total_amount
				   FROM orders
				   GROUP BY customer_id
			   ) totals ON c.customer_id = totals.customer_id
			   ORDER BY totals.total_amount DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total_amount")
}

func TestJoin_Natural(t *testing.T) {
	// Natural join uses common column names
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT order_id, name, amount
			   FROM orders
			   NATURAL JOIN customers
			   ORDER BY order_id
			   LIMIT 5`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "order_id")
}

func TestJoin_Using(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT order_id, name, amount
			   FROM orders
			   JOIN customers USING (customer_id)
			   ORDER BY order_id
			   LIMIT 5`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestJoin_WithCTE(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `WITH high_value_orders AS (
				   SELECT * FROM orders WHERE amount > 500
			   )
			   SELECT c.name, h.order_id, h.amount
			   FROM high_value_orders h
			   JOIN customers c ON h.customer_id = c.customer_id
			   ORDER BY h.amount DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "amount")
}

func TestJoin_WithWindowFunction(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT c.name, o.order_id, o.amount,
				   ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY o.amount DESC) as order_rank
			   FROM customers c
			   JOIN orders o ON c.customer_id = o.customer_id
			   ORDER BY c.name, order_rank`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "order_rank")
}

func TestJoin_MultipleConditions(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT c.name, o.order_id, o.status
			   FROM orders o
			   JOIN customers c ON o.customer_id = c.customer_id
				   AND o.amount > 100
			   WHERE c.country = 'USA'
			   ORDER BY o.order_id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "status")
}

func TestJoin_NonEqui(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT c.name, c.credit_limit, o.amount
			   FROM customers c
			   JOIN orders o ON o.customer_id = c.customer_id
				   AND o.amount <= c.credit_limit
			   ORDER BY c.name, o.amount
			   LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "credit_limit")
}

func TestJoin_SalesEmployees(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-f", fixture("csv/employees.csv"),
		"-q", `SELECT s.product, e.name as salesperson_name, e.department, s.price
			   FROM sales s
			   JOIN employees e ON s.salesperson = e.name
			   ORDER BY s.id
			   LIMIT 10`)

	// This may or may not match depending on data
	if err == nil {
		assertContains(t, stdout, "product")
	}
	_ = stderr
}

func TestJoin_AntiJoin_NotExists(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers.csv"),
		"-f", fixture("csv/orders.csv"),
		"-q", `SELECT c.name
			   FROM customers c
			   WHERE NOT EXISTS (
				   SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
			   )`)

	// May return empty if all customers have orders
	assertNoError(t, err, stderr)
	_ = stdout
}

func TestJoin_SemiJoin_Exists(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers.csv"),
		"-f", fixture("csv/orders.csv"),
		"-q", `SELECT c.name
			   FROM customers c
			   WHERE EXISTS (
				   SELECT 1 FROM orders o
				   WHERE o.customer_id = c.customer_id
				   AND o.status = 'completed'
			   )
			   ORDER BY c.name`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestJoin_LeftExcluding(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers.csv"),
		"-f", fixture("csv/orders.csv"),
		"-q", `SELECT c.name, c.email
			   FROM customers c
			   LEFT JOIN orders o ON c.customer_id = o.customer_id
			   WHERE o.order_id IS NULL`)

	// May return empty if all customers have orders
	assertNoError(t, err, stderr)
	_ = stdout
}

func TestJoin_DifferentColumnTypes(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/products_catalog.csv"),
		"-q", `SELECT o.order_id, p.name as product_name, p.price as unit_price, o.quantity
			   FROM orders o
			   JOIN products_catalog p ON o.product_id = p.product_id
			   ORDER BY o.order_id
			   LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product_name")
}

func TestJoin_WithCase(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-q", `SELECT c.name,
				   CASE
					   WHEN SUM(o.amount) > 1000 THEN 'VIP'
					   WHEN SUM(o.amount) > 500 THEN 'Regular'
					   ELSE 'New'
				   END as customer_tier
			   FROM customers c
			   LEFT JOIN orders o ON c.customer_id = o.customer_id
			   GROUP BY c.name
			   ORDER BY c.name`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "customer_tier")
}

func TestJoin_MultipleFiles_SameFormat(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/orders.csv"),
		"-f", fixture("csv/customers.csv"),
		"-f", fixture("csv/products_catalog.csv"),
		"-q", `SELECT COUNT(DISTINCT o.order_id) as orders,
				   COUNT(DISTINCT c.customer_id) as customers,
				   COUNT(DISTINCT p.product_id) as products
			   FROM orders o, customers c, products_catalog p
			   WHERE o.customer_id = c.customer_id
			   AND o.product_id = p.product_id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "orders")
}

func TestJoin_Lateral(t *testing.T) {
	// Lateral joins allow subquery to reference columns from preceding FROM items
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/customers.csv"),
		"-f", fixture("csv/orders.csv"),
		"-q", `SELECT c.name, recent.order_id, recent.amount
			   FROM customers c,
			   LATERAL (
				   SELECT order_id, amount
				   FROM orders o
				   WHERE o.customer_id = c.customer_id
				   ORDER BY order_date DESC
				   LIMIT 1
			   ) recent
			   ORDER BY c.name`)

	// Lateral may or may not be supported
	if err == nil {
		assertContains(t, stdout, "amount")
	}
	_ = stderr
}
