package e2e_test

import (
	"testing"
)

// ============================================
// Window Function Tests
// DuckDB supports window functions (ROW_NUMBER, RANK, etc.)
// ============================================

func TestWindow_RowNumber_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, price, ROW_NUMBER() OVER (ORDER BY price DESC) as rank FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
	assertContains(t, stdout, "rank")
}

func TestWindow_RowNumber_Partition(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, category, price, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as rank_in_category FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rank_in_category")
}

func TestWindow_Rank_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT name, salary, RANK() OVER (ORDER BY salary DESC) as salary_rank FROM employees")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "salary_rank")
}

func TestWindow_DenseRank(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT name, department, salary, DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank FROM employees")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "dept_rank")
}

func TestWindow_Lag(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT sale_date, price, LAG(price, 1) OVER (ORDER BY sale_date) as prev_price FROM sales ORDER BY sale_date LIMIT 10")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "prev_price")
}

func TestWindow_Lead(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT sale_date, price, LEAD(price, 1) OVER (ORDER BY sale_date) as next_price FROM sales ORDER BY sale_date LIMIT 10")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "next_price")
}

func TestWindow_SumOver(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, price, SUM(price) OVER (ORDER BY id) as running_total FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "running_total")
}

func TestWindow_AvgOver(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, category, price, AVG(price) OVER (PARTITION BY category) as avg_category_price FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "avg_category_price")
}

func TestWindow_CountOver(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT name, department, COUNT(*) OVER (PARTITION BY department) as dept_count FROM employees")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "dept_count")
}

func TestWindow_MinMaxOver(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, category, price, MIN(price) OVER (PARTITION BY category) as min_price, MAX(price) OVER (PARTITION BY category) as max_price FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "min_price")
	assertContains(t, stdout, "max_price")
}

func TestWindow_FirstValue(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, category, price, FIRST_VALUE(product) OVER (PARTITION BY category ORDER BY price DESC) as most_expensive FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "most_expensive")
}

func TestWindow_LastValue(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, category, price, LAST_VALUE(product) OVER (PARTITION BY category ORDER BY price DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as cheapest FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cheapest")
}

func TestWindow_NthValue(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT name, department, salary, NTH_VALUE(name, 2) OVER (PARTITION BY department ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as second_highest FROM employees")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "second_highest")
}

func TestWindow_Ntile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT name, salary, NTILE(4) OVER (ORDER BY salary DESC) as quartile FROM employees")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "quartile")
}

func TestWindow_PercentRank(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT name, salary, ROUND(PERCENT_RANK() OVER (ORDER BY salary), 2) as percentile FROM employees")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "percentile")
}

func TestWindow_CumeDist(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT name, salary, ROUND(CUME_DIST() OVER (ORDER BY salary), 2) as cume_dist FROM employees")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cume_dist")
}

func TestWindow_MultipleWindowFunctions(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, category, price,
			ROW_NUMBER() OVER (ORDER BY price DESC) as overall_rank,
			RANK() OVER (PARTITION BY category ORDER BY price DESC) as category_rank,
			SUM(price) OVER (PARTITION BY category) as category_total
			FROM sales LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "overall_rank")
	assertContains(t, stdout, "category_rank")
	assertContains(t, stdout, "category_total")
}

func TestWindow_WithWhereClause(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, category, price, ROW_NUMBER() OVER (ORDER BY price DESC) as rank FROM sales WHERE category = 'Electronics'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rank")
	assertContains(t, stdout, "Electronics")
}

func TestWindow_InSubquery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", `SELECT * FROM (
			SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
			FROM employees
		) ranked WHERE rank <= 2`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestWindow_FrameRows(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT id, price, AVG(price) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg FROM sales LIMIT 10")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "moving_avg")
}

func TestWindow_FrameRange(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT id, price, SUM(price) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_sum FROM sales LIMIT 10")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cumulative_sum")
}
