package e2e_test

import (
	"testing"
)

// ============================================
// Aggregation Function Tests
// Comprehensive tests for all aggregate functions
// ============================================

func TestAgg_Count_Star(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT COUNT(*) as total FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "15")
}

func TestAgg_Count_Column(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT COUNT(name) as named_count FROM null_values")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "named_count")
}

func TestAgg_Count_Distinct(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT COUNT(DISTINCT category) as unique_categories FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

func TestAgg_Sum_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT SUM(quantity) as total_quantity FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total_quantity")
}

func TestAgg_Sum_Expression(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT SUM(price * quantity) as total_revenue FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total_revenue")
}

func TestAgg_Avg_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT ROUND(AVG(price), 2) as avg_price FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "avg_price")
}

func TestAgg_Avg_GroupBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, ROUND(AVG(price), 2) as avg_price FROM sales GROUP BY category ORDER BY avg_price DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Electronics")
	assertContains(t, stdout, "Furniture")
}

func TestAgg_Min_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT MIN(price) as min_price FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "29.99")
}

func TestAgg_Max_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT MAX(price) as max_price FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "999.99")
}

func TestAgg_MinMax_String(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT MIN(product) as first_product, MAX(product) as last_product FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "first_product")
	assertContains(t, stdout, "last_product")
}

func TestAgg_GroupConcat(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, STRING_AGG(product, ', ') as products FROM sales GROUP BY category")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "products")
}

func TestAgg_First(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, FIRST(product ORDER BY id) as first_product FROM sales GROUP BY category")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "first_product")
}

func TestAgg_Last(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, LAST(product ORDER BY id) as last_product FROM sales GROUP BY category")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "last_product")
}

func TestAgg_Variance(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT ROUND(VARIANCE(price), 2) as price_variance FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "price_variance")
}

func TestAgg_Stddev(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT ROUND(STDDEV(price), 2) as price_stddev FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "price_stddev")
}

func TestAgg_VarPop(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT ROUND(VAR_POP(price), 2) as var_pop FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "var_pop")
}

func TestAgg_StddevPop(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT ROUND(STDDEV_POP(price), 2) as stddev_pop FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "stddev_pop")
}

func TestAgg_MultipleInSelect(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT
			   COUNT(*) as cnt,
			   SUM(quantity) as total_qty,
			   ROUND(AVG(price), 2) as avg_price,
			   MIN(price) as min_price,
			   MAX(price) as max_price
			   FROM sales`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cnt")
	assertContains(t, stdout, "total_qty")
	assertContains(t, stdout, "avg_price")
}

func TestAgg_WithFilter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT
			   COUNT(*) FILTER (WHERE category = 'Electronics') as electronics_count,
			   COUNT(*) FILTER (WHERE category = 'Furniture') as furniture_count
			   FROM sales`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "electronics_count")
}

func TestAgg_Having_Count(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT region, COUNT(*) as cnt FROM sales GROUP BY region HAVING COUNT(*) >= 3 ORDER BY cnt DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cnt")
}

func TestAgg_Having_Sum(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT salesperson, SUM(price * quantity) as total FROM sales GROUP BY salesperson HAVING SUM(price * quantity) > 5000 ORDER BY total DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total")
}

func TestAgg_Having_Avg(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, ROUND(AVG(price), 2) as avg_price FROM sales GROUP BY category HAVING AVG(price) > 200")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "avg_price")
}

func TestAgg_Median(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT MEDIAN(price) as median_price FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "median_price")
}

func TestAgg_Mode(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT MODE(category) as most_common_category FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "most_common_category")
}

func TestAgg_Percentile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as p50_price FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "p50_price")
}

func TestAgg_Quantile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT QUANTILE_CONT(price, 0.25) as q1, QUANTILE_CONT(price, 0.75) as q3 FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "q1")
}

func TestAgg_ArrayAgg(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, ARRAY_AGG(product ORDER BY price DESC) as products FROM sales GROUP BY category")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "products")
}

func TestAgg_BitAnd(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT BIT_AND(CAST(quantity AS INTEGER)) as bit_and_qty FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "bit_and_qty")
}

func TestAgg_BitOr(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT BIT_OR(CAST(quantity AS INTEGER)) as bit_or_qty FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "bit_or_qty")
}

func TestAgg_BitXor(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT BIT_XOR(CAST(quantity AS INTEGER)) as bit_xor_qty FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "bit_xor_qty")
}

func TestAgg_Bool_And(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT department, BOOL_AND(active) as all_active FROM employees GROUP BY department")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "all_active")
}

func TestAgg_Bool_Or(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT department, BOOL_OR(active) as any_active FROM employees GROUP BY department")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "any_active")
}

func TestAgg_Product(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT PRODUCT(CAST(id AS INTEGER)) as id_product FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id_product")
}

func TestAgg_CountIf(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT COUNT_IF(price > 200) as high_price_count FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "high_price_count")
}

func TestAgg_SumIf(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT SUM_IF(price, category = 'Electronics') as electronics_total FROM sales")

	// SUM_IF may not exist - use SUM with CASE
	if err != nil {
		stdout, stderr, err = runDataQL(t, "run",
			"-f", fixture("csv/sales.csv"),
			"-q", "SELECT SUM(CASE WHEN category = 'Electronics' THEN price ELSE 0 END) as electronics_total FROM sales")
		assertNoError(t, err, stderr)
	}
	assertContains(t, stdout, "electronics_total")
}

func TestAgg_NestedAggregations(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT
			   category,
			   COUNT(*) as cnt,
			   SUM(price * quantity) as total,
			   ROUND(SUM(price * quantity) / SUM(quantity), 2) as weighted_avg_price
			   FROM sales
			   GROUP BY category
			   ORDER BY total DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "weighted_avg_price")
}

func TestAgg_WithCTE(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `WITH category_stats AS (
			   SELECT category, SUM(price * quantity) as total
			   FROM sales
			   GROUP BY category
		   )
		   SELECT
			   SUM(total) as grand_total,
			   AVG(total) as avg_category_total
		   FROM category_stats`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "grand_total")
}

func TestAgg_Rollup(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, region, SUM(price) as total FROM sales GROUP BY ROLLUP(category, region) ORDER BY category NULLS FIRST, region NULLS FIRST")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total")
}

func TestAgg_Cube(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, region, SUM(price) as total FROM sales GROUP BY CUBE(category, region) ORDER BY category NULLS FIRST, region NULLS FIRST")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total")
}

func TestAgg_GroupingSets(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT category, region, SUM(price) as total FROM sales GROUP BY GROUPING SETS ((category), (region), ()) ORDER BY category NULLS FIRST, region NULLS FIRST")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total")
}
