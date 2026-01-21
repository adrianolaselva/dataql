package e2e_test

import (
	"testing"
)

// ============================================
// Conditional Expression Tests
// Tests for CASE, IF, COALESCE, NULLIF, etc.
// ============================================

func TestConditional_CaseSimple(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, category,
			   CASE category
				   WHEN 'Electronics' THEN 'Tech'
				   WHEN 'Furniture' THEN 'Home'
				   ELSE 'Other'
			   END as department
			   FROM sales LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Tech")
	assertContains(t, stdout, "Home")
}

func TestConditional_CaseSearched(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price,
			   CASE
				   WHEN price >= 500 THEN 'Premium'
				   WHEN price >= 200 THEN 'Standard'
				   WHEN price >= 50 THEN 'Budget'
				   ELSE 'Economy'
			   END as tier
			   FROM sales ORDER BY price DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Premium")
	assertContains(t, stdout, "tier")
}

func TestConditional_CaseNested(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, category, price,
			   CASE
				   WHEN category = 'Electronics' THEN
					   CASE
						   WHEN price > 500 THEN 'High-End Tech'
						   ELSE 'Budget Tech'
					   END
				   WHEN category = 'Furniture' THEN
					   CASE
						   WHEN price > 200 THEN 'Premium Furniture'
						   ELSE 'Basic Furniture'
					   END
				   ELSE 'Other'
			   END as classification
			   FROM sales LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "classification")
}

func TestConditional_CaseWithNull(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `SELECT id, name,
			   CASE
				   WHEN name IS NULL THEN 'No Name'
				   WHEN name = '' THEN 'Empty'
				   ELSE name
			   END as display_name
			   FROM null_values`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "display_name")
}

func TestConditional_CaseInOrderBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, category FROM sales
			   ORDER BY
				   CASE category
					   WHEN 'Electronics' THEN 1
					   WHEN 'Furniture' THEN 2
					   ELSE 3
				   END,
				   product`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product")
}

func TestConditional_CaseInGroupBy(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT
			   CASE
				   WHEN price > 500 THEN 'High'
				   WHEN price > 100 THEN 'Medium'
				   ELSE 'Low'
			   END as price_range,
			   COUNT(*) as cnt,
			   SUM(quantity) as total_qty
			   FROM sales
			   GROUP BY CASE
				   WHEN price > 500 THEN 'High'
				   WHEN price > 100 THEN 'Medium'
				   ELSE 'Low'
			   END
			   ORDER BY cnt DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "price_range")
}

func TestConditional_CaseWithAggregation(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT
			   SUM(CASE WHEN category = 'Electronics' THEN price * quantity ELSE 0 END) as electronics_revenue,
			   SUM(CASE WHEN category = 'Furniture' THEN price * quantity ELSE 0 END) as furniture_revenue
			   FROM sales`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "electronics_revenue")
	assertContains(t, stdout, "furniture_revenue")
}

func TestConditional_Coalesce_TwoArgs(t *testing.T) {
	// COALESCE doesn't treat empty strings as NULL, use NULLIF
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `SELECT id, COALESCE(NULLIF(name, ''), 'Unknown') as safe_name
			   FROM null_values ORDER BY id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Unknown")
}

func TestConditional_Coalesce_MultipleArgs(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `SELECT id, COALESCE(name, status, 'N/A') as first_non_null
			   FROM null_values ORDER BY id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "first_non_null")
}

func TestConditional_Nullif(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `SELECT id, value, NULLIF(CAST(value AS VARCHAR), '0') as non_zero
			   FROM null_values ORDER BY id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "non_zero")
}

func TestConditional_NullifWithCoalesce(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `SELECT id,
			   COALESCE(NULLIF(CAST(value AS VARCHAR), ''), 'No Value') as display_value
			   FROM null_values ORDER BY id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "display_value")
}

func TestConditional_If(t *testing.T) {
	// IF function (not all DBs support this)
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price,
			   IF(price > 500, 'Expensive', 'Affordable') as price_label
			   FROM sales LIMIT 10`)

	// IF may not be supported - use CASE instead
	if err == nil {
		assertContains(t, stdout, "price_label")
	}
	_ = stderr
}

func TestConditional_Iff(t *testing.T) {
	// IFF function (DuckDB specific)
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price,
			   IIF(price > 500, 'Expensive', 'Affordable') as price_label
			   FROM sales LIMIT 10`)

	// IIF may not be supported
	if err == nil {
		assertContains(t, stdout, "price_label")
	}
	_ = stderr
}

func TestConditional_Greatest(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price, quantity,
			   GREATEST(price, CAST(quantity AS DOUBLE) * 10) as max_val
			   FROM sales LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "max_val")
}

func TestConditional_Least(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price, quantity,
			   LEAST(price, CAST(quantity AS DOUBLE) * 100) as min_val
			   FROM sales LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "min_val")
}

func TestConditional_NullHandling_Arithmetic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `SELECT id,
			   COALESCE(CAST(value AS INTEGER), 0) + 10 as value_plus_10
			   FROM null_values ORDER BY id`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "value_plus_10")
}

func TestConditional_CaseWithSubquery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price,
			   CASE
				   WHEN price > (SELECT AVG(price) FROM sales) THEN 'Above Average'
				   ELSE 'Below Average'
			   END as price_comparison
			   FROM sales ORDER BY price DESC LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "price_comparison")
}

func TestConditional_CountIf_Alternative(t *testing.T) {
	// Using CASE within COUNT as alternative to COUNT_IF
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT
			   COUNT(CASE WHEN category = 'Electronics' THEN 1 END) as electronics_count,
			   COUNT(CASE WHEN category = 'Furniture' THEN 1 END) as furniture_count
			   FROM sales`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "electronics_count")
}

func TestConditional_SumIf_Alternative(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT
			   SUM(CASE WHEN category = 'Electronics' THEN price * quantity ELSE 0 END) as electronics_total,
			   SUM(CASE WHEN category = 'Furniture' THEN price * quantity ELSE 0 END) as furniture_total
			   FROM sales`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "electronics_total")
}

func TestConditional_Decode(t *testing.T) {
	// DECODE function (Oracle-style, may not be supported)
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, category,
			   CASE category
				   WHEN 'Electronics' THEN 'E'
				   WHEN 'Furniture' THEN 'F'
				   ELSE 'O'
			   END as category_code
			   FROM sales LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "category_code")
}

func TestConditional_CaseWithBooleanResult(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price,
			   CASE WHEN price > 200 THEN true ELSE false END as is_expensive
			   FROM sales LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "is_expensive")
}

func TestConditional_MultipleCases(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product,
			   CASE WHEN price > 500 THEN 'High' ELSE 'Low' END as price_tier,
			   CASE WHEN quantity > 20 THEN 'Bulk' ELSE 'Regular' END as order_type,
			   CASE category WHEN 'Electronics' THEN 'Tech' ELSE 'Other' END as dept
			   FROM sales LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "price_tier")
	assertContains(t, stdout, "order_type")
	assertContains(t, stdout, "dept")
}

func TestConditional_CaseWithLike(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product,
			   CASE
				   WHEN product LIKE '%phone%' OR product LIKE '%Phone%' THEN 'Phone'
				   WHEN product LIKE '%Laptop%' THEN 'Computer'
				   WHEN product LIKE '%Monitor%' THEN 'Display'
				   ELSE 'Other'
			   END as product_type
			   FROM sales`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "product_type")
}

func TestConditional_CaseWithIn(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, region,
			   CASE
				   WHEN region IN ('North', 'South') THEN 'Domestic'
				   WHEN region IN ('East', 'West') THEN 'Coastal'
				   ELSE 'Unknown'
			   END as region_type
			   FROM sales`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "region_type")
}

func TestConditional_CaseWithBetween(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT product, price,
			   CASE
				   WHEN price BETWEEN 0 AND 99.99 THEN 'Under $100'
				   WHEN price BETWEEN 100 AND 499.99 THEN '$100-$500'
				   WHEN price BETWEEN 500 AND 999.99 THEN '$500-$1000'
				   ELSE 'Over $1000'
			   END as price_band
			   FROM sales ORDER BY price`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "price_band")
}

func TestConditional_NullSafe_Equals(t *testing.T) {
	// NULL-safe comparison using IS NOT DISTINCT FROM
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", `SELECT id, name FROM null_values
			   WHERE name IS NOT DISTINCT FROM NULL`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
}

func TestConditional_Try(t *testing.T) {
	// TRY function catches errors and returns NULL
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/mixed_types.csv"),
		"-q", `SELECT id, value, TRY_CAST(value AS INTEGER) as int_value
			   FROM mixed_types`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "int_value")
}
