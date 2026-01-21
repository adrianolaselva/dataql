package e2e_test

import (
	"testing"
)

// ============================================
// Math Function Tests
// DuckDB supports extensive math functions
// ============================================

func TestMath_Abs(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT id, ABS(price - 500) as distance_from_500 FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "distance_from_500")
}

func TestMath_Round(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/numbers_precision.csv"),
		"-q", "SELECT id, float_val, ROUND(CAST(float_val AS DOUBLE), 2) as rounded FROM numbers_precision")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rounded")
}

func TestMath_Ceil(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/numbers_precision.csv"),
		"-q", "SELECT id, float_val, CEIL(CAST(float_val AS DOUBLE)) as ceiling FROM numbers_precision")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "ceiling")
}

func TestMath_Floor(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/numbers_precision.csv"),
		"-q", "SELECT id, float_val, FLOOR(CAST(float_val AS DOUBLE)) as floored FROM numbers_precision")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "floored")
}

func TestMath_Sqrt(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/numbers_precision.csv"),
		"-q", "SELECT id, integer_val, SQRT(CAST(integer_val AS DOUBLE)) as square_root FROM numbers_precision WHERE CAST(integer_val AS INTEGER) > 0")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "square_root")
}

func TestMath_Power(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, POWER(CAST(id AS INTEGER), 2) as squared FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "squared")
}

func TestMath_Log(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/numbers_precision.csv"),
		"-q", "SELECT id, integer_val, LOG(CAST(integer_val AS DOUBLE)) as natural_log FROM numbers_precision WHERE CAST(integer_val AS INTEGER) > 0")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "natural_log")
}

func TestMath_Log10(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/numbers_precision.csv"),
		"-q", "SELECT id, integer_val, LOG10(CAST(integer_val AS DOUBLE)) as log10 FROM numbers_precision WHERE CAST(integer_val AS INTEGER) > 0")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "log10")
}

func TestMath_Exp(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, EXP(CAST(id AS DOUBLE)) as exponential FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "exponential")
}

func TestMath_Mod(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT id, quantity, MOD(CAST(quantity AS INTEGER), 10) as remainder FROM sales")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "remainder")
}

func TestMath_Sign(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/numbers_precision.csv"),
		"-q", "SELECT id, SIGN(COALESCE(TRY_CAST(negative AS DOUBLE), 0)) as sign_val FROM numbers_precision")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "sign_val")
}

func TestMath_Trunc(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/numbers_precision.csv"),
		"-q", "SELECT id, float_val, TRUNC(CAST(float_val AS DOUBLE)) as truncated FROM numbers_precision")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "truncated")
}

func TestMath_Sin(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, ROUND(SIN(CAST(id AS DOUBLE)), 4) as sine FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "sine")
}

func TestMath_Cos(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, ROUND(COS(CAST(id AS DOUBLE)), 4) as cosine FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cosine")
}

func TestMath_Tan(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, ROUND(TAN(CAST(id AS DOUBLE)), 4) as tangent FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "tangent")
}

func TestMath_Pi(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT PI() as pi_value FROM simple LIMIT 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3.14")
}

func TestMath_Radians(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT 180 as degrees, RADIANS(180) as radians FROM simple LIMIT 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "radians")
}

func TestMath_Degrees(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT PI() as radians, DEGREES(PI()) as degrees FROM simple LIMIT 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "degrees")
}

func TestMath_Random(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT RANDOM() as random_value FROM simple LIMIT 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "random_value")
}

func TestMath_Greatest(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT id, price, quantity, GREATEST(price, CAST(quantity AS DOUBLE) * 10) as max_val FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "max_val")
}

func TestMath_Least(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT id, price, quantity, LEAST(price, CAST(quantity AS DOUBLE) * 100) as min_val FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "min_val")
}

func TestMath_Coalesce(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, name, COALESCE(NULLIF(name, ''), 'Unknown') as safe_name FROM null_values")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Unknown")
}

func TestMath_Nullif(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/null_values.csv"),
		"-q", "SELECT id, value, NULLIF(CAST(value AS VARCHAR), '0') as non_zero FROM null_values")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "non_zero")
}

func TestMath_ArithmeticOperations(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT id, price, quantity, price * quantity as total, price / quantity as unit_price FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total")
	assertContains(t, stdout, "unit_price")
}

func TestMath_ComplexCalculation(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT id, product, price, quantity,
			   ROUND(price * quantity * 1.1, 2) as total_with_tax,
			   ROUND((price * quantity) / (SELECT SUM(price * quantity) FROM sales) * 100, 2) as pct_of_total
			   FROM sales
			   ORDER BY total_with_tax DESC
			   LIMIT 5`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "total_with_tax")
	assertContains(t, stdout, "pct_of_total")
}

func TestMath_Factorial(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, FACTORIAL(CAST(id AS INTEGER)) as fact FROM simple WHERE CAST(id AS INTEGER) <= 10")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "fact")
}

func TestMath_Cbrt(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT 27 as num, CBRT(27) as cube_root FROM simple LIMIT 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cube_root")
}

func TestMath_Ln(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, LN(CAST(id AS DOUBLE)) as natural_log FROM simple WHERE CAST(id AS INTEGER) > 0")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "natural_log")
}
