package e2e_test

import (
	"testing"
)

// ============================================
// Date/Time Function Tests
// DuckDB supports various date/time functions
// ============================================

func TestDateTime_CurrentDate(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT CURRENT_DATE as today FROM simple LIMIT 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "today")
}

func TestDateTime_CurrentTimestamp(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT CURRENT_TIMESTAMP as now FROM simple LIMIT 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "now")
}

func TestDateTime_ExtractYear(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, event_date, EXTRACT(YEAR FROM CAST(event_date AS DATE)) as year FROM dates_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "year")
	assertContains(t, stdout, "2024")
}

func TestDateTime_ExtractMonth(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, EXTRACT(MONTH FROM CAST(event_date AS DATE)) as month FROM dates_data ORDER BY month")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "month")
}

func TestDateTime_ExtractDay(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, EXTRACT(DAY FROM CAST(event_date AS DATE)) as day FROM dates_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "day")
}

func TestDateTime_ExtractDOW(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, EXTRACT(DOW FROM CAST(event_date AS DATE)) as day_of_week FROM dates_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "day_of_week")
}

func TestDateTime_ExtractQuarter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, EXTRACT(QUARTER FROM CAST(event_date AS DATE)) as quarter FROM dates_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "quarter")
}

func TestDateTime_DateTrunc_Month(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT DATE_TRUNC('month', CAST(event_date AS DATE)) as month_start FROM dates_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "month_start")
}

func TestDateTime_DateTrunc_Year(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT DATE_TRUNC('year', CAST(event_date AS DATE)) as year_start FROM dates_data LIMIT 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "year_start")
}

func TestDateTime_DateAdd(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, event_date, CAST(event_date AS DATE) + INTERVAL '30 days' as plus_30_days FROM dates_data LIMIT 3")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "plus_30_days")
}

func TestDateTime_DateSub(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, event_date, CAST(event_date AS DATE) - INTERVAL '7 days' as minus_7_days FROM dates_data LIMIT 3")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "minus_7_days")
}

func TestDateTime_DateDiff(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, DATEDIFF('day', CAST('2024-01-01' AS DATE), CAST(event_date AS DATE)) as days_from_jan1 FROM dates_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "days_from_jan1")
}

func TestDateTime_Strftime(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, STRFTIME(CAST(event_date AS DATE), '%Y-%m-%d') as formatted FROM dates_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "formatted")
}

func TestDateTime_LastDay(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, LAST_DAY(CAST(event_date AS DATE)) as month_end FROM dates_data LIMIT 3")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "month_end")
}

func TestDateTime_MakeDate(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT MAKE_DATE(2024, 6, 15) as custom_date FROM simple LIMIT 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "custom_date")
}

func TestDateTime_Age(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, AGE(CAST(event_date AS DATE)) as age FROM dates_data LIMIT 3")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "age")
}

func TestDateTime_GroupByMonth(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT DATE_TRUNC('month', CAST(sale_date AS DATE)) as month,
			   COUNT(*) as sales_count,
			   SUM(price * quantity) as total_revenue
			   FROM sales
			   GROUP BY DATE_TRUNC('month', CAST(sale_date AS DATE))
			   ORDER BY month`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "month")
	assertContains(t, stdout, "sales_count")
}

func TestDateTime_FilterByDateRange(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", `SELECT * FROM sales
			   WHERE CAST(sale_date AS DATE) >= '2024-01-16'
			   AND CAST(sale_date AS DATE) <= '2024-01-18'
			   ORDER BY sale_date`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2024-01-16")
	assertContains(t, stdout, "2024-01-17")
	assertContains(t, stdout, "2024-01-18")
}

func TestDateTime_OrderByDate(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, event_date FROM dates_data ORDER BY CAST(event_date AS DATE) DESC LIMIT 3")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "event_name")
}

func TestDateTime_Epoch(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, EPOCH(CAST(event_date AS DATE)) as unix_timestamp FROM dates_data LIMIT 3")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "unix_timestamp")
}

func TestDateTime_TimeExtract(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", `SELECT event_name, event_time,
			   EXTRACT(HOUR FROM CAST(event_time AS TIME)) as hour,
			   EXTRACT(MINUTE FROM CAST(event_time AS TIME)) as minute
			   FROM dates_data LIMIT 3`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "hour")
	assertContains(t, stdout, "minute")
}

func TestDateTime_Weekday(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, DAYNAME(CAST(event_date AS DATE)) as weekday FROM dates_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "weekday")
}

func TestDateTime_MonthName(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT event_name, MONTHNAME(CAST(event_date AS DATE)) as month_name FROM dates_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "month_name")
}
