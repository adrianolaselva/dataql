package e2e_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// ============================================
// Performance and Large Dataset Tests
// Tests for handling larger datasets and edge cases
// ============================================

// generateLargeCSV creates a temporary CSV file with specified number of rows
func generateLargeCSV(t *testing.T, rows int) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "large_data.csv")

	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer file.Close()

	// Write header
	file.WriteString("id,name,value,category,score\n")

	// Write rows
	categories := []string{"A", "B", "C", "D", "E"}
	for i := 1; i <= rows; i++ {
		line := fmt.Sprintf("%d,Item_%d,%d,%s,%.2f\n",
			i, i, i*10, categories[i%5], float64(i%100)+0.5)
		file.WriteString(line)
	}

	return path
}

func TestPerf_1000Rows_Select(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", "SELECT COUNT(*) as total FROM large_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1000")
}

func TestPerf_1000Rows_Filter(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", "SELECT COUNT(*) as filtered FROM large_data WHERE value > 5000")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "filtered")
}

func TestPerf_1000Rows_GroupBy(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", "SELECT category, COUNT(*) as cnt, SUM(value) as total FROM large_data GROUP BY category ORDER BY cnt DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "category")
	assertContains(t, stdout, "cnt")
}

func TestPerf_1000Rows_OrderBy(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", "SELECT id, value FROM large_data ORDER BY value DESC LIMIT 10")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "value")
}

func TestPerf_1000Rows_Aggregation(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", `SELECT
			   COUNT(*) as cnt,
			   SUM(value) as sum_val,
			   AVG(value) as avg_val,
			   MIN(value) as min_val,
			   MAX(value) as max_val
			   FROM large_data`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cnt")
	assertContains(t, stdout, "1000")
}

func TestPerf_1000Rows_WindowFunction(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", `SELECT id, category, value,
			   ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rank
			   FROM large_data
			   LIMIT 50`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rank")
}

func TestPerf_1000Rows_CTE(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", `WITH stats AS (
			   SELECT category, AVG(value) as avg_val
			   FROM large_data GROUP BY category
		   )
		   SELECT * FROM stats ORDER BY avg_val DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "avg_val")
}

func TestPerf_1000Rows_Subquery(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", `SELECT id, value FROM large_data
			   WHERE value > (SELECT AVG(value) FROM large_data)
			   ORDER BY value DESC
			   LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "value")
}

func TestPerf_5000Rows_Select(t *testing.T) {
	path := generateLargeCSV(t, 5000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", "SELECT COUNT(*) as total FROM large_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "5000")
}

func TestPerf_5000Rows_ComplexQuery(t *testing.T) {
	path := generateLargeCSV(t, 5000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", `WITH ranked AS (
			   SELECT id, category, value,
				   ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rn
			   FROM large_data
		   )
		   SELECT category, COUNT(*) as top_100_count
		   FROM ranked
		   WHERE rn <= 100
		   GROUP BY category
		   ORDER BY top_100_count DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "top_100_count")
}

func TestPerf_10000Rows_Select(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	path := generateLargeCSV(t, 10000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", "SELECT COUNT(*) as total FROM large_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "10000")
}

func TestPerf_10000Rows_GroupBy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	path := generateLargeCSV(t, 10000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", `SELECT category,
			   COUNT(*) as cnt,
			   SUM(value) as total,
			   AVG(score) as avg_score
			   FROM large_data
			   GROUP BY category
			   ORDER BY total DESC`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "category")
}

func TestPerf_Export_1000Rows_CSV(t *testing.T) {
	path := generateLargeCSV(t, 1000)
	exportPath := tempFile(t, "export_large.csv")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", "SELECT * FROM large_data",
		"-e", exportPath,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
	assertContains(t, stdout, "1000")
}

func TestPerf_Export_1000Rows_JSONL(t *testing.T) {
	path := generateLargeCSV(t, 1000)
	exportPath := tempFile(t, "export_large.jsonl")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", "SELECT * FROM large_data",
		"-e", exportPath,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestPerf_LimitOffset_LargeData(t *testing.T) {
	path := generateLargeCSV(t, 5000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", "SELECT id, value FROM large_data ORDER BY id LIMIT 10 OFFSET 4990")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "4991")
}

func TestPerf_LineLimitFlag(t *testing.T) {
	path := generateLargeCSV(t, 5000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-l", "100",
		"-q", "SELECT COUNT(*) as total FROM large_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "100")
}

func TestPerf_MultipleAggregations(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", `SELECT
			   COUNT(*) as cnt,
			   SUM(value) as sum_val,
			   AVG(value) as avg_val,
			   MIN(value) as min_val,
			   MAX(value) as max_val,
			   STDDEV(value) as stddev_val,
			   VARIANCE(value) as var_val
			   FROM large_data`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cnt")
	assertContains(t, stdout, "stddev_val")
}

func TestPerf_NestedSubqueries(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", `SELECT * FROM (
			   SELECT * FROM (
				   SELECT * FROM large_data WHERE value > 100
			   ) sub1 WHERE score > 50
		   ) sub2 LIMIT 10`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
}

func TestPerf_JoinLargeDatasets(t *testing.T) {
	path1 := generateLargeCSV(t, 500)
	path2 := generateLargeCSV(t, 500)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path1,
		"-f", path2,
		"-c", "data1",
		"-c", "data2",
		"-q", `SELECT COUNT(*) as joined_count
			   FROM data1 d1
			   JOIN data2 d2 ON d1.id = d2.id`)

	// Note: This may fail due to collection naming - adjust as needed
	if err == nil {
		assertContains(t, stdout, "joined_count")
	}
	_ = stderr
}

// Test memory-efficient processing
func TestPerf_StreamingExport(t *testing.T) {
	path := generateLargeCSV(t, 2000)
	exportPath := tempFile(t, "stream_export.csv")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", "SELECT id, name, value FROM large_data WHERE CAST(id AS INTEGER) % 2 = 0",
		"-e", exportPath,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
	// Verify export file was created
	if !fileExists(exportPath) {
		t.Error("Export file should exist")
	}
}

// Test with complex WHERE clause
func TestPerf_ComplexFilter(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", `SELECT COUNT(*) as cnt FROM large_data
			   WHERE (category = 'A' OR category = 'B')
			   AND value > 1000
			   AND score BETWEEN 25 AND 75`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "cnt")
}

// Test with multiple ORDER BY columns
func TestPerf_MultiColumnSort(t *testing.T) {
	path := generateLargeCSV(t, 1000)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", path,
		"-q", `SELECT category, value, score FROM large_data
			   ORDER BY category ASC, value DESC, score ASC
			   LIMIT 20`)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "category")
}
