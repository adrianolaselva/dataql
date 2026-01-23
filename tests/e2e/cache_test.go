package e2e_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCache_BasicUsage(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()

	// Create a test CSV file
	csvContent := "id,name,value\n1,alice,100\n2,bob,200\n3,charlie,300"
	dataDir := t.TempDir()
	csvFile := filepath.Join(dataDir, "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// First run - should import and cache
	stdout, stderr, err := runDataQL(t, "run",
		"-f", csvFile,
		"-q", "SELECT COUNT(*) as cnt FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q", // quiet mode
	)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")

	// Verify cache files were created
	files, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("failed to read cache dir: %v", err)
	}

	hasDuckDB := false
	hasJSON := false
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".duckdb") {
			hasDuckDB = true
		}
		if strings.HasSuffix(f.Name(), ".json") {
			hasJSON = true
		}
	}

	if !hasDuckDB {
		t.Error("expected .duckdb cache file to be created")
	}
	if !hasJSON {
		t.Error("expected .json metadata file to be created")
	}
}

func TestCache_SecondRunUsesCachedData(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()

	// Create a test CSV file
	csvContent := "id,name,value\n1,alice,100\n2,bob,200\n3,charlie,300"
	dataDir := t.TempDir()
	csvFile := filepath.Join(dataDir, "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// First run - should import and cache
	stdout1, stderr1, err := runDataQL(t, "run",
		"-f", csvFile,
		"-q", "SELECT COUNT(*) as cnt FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
		"-v", // verbose to see cache messages
	)

	assertNoError(t, err, stderr1)
	assertContains(t, stdout1, "Starting data import")

	// Second run - should use cached data
	stdout2, stderr2, err := runDataQL(t, "run",
		"-f", csvFile,
		"-q", "SELECT COUNT(*) as cnt FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
		"-v", // verbose to see cache messages
	)

	assertNoError(t, err, stderr2)
	assertContains(t, stdout2, "Using cached data")
	assertNotContains(t, stdout2, "Starting data import")
}

func TestCache_InvalidatedOnFileChange(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()
	dataDir := t.TempDir()

	// Create a test CSV file
	csvContent := "id,name,value\n1,alice,100\n2,bob,200"
	csvFile := filepath.Join(dataDir, "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// First run - should import and cache
	_, stderr1, err := runDataQL(t, "run",
		"-f", csvFile,
		"-q", "SELECT COUNT(*) as cnt FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
	)
	assertNoError(t, err, stderr1)

	// Modify the file (add a row)
	csvContent2 := "id,name,value\n1,alice,100\n2,bob,200\n3,charlie,300"
	if err := os.WriteFile(csvFile, []byte(csvContent2), 0644); err != nil {
		t.Fatalf("failed to modify test file: %v", err)
	}

	// Second run - should NOT use cache (file modified)
	stdout2, stderr2, err := runDataQL(t, "run",
		"-f", csvFile,
		"-q", "SELECT COUNT(*) as cnt FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
	)

	assertNoError(t, err, stderr2)
	// Should show 3 rows now (from the modified file)
	assertContains(t, stdout2, "3")
}

func TestCache_ListCommand(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()

	// Create a test CSV file
	csvContent := "id,name,value\n1,alice,100\n2,bob,200"
	dataDir := t.TempDir()
	csvFile := filepath.Join(dataDir, "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Run with cache to create cache entry
	_, stderr1, err := runDataQL(t, "run",
		"-f", csvFile,
		"-q", "SELECT * FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
	)
	assertNoError(t, err, stderr1)

	// List cache
	stdout, stderr, err := runDataQL(t, "cache", "list",
		"-d", cacheDir,
	)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "test")
	assertContains(t, stdout, "Total: 1")
}

func TestCache_StatsCommand(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()

	// Create a test CSV file
	csvContent := "id,name,value\n1,alice,100\n2,bob,200"
	dataDir := t.TempDir()
	csvFile := filepath.Join(dataDir, "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Run with cache to create cache entry
	_, stderr1, err := runDataQL(t, "run",
		"-f", csvFile,
		"-q", "SELECT * FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
	)
	assertNoError(t, err, stderr1)

	// Get cache stats
	stdout, stderr, err := runDataQL(t, "cache", "stats",
		"-d", cacheDir,
	)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Cache directory:")
	assertContains(t, stdout, "Cached entries: 1")
}

func TestCache_ClearCommand(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()

	// Create a test CSV file
	csvContent := "id,name,value\n1,alice,100\n2,bob,200"
	dataDir := t.TempDir()
	csvFile := filepath.Join(dataDir, "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Run with cache to create cache entry
	_, stderr1, err := runDataQL(t, "run",
		"-f", csvFile,
		"-q", "SELECT * FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
	)
	assertNoError(t, err, stderr1)

	// Clear cache with --all flag
	stdout, stderr, err := runDataQL(t, "cache", "clear",
		"-d", cacheDir,
		"--all",
	)

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Cleared")

	// Verify cache is empty
	files, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("failed to read cache dir: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("cache directory should be empty after clear, found %d files", len(files))
	}
}

func TestCache_WithoutCacheFlag(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()

	// Create a test CSV file
	csvContent := "id,name,value\n1,alice,100\n2,bob,200"
	dataDir := t.TempDir()
	csvFile := filepath.Join(dataDir, "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Run WITHOUT cache flag
	_, stderr, err := runDataQL(t, "run",
		"-f", csvFile,
		"-q", "SELECT * FROM test",
		"--cache-dir", cacheDir, // specify dir but not --cache flag
		"-Q",
	)
	assertNoError(t, err, stderr)

	// Verify no cache files were created
	files, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("failed to read cache dir: %v", err)
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".duckdb") || strings.HasSuffix(f.Name(), ".json") {
			t.Error("no cache files should be created when --cache flag is not used")
		}
	}
}

func TestCache_MultipleFiles(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()
	dataDir := t.TempDir()

	// Create two test CSV files
	csv1Content := "id,name\n1,alice\n2,bob"
	csv2Content := "id,value\n1,100\n2,200"
	csvFile1 := filepath.Join(dataDir, "users.csv")
	csvFile2 := filepath.Join(dataDir, "values.csv")
	if err := os.WriteFile(csvFile1, []byte(csv1Content), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	if err := os.WriteFile(csvFile2, []byte(csv2Content), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// First run - should import and cache
	stdout1, stderr1, err := runDataQL(t, "run",
		"-f", csvFile1,
		"-f", csvFile2,
		"-q", "SELECT u.name, v.value FROM users u JOIN values v ON u.id = v.id",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
		"-v",
	)

	assertNoError(t, err, stderr1)
	assertContains(t, stdout1, "Starting data import")

	// Second run - should use cache
	stdout2, stderr2, err := runDataQL(t, "run",
		"-f", csvFile1,
		"-f", csvFile2,
		"-q", "SELECT u.name, v.value FROM users u JOIN values v ON u.id = v.id",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
		"-v",
	)

	assertNoError(t, err, stderr2)
	assertContains(t, stdout2, "Using cached data")
}
