package e2e

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCache_BasicUsage(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()

	// Create a test CSV file
	csvContent := "id,name,value\n1,alice,100\n2,bob,200\n3,charlie,300"
	csvFile := filepath.Join(t.TempDir(), "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// First run - should import and cache
	cmd := exec.Command("./dataql", "run",
		"-f", csvFile,
		"-q", "SELECT COUNT(*) as cnt FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q", // quiet mode
	)
	cmd.Dir = findProjectRoot(t)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("first run failed: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}

	output := stdout.String()
	if !strings.Contains(output, "3") {
		t.Errorf("expected count of 3, got: %s", output)
	}

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
	csvFile := filepath.Join(t.TempDir(), "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	projectRoot := findProjectRoot(t)

	// First run - should import and cache
	cmd1 := exec.Command("./dataql", "run",
		"-f", csvFile,
		"-q", "SELECT COUNT(*) as cnt FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
		"-v", // verbose to see cache messages
	)
	cmd1.Dir = projectRoot
	var stdout1, stderr1 bytes.Buffer
	cmd1.Stdout = &stdout1
	cmd1.Stderr = &stderr1

	if err := cmd1.Run(); err != nil {
		t.Fatalf("first run failed: %v\nstdout: %s\nstderr: %s", err, stdout1.String(), stderr1.String())
	}

	firstOutput := stdout1.String()
	if !strings.Contains(firstOutput, "Starting data import") {
		t.Error("first run should show import message")
	}

	// Second run - should use cached data
	cmd2 := exec.Command("./dataql", "run",
		"-f", csvFile,
		"-q", "SELECT COUNT(*) as cnt FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
		"-v", // verbose to see cache messages
	)
	cmd2.Dir = projectRoot
	var stdout2, stderr2 bytes.Buffer
	cmd2.Stdout = &stdout2
	cmd2.Stderr = &stderr2

	if err := cmd2.Run(); err != nil {
		t.Fatalf("second run failed: %v\nstdout: %s\nstderr: %s", err, stdout2.String(), stderr2.String())
	}

	secondOutput := stdout2.String()
	if !strings.Contains(secondOutput, "Using cached data") {
		t.Error("second run should show cache hit message")
	}
	if strings.Contains(secondOutput, "Starting data import") {
		t.Error("second run should not show import message")
	}
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

	projectRoot := findProjectRoot(t)

	// First run - should import and cache
	cmd1 := exec.Command("./dataql", "run",
		"-f", csvFile,
		"-q", "SELECT COUNT(*) as cnt FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
	)
	cmd1.Dir = projectRoot
	if err := cmd1.Run(); err != nil {
		t.Fatalf("first run failed: %v", err)
	}

	// Modify the file (add a row)
	csvContent2 := "id,name,value\n1,alice,100\n2,bob,200\n3,charlie,300"
	if err := os.WriteFile(csvFile, []byte(csvContent2), 0644); err != nil {
		t.Fatalf("failed to modify test file: %v", err)
	}

	// Second run - should NOT use cache (file modified)
	cmd2 := exec.Command("./dataql", "run",
		"-f", csvFile,
		"-q", "SELECT COUNT(*) as cnt FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
	)
	cmd2.Dir = projectRoot
	var stdout2 bytes.Buffer
	cmd2.Stdout = &stdout2

	if err := cmd2.Run(); err != nil {
		t.Fatalf("second run failed: %v", err)
	}

	output := stdout2.String()
	// Should show 3 rows now (from the modified file)
	if !strings.Contains(output, "3") {
		t.Errorf("expected count of 3 after file modification, got: %s", output)
	}
}

func TestCache_ListCommand(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()

	// Create a test CSV file
	csvContent := "id,name,value\n1,alice,100\n2,bob,200"
	csvFile := filepath.Join(t.TempDir(), "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	projectRoot := findProjectRoot(t)

	// Run with cache to create cache entry
	cmd1 := exec.Command("./dataql", "run",
		"-f", csvFile,
		"-q", "SELECT * FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
	)
	cmd1.Dir = projectRoot
	if err := cmd1.Run(); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	// List cache
	cmd2 := exec.Command("./dataql", "cache", "list",
		"-d", cacheDir,
	)
	cmd2.Dir = projectRoot
	var stdout bytes.Buffer
	cmd2.Stdout = &stdout

	if err := cmd2.Run(); err != nil {
		t.Fatalf("cache list failed: %v", err)
	}

	output := stdout.String()
	if !strings.Contains(output, "test") {
		t.Errorf("cache list should show table name, got: %s", output)
	}
	if !strings.Contains(output, "Total: 1") {
		t.Errorf("cache list should show 1 entry, got: %s", output)
	}
}

func TestCache_StatsCommand(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()

	// Create a test CSV file
	csvContent := "id,name,value\n1,alice,100\n2,bob,200"
	csvFile := filepath.Join(t.TempDir(), "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	projectRoot := findProjectRoot(t)

	// Run with cache to create cache entry
	cmd1 := exec.Command("./dataql", "run",
		"-f", csvFile,
		"-q", "SELECT * FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
	)
	cmd1.Dir = projectRoot
	if err := cmd1.Run(); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	// Get cache stats
	cmd2 := exec.Command("./dataql", "cache", "stats",
		"-d", cacheDir,
	)
	cmd2.Dir = projectRoot
	var stdout bytes.Buffer
	cmd2.Stdout = &stdout

	if err := cmd2.Run(); err != nil {
		t.Fatalf("cache stats failed: %v", err)
	}

	output := stdout.String()
	if !strings.Contains(output, "Cache directory:") {
		t.Errorf("cache stats should show cache directory, got: %s", output)
	}
	if !strings.Contains(output, "Cached entries: 1") {
		t.Errorf("cache stats should show 1 entry, got: %s", output)
	}
}

func TestCache_ClearCommand(t *testing.T) {
	// Create temp directory for cache
	cacheDir := t.TempDir()

	// Create a test CSV file
	csvContent := "id,name,value\n1,alice,100\n2,bob,200"
	csvFile := filepath.Join(t.TempDir(), "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	projectRoot := findProjectRoot(t)

	// Run with cache to create cache entry
	cmd1 := exec.Command("./dataql", "run",
		"-f", csvFile,
		"-q", "SELECT * FROM test",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
	)
	cmd1.Dir = projectRoot
	if err := cmd1.Run(); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	// Clear cache with --all flag
	cmd2 := exec.Command("./dataql", "cache", "clear",
		"-d", cacheDir,
		"--all",
	)
	cmd2.Dir = projectRoot
	var stdout bytes.Buffer
	cmd2.Stdout = &stdout

	if err := cmd2.Run(); err != nil {
		t.Fatalf("cache clear failed: %v", err)
	}

	output := stdout.String()
	if !strings.Contains(output, "Cleared") {
		t.Errorf("cache clear should show cleared message, got: %s", output)
	}

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
	csvFile := filepath.Join(t.TempDir(), "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	projectRoot := findProjectRoot(t)

	// Run WITHOUT cache flag
	cmd := exec.Command("./dataql", "run",
		"-f", csvFile,
		"-q", "SELECT * FROM test",
		"--cache-dir", cacheDir, // specify dir but not --cache flag
		"-Q",
	)
	cmd.Dir = projectRoot
	if err := cmd.Run(); err != nil {
		t.Fatalf("run failed: %v", err)
	}

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

	projectRoot := findProjectRoot(t)

	// First run - should import and cache
	cmd1 := exec.Command("./dataql", "run",
		"-f", csvFile1,
		"-f", csvFile2,
		"-q", "SELECT u.name, v.value FROM users u JOIN values v ON u.id = v.id",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
		"-v",
	)
	cmd1.Dir = projectRoot
	var stdout1 bytes.Buffer
	cmd1.Stdout = &stdout1

	if err := cmd1.Run(); err != nil {
		t.Fatalf("first run failed: %v", err)
	}

	if !strings.Contains(stdout1.String(), "Starting data import") {
		t.Error("first run should show import message")
	}

	// Second run - should use cache
	cmd2 := exec.Command("./dataql", "run",
		"-f", csvFile1,
		"-f", csvFile2,
		"-q", "SELECT u.name, v.value FROM users u JOIN values v ON u.id = v.id",
		"--cache",
		"--cache-dir", cacheDir,
		"-Q",
		"-v",
	)
	cmd2.Dir = projectRoot
	var stdout2 bytes.Buffer
	cmd2.Stdout = &stdout2

	if err := cmd2.Run(); err != nil {
		t.Fatalf("second run failed: %v", err)
	}

	if !strings.Contains(stdout2.String(), "Using cached data") {
		t.Error("second run should use cached data")
	}
}

// findProjectRoot finds the project root directory
func findProjectRoot(t *testing.T) string {
	// Try to find the project root by looking for go.mod
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find project root")
		}
		dir = parent
	}
}
