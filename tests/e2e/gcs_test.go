package e2e_test

import (
	"os"
	"strings"
	"testing"
)

// GCS tests require valid GCS credentials and are skipped by default
// Set DATAQL_TEST_GCS=1 to enable them

func skipIfNoGCS(t *testing.T) {
	t.Helper()
	if os.Getenv("DATAQL_TEST_GCS") == "" {
		t.Skip("Skipping GCS test: DATAQL_TEST_GCS not set")
	}
}

func TestGCS_InvalidURL(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "gs://invalid", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for invalid GCS URL")
	}
	if !strings.Contains(stderr, "invalid GCS URL") && !strings.Contains(stderr, "Error") {
		t.Errorf("Expected invalid URL error, got: %s", stderr)
	}
}

func TestGCS_MissingObject(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "gs://bucket-only", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for GCS URL without object")
	}
	if !strings.Contains(stderr, "invalid GCS URL") && !strings.Contains(stderr, "Error") {
		t.Errorf("Expected invalid URL error, got: %s", stderr)
	}
}

func TestGCS_BasicCSVQuery(t *testing.T) {
	skipIfNoGCS(t)

	gcsURL := os.Getenv("DATAQL_GCS_TEST_CSV")
	if gcsURL == "" {
		gcsURL = "gs://test-bucket/data/users.csv"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", gcsURL, "-q", "SELECT * FROM users")
	if err != nil {
		t.Errorf("GCS query failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("GCS output: %s", stdout)
}

func TestGCS_BasicJSONQuery(t *testing.T) {
	skipIfNoGCS(t)

	gcsURL := os.Getenv("DATAQL_GCS_TEST_JSON")
	if gcsURL == "" {
		gcsURL = "gs://test-bucket/data/users.json"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", gcsURL, "-q", "SELECT * FROM users")
	if err != nil {
		t.Errorf("GCS JSON query failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("GCS JSON output: %s", stdout)
}

func TestGCS_WithLineLimit(t *testing.T) {
	skipIfNoGCS(t)

	gcsURL := os.Getenv("DATAQL_GCS_TEST_CSV")
	if gcsURL == "" {
		gcsURL = "gs://test-bucket/data/users.csv"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", gcsURL, "-l", "5", "-q", "SELECT * FROM users")
	if err != nil {
		t.Errorf("GCS query with limit failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("GCS limited output: %s", stdout)
}

func TestGCS_SelectSpecificColumns(t *testing.T) {
	skipIfNoGCS(t)

	gcsURL := os.Getenv("DATAQL_GCS_TEST_CSV")
	if gcsURL == "" {
		gcsURL = "gs://test-bucket/data/users.csv"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", gcsURL, "-q", "SELECT id, name FROM users LIMIT 5")
	if err != nil {
		t.Errorf("GCS select columns failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "id") || !strings.Contains(stdout, "name") {
		t.Errorf("Expected id and name columns in output, got: %s", stdout)
	}
}

func TestGCS_ExportToCSV(t *testing.T) {
	skipIfNoGCS(t)

	gcsURL := os.Getenv("DATAQL_GCS_TEST_CSV")
	if gcsURL == "" {
		gcsURL = "gs://test-bucket/data/users.csv"
	}

	outputFile := tempFile(t, "gcs_export.csv")

	_, stderr, err := runDataQL(t, "run", "-f", gcsURL, "-q", "SELECT * FROM users LIMIT 10", "-e", outputFile, "-t", "csv")
	if err != nil {
		t.Errorf("GCS export to CSV failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("Export file was not created")
	}
}

func TestGCS_ExportToJSON(t *testing.T) {
	skipIfNoGCS(t)

	gcsURL := os.Getenv("DATAQL_GCS_TEST_CSV")
	if gcsURL == "" {
		gcsURL = "gs://test-bucket/data/users.csv"
	}

	outputFile := tempFile(t, "gcs_export.json")

	_, stderr, err := runDataQL(t, "run", "-f", gcsURL, "-q", "SELECT * FROM users LIMIT 10", "-e", outputFile, "-t", "json")
	if err != nil {
		t.Errorf("GCS export to JSON failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("Export file was not created")
	}
}

func TestGCS_ParquetFile(t *testing.T) {
	skipIfNoGCS(t)

	gcsURL := os.Getenv("DATAQL_GCS_TEST_PARQUET")
	if gcsURL == "" {
		gcsURL = "gs://test-bucket/data/users.parquet"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", gcsURL, "-q", "SELECT * FROM users LIMIT 5")
	if err != nil {
		t.Errorf("GCS Parquet query failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("GCS Parquet output: %s", stdout)
}
