package e2e_test

import (
	"os"
	"strings"
	"testing"
)

// Azure tests require valid Azure credentials and are skipped by default
// Set DATAQL_TEST_AZURE=1 to enable them

func skipIfNoAzure(t *testing.T) {
	t.Helper()
	if os.Getenv("DATAQL_TEST_AZURE") == "" {
		t.Skip("Skipping Azure test: DATAQL_TEST_AZURE not set")
	}
}

func TestAzure_InvalidURL(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "azure://invalid", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for invalid Azure URL")
	}
	if !strings.Contains(stderr, "invalid Azure URL") && !strings.Contains(stderr, "Error") {
		t.Errorf("Expected invalid URL error, got: %s", stderr)
	}
}

func TestAzure_MissingBlob(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "azure://container-only", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for Azure URL without blob")
	}
	if !strings.Contains(stderr, "invalid Azure URL") && !strings.Contains(stderr, "Error") {
		t.Errorf("Expected invalid URL error, got: %s", stderr)
	}
}

func TestAzure_BasicCSVQuery(t *testing.T) {
	skipIfNoAzure(t)

	azureURL := os.Getenv("DATAQL_AZURE_TEST_CSV")
	if azureURL == "" {
		azureURL = "azure://testcontainer/data/users.csv"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", azureURL, "-q", "SELECT * FROM users")
	if err != nil {
		t.Errorf("Azure query failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("Azure output: %s", stdout)
}

func TestAzure_BasicJSONQuery(t *testing.T) {
	skipIfNoAzure(t)

	azureURL := os.Getenv("DATAQL_AZURE_TEST_JSON")
	if azureURL == "" {
		azureURL = "azure://testcontainer/data/users.json"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", azureURL, "-q", "SELECT * FROM users")
	if err != nil {
		t.Errorf("Azure JSON query failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("Azure JSON output: %s", stdout)
}

func TestAzure_WithLineLimit(t *testing.T) {
	skipIfNoAzure(t)

	azureURL := os.Getenv("DATAQL_AZURE_TEST_CSV")
	if azureURL == "" {
		azureURL = "azure://testcontainer/data/users.csv"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", azureURL, "-l", "5", "-q", "SELECT * FROM users")
	if err != nil {
		t.Errorf("Azure query with limit failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("Azure limited output: %s", stdout)
}

func TestAzure_SelectSpecificColumns(t *testing.T) {
	skipIfNoAzure(t)

	azureURL := os.Getenv("DATAQL_AZURE_TEST_CSV")
	if azureURL == "" {
		azureURL = "azure://testcontainer/data/users.csv"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", azureURL, "-q", "SELECT id, name FROM users LIMIT 5")
	if err != nil {
		t.Errorf("Azure select columns failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "id") || !strings.Contains(stdout, "name") {
		t.Errorf("Expected id and name columns in output, got: %s", stdout)
	}
}

func TestAzure_ExportToCSV(t *testing.T) {
	skipIfNoAzure(t)

	azureURL := os.Getenv("DATAQL_AZURE_TEST_CSV")
	if azureURL == "" {
		azureURL = "azure://testcontainer/data/users.csv"
	}

	outputFile := tempFile(t, "azure_export.csv")

	_, stderr, err := runDataQL(t, "run", "-f", azureURL, "-q", "SELECT * FROM users LIMIT 10", "-e", outputFile, "-t", "csv")
	if err != nil {
		t.Errorf("Azure export to CSV failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("Export file was not created")
	}
}

func TestAzure_ExportToJSON(t *testing.T) {
	skipIfNoAzure(t)

	azureURL := os.Getenv("DATAQL_AZURE_TEST_CSV")
	if azureURL == "" {
		azureURL = "azure://testcontainer/data/users.csv"
	}

	outputFile := tempFile(t, "azure_export.json")

	_, stderr, err := runDataQL(t, "run", "-f", azureURL, "-q", "SELECT * FROM users LIMIT 10", "-e", outputFile, "-t", "json")
	if err != nil {
		t.Errorf("Azure export to JSON failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("Export file was not created")
	}
}

func TestAzure_BlobURLFormat(t *testing.T) {
	skipIfNoAzure(t)

	// Test https://<account>.blob.core.windows.net/<container>/<blob> format
	azureURL := os.Getenv("DATAQL_AZURE_TEST_BLOB_URL")
	if azureURL == "" {
		azureURL = "https://teststorage.blob.core.windows.net/testcontainer/data/users.csv"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", azureURL, "-q", "SELECT * FROM users LIMIT 5")
	if err != nil {
		t.Errorf("Azure Blob URL query failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("Azure Blob URL output: %s", stdout)
}

func TestAzure_ParquetFile(t *testing.T) {
	skipIfNoAzure(t)

	azureURL := os.Getenv("DATAQL_AZURE_TEST_PARQUET")
	if azureURL == "" {
		azureURL = "azure://testcontainer/data/users.parquet"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", azureURL, "-q", "SELECT * FROM users LIMIT 5")
	if err != nil {
		t.Errorf("Azure Parquet query failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("Azure Parquet output: %s", stdout)
}
