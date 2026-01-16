package e2e_test

import (
	"os"
	"strings"
	"testing"
)

// S3 tests require AWS credentials and an S3 bucket with test files.
// Set environment variables to enable:
// - DATAQL_TEST_S3_CSV=s3://bucket/path/to/file.csv
// - DATAQL_TEST_S3_JSON=s3://bucket/path/to/file.json
// - AWS credentials should be configured via environment variables or ~/.aws/credentials

func getS3CSVURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATAQL_TEST_S3_CSV")
	if url == "" {
		t.Skip("Skipping S3 CSV test: DATAQL_TEST_S3_CSV not set")
	}
	return url
}

func getS3JSONURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATAQL_TEST_S3_JSON")
	if url == "" {
		t.Skip("Skipping S3 JSON test: DATAQL_TEST_S3_JSON not set")
	}
	return url
}

func TestS3_CSVBasicQuery(t *testing.T) {
	s3URL := getS3CSVURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", s3URL, "-q", "SELECT COUNT(*) FROM data")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "COUNT") {
		t.Errorf("Expected COUNT in output, got: %s", stdout)
	}
}

func TestS3_CSVSelectAll(t *testing.T) {
	s3URL := getS3CSVURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", s3URL, "-q", "SELECT * FROM data LIMIT 5")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if stdout == "" {
		t.Error("Expected some output")
	}
}

func TestS3_CSVWithCollection(t *testing.T) {
	s3URL := getS3CSVURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", s3URL, "-c", "mydata", "-q", "SELECT COUNT(*) FROM mydata")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "COUNT") {
		t.Errorf("Expected COUNT in output, got: %s", stdout)
	}
}

func TestS3_CSVWithLineLimit(t *testing.T) {
	s3URL := getS3CSVURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", s3URL, "-l", "5", "-q", "SELECT COUNT(*) as cnt FROM data")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "5") {
		t.Errorf("Expected 5 rows, got: %s", stdout)
	}
}

func TestS3_CSVExportJSONL(t *testing.T) {
	s3URL := getS3CSVURL(t)
	tmpFile := tempFile(t, "export.jsonl")

	_, stderr, err := runDataQL(t, "run", "-f", s3URL, "-q", "SELECT * FROM data LIMIT 3", "-e", tmpFile, "-t", "jsonl")
	if err != nil {
		t.Fatalf("Failed to export: %v\nstderr: %s", err, stderr)
	}

	content, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read export: %v", err)
	}

	if len(content) == 0 {
		t.Error("Expected non-empty export")
	}
}

func TestS3_JSONBasicQuery(t *testing.T) {
	s3URL := getS3JSONURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", s3URL, "-q", "SELECT COUNT(*) FROM data")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "COUNT") {
		t.Errorf("Expected COUNT in output, got: %s", stdout)
	}
}

func TestS3_JSONSelectAll(t *testing.T) {
	s3URL := getS3JSONURL(t)

	stdout, stderr, err := runDataQL(t, "run", "-f", s3URL, "-q", "SELECT * FROM data LIMIT 5")
	if err != nil {
		t.Fatalf("Failed to run dataql: %v\nstderr: %s", err, stderr)
	}

	if stdout == "" {
		t.Error("Expected some output")
	}
}

func TestS3_JSONExportCSV(t *testing.T) {
	s3URL := getS3JSONURL(t)
	tmpFile := tempFile(t, "export.csv")

	_, stderr, err := runDataQL(t, "run", "-f", s3URL, "-q", "SELECT * FROM data LIMIT 3", "-e", tmpFile, "-t", "csv")
	if err != nil {
		t.Fatalf("Failed to export: %v\nstderr: %s", err, stderr)
	}

	content, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read export: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) < 2 {
		t.Errorf("Expected at least header + 1 data row, got %d lines", len(lines))
	}
}

// Error cases

func TestS3_InvalidURL(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "s3://invalid", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for invalid S3 URL")
	}
	if !strings.Contains(stderr, "invalid S3 URL") && !strings.Contains(stderr, "expected s3://bucket/key") {
		t.Errorf("Expected invalid URL error, got: %s", stderr)
	}
}

func TestS3_InvalidBucket(t *testing.T) {
	// Skip this test if AWS is not configured
	if os.Getenv("AWS_REGION") == "" && os.Getenv("AWS_DEFAULT_REGION") == "" {
		t.Skip("Skipping S3 bucket error test: AWS not configured")
	}

	_, stderr, err := runDataQL(t, "run", "-f", "s3://nonexistent-bucket-xyz123/test.csv", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for non-existent bucket")
	}
	if !strings.Contains(stderr, "failed") && !strings.Contains(stderr, "NoSuchBucket") && !strings.Contains(stderr, "Access Denied") {
		t.Errorf("Expected S3 error, got: %s", stderr)
	}
}
