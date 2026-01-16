package e2e_test

import (
	"os"
	"strings"
	"testing"
)

// MongoDB tests require a running MongoDB instance and are skipped by default
// Set DATAQL_TEST_MONGODB=1 to enable them

func skipIfNoMongoDB(t *testing.T) {
	t.Helper()
	if os.Getenv("DATAQL_TEST_MONGODB") == "" {
		t.Skip("Skipping MongoDB test: DATAQL_TEST_MONGODB not set")
	}
}

func TestMongoDB_InvalidURL(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "mongodb://invalid", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for invalid MongoDB URL")
	}
	if !strings.Contains(stderr, "invalid MongoDB URL") && !strings.Contains(stderr, "missing") && !strings.Contains(stderr, "Error") {
		t.Errorf("Expected invalid URL error, got: %s", stderr)
	}
}

func TestMongoDB_NoDatabase(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "mongodb://localhost:27017", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for MongoDB URL without database")
	}
	if !strings.Contains(stderr, "database") && !strings.Contains(stderr, "Error") {
		t.Errorf("Expected database error, got: %s", stderr)
	}
}

func TestMongoDB_NoCollection(t *testing.T) {
	_, stderr, err := runDataQL(t, "run", "-f", "mongodb://localhost:27017/testdb", "-q", "SELECT * FROM data")
	if err == nil {
		t.Error("Expected error for MongoDB URL without collection")
	}
	if !strings.Contains(stderr, "collection") && !strings.Contains(stderr, "Error") {
		t.Errorf("Expected collection error, got: %s", stderr)
	}
}

func TestMongoDB_BasicQuery(t *testing.T) {
	skipIfNoMongoDB(t)

	// This test requires a running MongoDB instance with test data
	// mongodb://localhost:27017/testdb/users
	mongoURL := os.Getenv("DATAQL_MONGODB_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27017/testdb/users"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", mongoURL, "-q", "SELECT * FROM users")
	if err != nil {
		t.Errorf("MongoDB query failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("MongoDB output: %s", stdout)
}

func TestMongoDB_WithLineLimit(t *testing.T) {
	skipIfNoMongoDB(t)

	mongoURL := os.Getenv("DATAQL_MONGODB_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27017/testdb/users"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", mongoURL, "-l", "5", "-q", "SELECT * FROM users")
	if err != nil {
		t.Errorf("MongoDB query with limit failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("MongoDB limited output: %s", stdout)
}

func TestMongoDB_SelectSpecificColumns(t *testing.T) {
	skipIfNoMongoDB(t)

	mongoURL := os.Getenv("DATAQL_MONGODB_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27017/testdb/users"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", mongoURL, "-q", "SELECT _id, name FROM users LIMIT 5")
	if err != nil {
		t.Errorf("MongoDB select columns failed: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "_id") || !strings.Contains(stdout, "name") {
		t.Errorf("Expected _id and name columns in output, got: %s", stdout)
	}
}

func TestMongoDB_WithCollection(t *testing.T) {
	skipIfNoMongoDB(t)

	mongoURL := os.Getenv("DATAQL_MONGODB_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27017/testdb/users"
	}

	stdout, stderr, err := runDataQL(t, "run", "-f", mongoURL, "-c", "custom_collection", "-q", "SELECT * FROM custom_collection LIMIT 5")
	if err != nil {
		t.Errorf("MongoDB with custom collection failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("MongoDB custom collection output: %s", stdout)
}

func TestMongoDB_ExportToCSV(t *testing.T) {
	skipIfNoMongoDB(t)

	mongoURL := os.Getenv("DATAQL_MONGODB_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27017/testdb/users"
	}

	outputFile := tempFile(t, "mongodb_export.csv")

	_, stderr, err := runDataQL(t, "run", "-f", mongoURL, "-q", "SELECT * FROM users LIMIT 10", "-e", outputFile, "-t", "csv")
	if err != nil {
		t.Errorf("MongoDB export to CSV failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("Export file was not created")
	}
}

func TestMongoDB_ExportToJSONL(t *testing.T) {
	skipIfNoMongoDB(t)

	mongoURL := os.Getenv("DATAQL_MONGODB_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27017/testdb/users"
	}

	outputFile := tempFile(t, "mongodb_export.jsonl")

	_, stderr, err := runDataQL(t, "run", "-f", mongoURL, "-q", "SELECT * FROM users LIMIT 10", "-e", outputFile, "-t", "jsonl")
	if err != nil {
		t.Errorf("MongoDB export to JSONL failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("Export file was not created")
	}
}

func TestMongoDB_URLWithAuth(t *testing.T) {
	// Test URL parsing with authentication
	_, stderr, err := runDataQL(t, "run", "-f", "mongodb://user:pass@localhost:27017/testdb/users", "-q", "SELECT * FROM users")
	// This will fail to connect (no server) but should parse the URL correctly
	if err == nil {
		t.Log("Command succeeded - MongoDB server might be running")
	}
	// Should not be a URL parsing error
	if strings.Contains(stderr, "invalid MongoDB URL") || strings.Contains(stderr, "missing database") || strings.Contains(stderr, "missing collection") {
		t.Errorf("URL parsing failed: %s", stderr)
	}
}

func TestMongoDB_SRVFormat(t *testing.T) {
	// Test mongodb+srv:// URL format (Atlas style)
	_, stderr, err := runDataQL(t, "run", "-f", "mongodb+srv://user:pass@cluster.mongodb.net/testdb/users", "-q", "SELECT * FROM users")
	// This will fail to connect but should parse the URL correctly
	if err == nil {
		t.Log("Command succeeded - MongoDB Atlas server might be reachable")
	}
	// Should not be a URL parsing error for format
	if strings.Contains(stderr, "invalid MongoDB URL") || strings.Contains(stderr, "missing database") || strings.Contains(stderr, "missing collection") {
		t.Errorf("URL parsing failed for SRV format: %s", stderr)
	}
}
