package compressionhandler

import (
	"os"
	"os/exec"
	"testing"
)

func TestDetectCompression_Gzip(t *testing.T) {
	tests := []struct {
		path     string
		expected Compression
	}{
		{"data.csv.gz", CompressionGzip},
		{"data.csv.gzip", CompressionGzip},
		{"data.CSV.GZ", CompressionGzip},
		{"data.csv.bz2", CompressionBzip2},
		{"data.csv.xz", CompressionXZ},
		{"data.csv.zst", CompressionZstd},
		{"data.csv.zstd", CompressionZstd},
		{"data.csv", CompressionNone},
		{"data.json", CompressionNone},
		{"/path/to/file.csv.gz", CompressionGzip},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := DetectCompression(tt.path)
			if result != tt.expected {
				t.Errorf("DetectCompression(%q) = %q, want %q", tt.path, result, tt.expected)
			}
		})
	}
}

func TestIsCompressed(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"data.csv.gz", true},
		{"data.csv.bz2", true},
		{"data.csv.xz", true},
		{"data.csv", false},
		{"data.json", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := IsCompressed(tt.path)
			if result != tt.expected {
				t.Errorf("IsCompressed(%q) = %v, want %v", tt.path, result, tt.expected)
			}
		})
	}
}

func TestGetUncompressedPath(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"data.csv.gz", "data.csv"},
		{"data.csv.bz2", "data.csv"},
		{"data.csv.xz", "data.csv"},
		{"data.csv", "data.csv"},
		{"/path/to/data.json.gz", "/path/to/data.json"},
		{"file.tar.gz", "file.tar"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := GetUncompressedPath(tt.path)
			if result != tt.expected {
				t.Errorf("GetUncompressedPath(%q) = %q, want %q", tt.path, result, tt.expected)
			}
		})
	}
}

func TestGetInnerExtension(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"data.csv.gz", ".csv"},
		{"data.json.bz2", ".json"},
		{"data.xml.xz", ".xml"},
		{"data.csv", ".csv"},
		{"/path/to/data.parquet.gz", ".parquet"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := GetInnerExtension(tt.path)
			if result != tt.expected {
				t.Errorf("GetInnerExtension(%q) = %q, want %q", tt.path, result, tt.expected)
			}
		})
	}
}

func TestCompressionHandler_ResolveFiles_Gzip(t *testing.T) {
	// Skip if gzip is not available
	if _, err := exec.LookPath("gzip"); err != nil {
		t.Skip("gzip command not available")
	}

	// Create a temp CSV file
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	csvPath := tmpFile.Name()
	defer os.Remove(csvPath)

	content := "id,name,value\n1,Alice,100\n2,Bob,200\n"
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Compress with gzip
	gzPath := csvPath + ".gz"
	cmd := exec.Command("gzip", "-k", csvPath)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to compress file: %v", err)
	}
	defer os.Remove(gzPath)

	// Test ResolveFiles
	handler := NewCompressionHandler()
	defer handler.Cleanup()

	resolved, err := handler.ResolveFiles([]string{gzPath})
	if err != nil {
		t.Fatalf("ResolveFiles failed: %v", err)
	}

	if len(resolved) != 1 {
		t.Fatalf("Expected 1 resolved file, got %d", len(resolved))
	}

	// Verify the resolved file is different from input
	if resolved[0] == gzPath {
		t.Error("Resolved file should be different from compressed file")
	}

	// Verify the resolved file exists and has correct content
	decompressedContent, err := os.ReadFile(resolved[0])
	if err != nil {
		t.Fatalf("Failed to read decompressed file: %v", err)
	}

	if string(decompressedContent) != content {
		t.Errorf("Decompressed content mismatch.\nGot: %q\nWant: %q", string(decompressedContent), content)
	}

	// Verify GetOriginalPath works
	originalPath := handler.GetOriginalPath(resolved[0])
	if originalPath != gzPath {
		t.Errorf("GetOriginalPath returned %q, want %q", originalPath, gzPath)
	}
}

func TestCompressionHandler_ResolveFiles_NonCompressed(t *testing.T) {
	// Create a temp CSV file (not compressed)
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	csvPath := tmpFile.Name()
	defer os.Remove(csvPath)

	content := "id,name,value\n1,Alice,100\n"
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Test ResolveFiles with non-compressed file
	handler := NewCompressionHandler()
	defer handler.Cleanup()

	resolved, err := handler.ResolveFiles([]string{csvPath})
	if err != nil {
		t.Fatalf("ResolveFiles failed: %v", err)
	}

	if len(resolved) != 1 {
		t.Fatalf("Expected 1 resolved file, got %d", len(resolved))
	}

	// Non-compressed files should be returned as-is
	if resolved[0] != csvPath {
		t.Errorf("Non-compressed file should be returned unchanged. Got %q, want %q", resolved[0], csvPath)
	}
}

func TestCompressionHandler_Cleanup(t *testing.T) {
	// Skip if gzip is not available
	if _, err := exec.LookPath("gzip"); err != nil {
		t.Skip("gzip command not available")
	}

	// Create a temp CSV file
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	csvPath := tmpFile.Name()
	defer os.Remove(csvPath)

	if _, err := tmpFile.WriteString("id,name\n1,test\n"); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Compress with gzip
	gzPath := csvPath + ".gz"
	cmd := exec.Command("gzip", "-k", csvPath)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to compress file: %v", err)
	}
	defer os.Remove(gzPath)

	// Resolve and get temp file path
	handler := NewCompressionHandler()
	resolved, err := handler.ResolveFiles([]string{gzPath})
	if err != nil {
		t.Fatalf("ResolveFiles failed: %v", err)
	}

	tempPath := resolved[0]

	// Verify temp file exists
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatalf("Temp file should exist: %v", err)
	}

	// Cleanup
	if err := handler.Cleanup(); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	// Verify temp file is removed
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Error("Temp file should be removed after Cleanup")
	}
}

func TestSupportedCompressions(t *testing.T) {
	compressions := SupportedCompressions()
	if len(compressions) == 0 {
		t.Error("SupportedCompressions should return at least one format")
	}

	// Should include gzip at minimum
	hasGzip := false
	for _, c := range compressions {
		if c == "gzip (.gz)" {
			hasGzip = true
			break
		}
	}
	if !hasGzip {
		t.Error("SupportedCompressions should include gzip")
	}
}
