package cachehandler

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewCacheHandler_Disabled(t *testing.T) {
	handler, err := NewCacheHandler("", false)
	if err != nil {
		t.Fatalf("NewCacheHandler failed: %v", err)
	}

	if handler.IsEnabled() {
		t.Error("expected handler to be disabled")
	}
}

func TestNewCacheHandler_Enabled(t *testing.T) {
	tmpDir := t.TempDir()

	handler, err := NewCacheHandler(tmpDir, true)
	if err != nil {
		t.Fatalf("NewCacheHandler failed: %v", err)
	}

	if !handler.IsEnabled() {
		t.Error("expected handler to be enabled")
	}

	if handler.GetCacheDir() != tmpDir {
		t.Errorf("expected cache dir %s, got %s", tmpDir, handler.GetCacheDir())
	}
}

func TestNewCacheHandler_DefaultDir(t *testing.T) {
	handler, err := NewCacheHandler("", true)
	if err != nil {
		t.Fatalf("NewCacheHandler failed: %v", err)
	}

	if !handler.IsEnabled() {
		t.Error("expected handler to be enabled")
	}

	homeDir, _ := os.UserHomeDir()
	expectedDir := filepath.Join(homeDir, ".dataql", "cache")
	if handler.GetCacheDir() != expectedDir {
		t.Errorf("expected cache dir %s, got %s", expectedDir, handler.GetCacheDir())
	}
}

func TestGenerateCacheKey(t *testing.T) {
	tmpDir := t.TempDir()
	handler, err := NewCacheHandler(tmpDir, true)
	if err != nil {
		t.Fatalf("NewCacheHandler failed: %v", err)
	}

	// Create a temp file
	tmpFile := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(tmpFile, []byte("a,b,c\n1,2,3"), 0644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	key, err := handler.GenerateCacheKey([]string{tmpFile})
	if err != nil {
		t.Fatalf("GenerateCacheKey failed: %v", err)
	}

	if len(key) != 32 { // 16 bytes hex encoded = 32 chars
		t.Errorf("expected key length 32, got %d", len(key))
	}

	// Same files should produce same key
	key2, err := handler.GenerateCacheKey([]string{tmpFile})
	if err != nil {
		t.Fatalf("GenerateCacheKey failed: %v", err)
	}

	if key != key2 {
		t.Error("same files should produce same cache key")
	}
}

func TestGenerateCacheKey_DifferentFiles(t *testing.T) {
	tmpDir := t.TempDir()
	handler, err := NewCacheHandler(tmpDir, true)
	if err != nil {
		t.Fatalf("NewCacheHandler failed: %v", err)
	}

	// Create two temp files
	tmpFile1 := filepath.Join(tmpDir, "test1.csv")
	tmpFile2 := filepath.Join(tmpDir, "test2.csv")
	if err := os.WriteFile(tmpFile1, []byte("a,b,c\n1,2,3"), 0644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	if err := os.WriteFile(tmpFile2, []byte("x,y,z\n4,5,6"), 0644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	key1, _ := handler.GenerateCacheKey([]string{tmpFile1})
	key2, _ := handler.GenerateCacheKey([]string{tmpFile2})

	if key1 == key2 {
		t.Error("different files should produce different cache keys")
	}
}

func TestGenerateCacheKey_Disabled(t *testing.T) {
	handler, _ := NewCacheHandler("", false)

	key, err := handler.GenerateCacheKey([]string{"test.csv"})
	if err != nil {
		t.Fatalf("GenerateCacheKey failed: %v", err)
	}

	if key != "" {
		t.Error("expected empty key when handler is disabled")
	}
}

func TestGetCachePath(t *testing.T) {
	tmpDir := t.TempDir()
	handler, _ := NewCacheHandler(tmpDir, true)

	path := handler.GetCachePath("abc123")
	expected := filepath.Join(tmpDir, "abc123.duckdb")

	if path != expected {
		t.Errorf("expected %s, got %s", expected, path)
	}
}

func TestGetCachePath_Disabled(t *testing.T) {
	handler, _ := NewCacheHandler("", false)

	path := handler.GetCachePath("abc123")
	if path != "" {
		t.Error("expected empty path when handler is disabled")
	}
}

func TestGetMetadataPath(t *testing.T) {
	tmpDir := t.TempDir()
	handler, _ := NewCacheHandler(tmpDir, true)

	path := handler.GetMetadataPath("abc123")
	expected := filepath.Join(tmpDir, "abc123.json")

	if path != expected {
		t.Errorf("expected %s, got %s", expected, path)
	}
}

func TestIsCacheValid_NoCache(t *testing.T) {
	tmpDir := t.TempDir()
	handler, _ := NewCacheHandler(tmpDir, true)

	// Create a temp file
	tmpFile := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(tmpFile, []byte("a,b,c\n1,2,3"), 0644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	valid, _, err := handler.IsCacheValid([]string{tmpFile})
	if err != nil {
		t.Fatalf("IsCacheValid failed: %v", err)
	}

	if valid {
		t.Error("expected cache to be invalid when no cache exists")
	}
}

func TestSaveAndReadMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	handler, _ := NewCacheHandler(tmpDir, true)

	// Create a temp file for the source
	tmpFile := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(tmpFile, []byte("a,b,c\n1,2,3"), 0644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	cacheKey := "testkey123"
	tables := []string{"test"}
	totalRows := int64(1)

	// Save metadata
	err := handler.SaveMetadata(cacheKey, []string{tmpFile}, tables, totalRows)
	if err != nil {
		t.Fatalf("SaveMetadata failed: %v", err)
	}

	// Read metadata
	metadata, err := handler.ReadMetadata(cacheKey)
	if err != nil {
		t.Fatalf("ReadMetadata failed: %v", err)
	}

	if metadata.TotalRows != totalRows {
		t.Errorf("expected total rows %d, got %d", totalRows, metadata.TotalRows)
	}

	if len(metadata.Tables) != 1 || metadata.Tables[0] != "test" {
		t.Errorf("expected tables [test], got %v", metadata.Tables)
	}

	if metadata.FormatVersion != cacheFormatVersion {
		t.Errorf("expected format version %d, got %d", cacheFormatVersion, metadata.FormatVersion)
	}
}

func TestClearCache(t *testing.T) {
	tmpDir := t.TempDir()
	handler, _ := NewCacheHandler(tmpDir, true)

	// Create some cache files
	cacheFile := filepath.Join(tmpDir, "test.duckdb")
	metaFile := filepath.Join(tmpDir, "test.json")
	if err := os.WriteFile(cacheFile, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create cache file: %v", err)
	}
	if err := os.WriteFile(metaFile, []byte("{}"), 0644); err != nil {
		t.Fatalf("failed to create meta file: %v", err)
	}

	// Clear cache
	err := handler.ClearCache()
	if err != nil {
		t.Fatalf("ClearCache failed: %v", err)
	}

	// Verify files are gone
	if _, err := os.Stat(cacheFile); !os.IsNotExist(err) {
		t.Error("cache file should have been deleted")
	}
	if _, err := os.Stat(metaFile); !os.IsNotExist(err) {
		t.Error("metadata file should have been deleted")
	}
}

func TestClearCacheEntry(t *testing.T) {
	tmpDir := t.TempDir()
	handler, _ := NewCacheHandler(tmpDir, true)

	// Create cache files for two entries
	cacheFile1 := filepath.Join(tmpDir, "entry1.duckdb")
	metaFile1 := filepath.Join(tmpDir, "entry1.json")
	cacheFile2 := filepath.Join(tmpDir, "entry2.duckdb")
	metaFile2 := filepath.Join(tmpDir, "entry2.json")

	for _, f := range []string{cacheFile1, metaFile1, cacheFile2, metaFile2} {
		if err := os.WriteFile(f, []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create file: %v", err)
		}
	}

	// Clear only entry1
	err := handler.ClearCacheEntry("entry1")
	if err != nil {
		t.Fatalf("ClearCacheEntry failed: %v", err)
	}

	// Verify entry1 files are gone
	if _, err := os.Stat(cacheFile1); !os.IsNotExist(err) {
		t.Error("entry1 cache file should have been deleted")
	}
	if _, err := os.Stat(metaFile1); !os.IsNotExist(err) {
		t.Error("entry1 metadata file should have been deleted")
	}

	// Verify entry2 files still exist
	if _, err := os.Stat(cacheFile2); os.IsNotExist(err) {
		t.Error("entry2 cache file should still exist")
	}
	if _, err := os.Stat(metaFile2); os.IsNotExist(err) {
		t.Error("entry2 metadata file should still exist")
	}
}

func TestListCache(t *testing.T) {
	tmpDir := t.TempDir()
	handler, _ := NewCacheHandler(tmpDir, true)

	// Create a temp file for the source
	tmpFile := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(tmpFile, []byte("a,b,c\n1,2,3"), 0644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	// Create cache with metadata
	cacheKey := "testcache"
	cacheFile := filepath.Join(tmpDir, cacheKey+".duckdb")
	if err := os.WriteFile(cacheFile, []byte("duckdb data"), 0644); err != nil {
		t.Fatalf("failed to create cache file: %v", err)
	}

	err := handler.SaveMetadata(cacheKey, []string{tmpFile}, []string{"test"}, 100)
	if err != nil {
		t.Fatalf("SaveMetadata failed: %v", err)
	}

	// List cache
	entries, err := handler.ListCache()
	if err != nil {
		t.Fatalf("ListCache failed: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if entries[0].CacheKey != cacheKey {
		t.Errorf("expected cache key %s, got %s", cacheKey, entries[0].CacheKey)
	}

	if entries[0].TotalRows != 100 {
		t.Errorf("expected 100 rows, got %d", entries[0].TotalRows)
	}
}

func TestGetCacheStats(t *testing.T) {
	tmpDir := t.TempDir()
	handler, _ := NewCacheHandler(tmpDir, true)

	// Create cache files
	cacheFile1 := filepath.Join(tmpDir, "entry1.duckdb")
	cacheFile2 := filepath.Join(tmpDir, "entry2.duckdb")

	data1 := []byte("test data 1")
	data2 := []byte("test data 2 longer")

	if err := os.WriteFile(cacheFile1, data1, 0644); err != nil {
		t.Fatalf("failed to create cache file: %v", err)
	}
	if err := os.WriteFile(cacheFile2, data2, 0644); err != nil {
		t.Fatalf("failed to create cache file: %v", err)
	}

	count, size, err := handler.GetCacheStats()
	if err != nil {
		t.Fatalf("GetCacheStats failed: %v", err)
	}

	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}

	expectedSize := int64(len(data1) + len(data2))
	if size != expectedSize {
		t.Errorf("expected size %d, got %d", expectedSize, size)
	}
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
	}

	for _, tc := range tests {
		result := FormatSize(tc.bytes)
		if result != tc.expected {
			t.Errorf("FormatSize(%d): expected %s, got %s", tc.bytes, tc.expected, result)
		}
	}
}

func TestIsCacheValid_FileModified(t *testing.T) {
	tmpDir := t.TempDir()
	handler, _ := NewCacheHandler(tmpDir, true)

	// Create a temp file
	tmpFile := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(tmpFile, []byte("a,b,c\n1,2,3"), 0644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	// Generate cache key and save metadata
	cacheKey, _ := handler.GenerateCacheKey([]string{tmpFile})
	cacheFile := handler.GetCachePath(cacheKey)
	if err := os.WriteFile(cacheFile, []byte("cache"), 0644); err != nil {
		t.Fatalf("failed to create cache file: %v", err)
	}
	if err := handler.SaveMetadata(cacheKey, []string{tmpFile}, []string{"test"}, 1); err != nil {
		t.Fatalf("SaveMetadata failed: %v", err)
	}

	// Cache should be valid
	valid, _, _ := handler.IsCacheValid([]string{tmpFile})
	if !valid {
		t.Error("cache should be valid before file modification")
	}

	// Wait a bit and modify the file
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(tmpFile, []byte("a,b,c\n1,2,3\n4,5,6"), 0644); err != nil {
		t.Fatalf("failed to modify temp file: %v", err)
	}

	// Cache should now be invalid (different mod time means different key)
	valid, _, _ = handler.IsCacheValid([]string{tmpFile})
	if valid {
		t.Error("cache should be invalid after file modification")
	}
}
