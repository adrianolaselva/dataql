package cachehandler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// CacheHandler manages data caching for files
type CacheHandler struct {
	cacheDir string
	enabled  bool
}

// CacheMetadata stores information about a cached file
type CacheMetadata struct {
	SourceFiles   []string  `json:"source_files"`
	ModTimes      []int64   `json:"mod_times"`
	CachedAt      time.Time `json:"cached_at"`
	CacheFile     string    `json:"cache_file"`
	TotalRows     int64     `json:"total_rows"`
	Tables        []string  `json:"tables"`
	FileHash      string    `json:"file_hash"`      // Hash of file paths + mod times
	FormatVersion int       `json:"format_version"` // For cache format compatibility
}

const (
	cacheFormatVersion = 1
	metadataFileName   = "metadata.json"
)

// NewCacheHandler creates a new cache handler
func NewCacheHandler(cacheDir string, enabled bool) (*CacheHandler, error) {
	if !enabled {
		return &CacheHandler{enabled: false}, nil
	}

	// Use default cache directory if not specified
	if cacheDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get home directory: %w", err)
		}
		cacheDir = filepath.Join(homeDir, ".dataql", "cache")
	}

	// Create cache directory if it doesn't exist
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	return &CacheHandler{
		cacheDir: cacheDir,
		enabled:  true,
	}, nil
}

// IsEnabled returns whether caching is enabled
func (h *CacheHandler) IsEnabled() bool {
	return h.enabled
}

// GetCacheDir returns the cache directory path
func (h *CacheHandler) GetCacheDir() string {
	return h.cacheDir
}

// GenerateCacheKey creates a unique cache key based on file paths and modification times
func (h *CacheHandler) GenerateCacheKey(files []string) (string, error) {
	if !h.enabled {
		return "", nil
	}

	// Sort files for consistent hashing
	sortedFiles := make([]string, len(files))
	copy(sortedFiles, files)
	sort.Strings(sortedFiles)

	var keyParts []string
	for _, file := range sortedFiles {
		absPath, err := filepath.Abs(file)
		if err != nil {
			return "", fmt.Errorf("failed to get absolute path: %w", err)
		}

		info, err := os.Stat(file)
		if err != nil {
			return "", fmt.Errorf("failed to stat file: %w", err)
		}

		keyParts = append(keyParts, fmt.Sprintf("%s:%d", absPath, info.ModTime().UnixNano()))
	}

	// Create hash of all file paths and mod times
	hash := sha256.Sum256([]byte(strings.Join(keyParts, "|")))
	return hex.EncodeToString(hash[:16]), nil // Use first 16 bytes for shorter key
}

// GetCachePath returns the path to the cache database for given files
func (h *CacheHandler) GetCachePath(cacheKey string) string {
	if !h.enabled || cacheKey == "" {
		return ""
	}
	return filepath.Join(h.cacheDir, cacheKey+".duckdb")
}

// GetMetadataPath returns the path to the metadata file for given cache key
func (h *CacheHandler) GetMetadataPath(cacheKey string) string {
	if !h.enabled || cacheKey == "" {
		return ""
	}
	return filepath.Join(h.cacheDir, cacheKey+".json")
}

// IsCacheValid checks if a valid cache exists for the given files
func (h *CacheHandler) IsCacheValid(files []string) (bool, string, error) {
	if !h.enabled {
		return false, "", nil
	}

	cacheKey, err := h.GenerateCacheKey(files)
	if err != nil {
		return false, "", err
	}

	cachePath := h.GetCachePath(cacheKey)
	metadataPath := h.GetMetadataPath(cacheKey)

	// Check if cache file exists
	if _, err := os.Stat(cachePath); os.IsNotExist(err) {
		return false, "", nil
	}

	// Check if metadata file exists
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		return false, "", nil
	}

	// Read and validate metadata
	metadata, err := h.ReadMetadata(cacheKey)
	if err != nil {
		// Treat as cache miss if metadata is invalid - not an error condition
		// nolint:nilerr // intentionally ignoring error to treat as cache miss
		return false, "", nil
	}

	// Check format version
	if metadata.FormatVersion != cacheFormatVersion {
		return false, "", nil
	}

	// Validate source files still match
	if !h.validateSourceFiles(files, metadata) {
		return false, "", nil
	}

	return true, cachePath, nil
}

// validateSourceFiles checks if source files match the cached metadata
func (h *CacheHandler) validateSourceFiles(files []string, metadata *CacheMetadata) bool {
	if len(files) != len(metadata.SourceFiles) {
		return false
	}

	// Create map of expected file -> modtime
	expected := make(map[string]int64)
	for i, file := range metadata.SourceFiles {
		expected[file] = metadata.ModTimes[i]
	}

	// Check each file
	for _, file := range files {
		absPath, err := filepath.Abs(file)
		if err != nil {
			return false
		}

		modTime, exists := expected[absPath]
		if !exists {
			return false
		}

		info, err := os.Stat(file)
		if err != nil {
			return false
		}

		if info.ModTime().UnixNano() != modTime {
			return false
		}
	}

	return true
}

// SaveMetadata saves cache metadata
func (h *CacheHandler) SaveMetadata(cacheKey string, files []string, tables []string, totalRows int64) error {
	if !h.enabled {
		return nil
	}

	// Get absolute paths and mod times
	var absPaths []string
	var modTimes []int64
	for _, file := range files {
		absPath, err := filepath.Abs(file)
		if err != nil {
			return fmt.Errorf("failed to get absolute path: %w", err)
		}
		absPaths = append(absPaths, absPath)

		info, err := os.Stat(file)
		if err != nil {
			return fmt.Errorf("failed to stat file: %w", err)
		}
		modTimes = append(modTimes, info.ModTime().UnixNano())
	}

	metadata := CacheMetadata{
		SourceFiles:   absPaths,
		ModTimes:      modTimes,
		CachedAt:      time.Now(),
		CacheFile:     h.GetCachePath(cacheKey),
		TotalRows:     totalRows,
		Tables:        tables,
		FileHash:      cacheKey,
		FormatVersion: cacheFormatVersion,
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metadataPath := h.GetMetadataPath(cacheKey)
	if err := os.WriteFile(metadataPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// ReadMetadata reads cache metadata
func (h *CacheHandler) ReadMetadata(cacheKey string) (*CacheMetadata, error) {
	if !h.enabled {
		return nil, fmt.Errorf("cache not enabled")
	}

	metadataPath := h.GetMetadataPath(cacheKey)
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata CacheMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &metadata, nil
}

// CacheEntry represents a cache entry for listing
type CacheEntry struct {
	CacheKey    string
	SourceFiles []string
	CachedAt    time.Time
	TotalRows   int64
	Tables      []string
	SizeBytes   int64
}

// ListCache returns all cache entries
func (h *CacheHandler) ListCache() ([]CacheEntry, error) {
	if !h.enabled {
		return nil, fmt.Errorf("cache not enabled")
	}

	entries, err := os.ReadDir(h.cacheDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read cache directory: %w", err)
	}

	var cacheEntries []CacheEntry
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		cacheKey := strings.TrimSuffix(entry.Name(), ".json")
		metadata, err := h.ReadMetadata(cacheKey)
		if err != nil {
			continue // Skip invalid entries
		}

		// Get cache file size
		var sizeBytes int64
		cachePath := h.GetCachePath(cacheKey)
		if info, err := os.Stat(cachePath); err == nil {
			sizeBytes = info.Size()
		}

		cacheEntries = append(cacheEntries, CacheEntry{
			CacheKey:    cacheKey,
			SourceFiles: metadata.SourceFiles,
			CachedAt:    metadata.CachedAt,
			TotalRows:   metadata.TotalRows,
			Tables:      metadata.Tables,
			SizeBytes:   sizeBytes,
		})
	}

	// Sort by cached time, newest first
	sort.Slice(cacheEntries, func(i, j int) bool {
		return cacheEntries[i].CachedAt.After(cacheEntries[j].CachedAt)
	})

	return cacheEntries, nil
}

// ClearCache removes all cache files
func (h *CacheHandler) ClearCache() error {
	if !h.enabled {
		return fmt.Errorf("cache not enabled")
	}

	entries, err := os.ReadDir(h.cacheDir)
	if err != nil {
		return fmt.Errorf("failed to read cache directory: %w", err)
	}

	var cleared int
	for _, entry := range entries {
		path := filepath.Join(h.cacheDir, entry.Name())
		if err := os.Remove(path); err != nil {
			// Continue on error, but log it
			fmt.Fprintf(os.Stderr, "Warning: failed to remove %s: %v\n", path, err)
		} else {
			cleared++
		}
	}

	fmt.Printf("Cleared %d cache files\n", cleared)
	return nil
}

// ClearCacheEntry removes a specific cache entry
func (h *CacheHandler) ClearCacheEntry(cacheKey string) error {
	if !h.enabled {
		return fmt.Errorf("cache not enabled")
	}

	cachePath := h.GetCachePath(cacheKey)
	metadataPath := h.GetMetadataPath(cacheKey)

	// Remove cache file
	if err := os.Remove(cachePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove cache file: %w", err)
	}

	// Remove metadata file
	if err := os.Remove(metadataPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove metadata file: %w", err)
	}

	return nil
}

// GetCacheStats returns cache statistics
func (h *CacheHandler) GetCacheStats() (int, int64, error) {
	if !h.enabled {
		return 0, 0, fmt.Errorf("cache not enabled")
	}

	entries, err := os.ReadDir(h.cacheDir)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read cache directory: %w", err)
	}

	var count int
	var totalSize int64
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".duckdb") {
			count++
			info, err := entry.Info()
			if err == nil {
				totalSize += info.Size()
			}
		}
	}

	return count, totalSize, nil
}

// FormatSize formats bytes to human-readable size
func FormatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
