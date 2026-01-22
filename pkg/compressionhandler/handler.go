package compressionhandler

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ulikunitz/xz"
)

// Compression represents a supported compression format
type Compression string

const (
	CompressionNone  Compression = ""
	CompressionGzip  Compression = "gzip"
	CompressionBzip2 Compression = "bzip2"
	CompressionXZ    Compression = "xz"
	CompressionZstd  Compression = "zstd"
)

// compressionExtensions maps file extensions to compression types
var compressionExtensions = map[string]Compression{
	".gz":   CompressionGzip,
	".gzip": CompressionGzip,
	".bz2":  CompressionBzip2,
	".xz":   CompressionXZ,
	".zst":  CompressionZstd,
	".zstd": CompressionZstd,
}

// CompressionHandler handles decompression of compressed files
type CompressionHandler struct {
	tempFiles     []string
	originalPaths map[string]string // maps decompressed path -> original path
}

// NewCompressionHandler creates a new compression handler
func NewCompressionHandler() *CompressionHandler {
	return &CompressionHandler{
		tempFiles:     make([]string, 0),
		originalPaths: make(map[string]string),
	}
}

// GetOriginalPath returns the original path for a decompressed file path
// If the path was not decompressed, returns the same path
func (h *CompressionHandler) GetOriginalPath(decompressedPath string) string {
	if original, ok := h.originalPaths[decompressedPath]; ok {
		return original
	}
	return decompressedPath
}

// GetPathMapping returns a map of decompressed path -> original path
// Only includes entries for files that were actually decompressed
func (h *CompressionHandler) GetPathMapping() map[string]string {
	result := make(map[string]string)
	for k, v := range h.originalPaths {
		result[k] = v
	}
	return result
}

// DetectCompression detects if a file is compressed and returns the compression type
func DetectCompression(filePath string) Compression {
	ext := strings.ToLower(filepath.Ext(filePath))
	if compression, ok := compressionExtensions[ext]; ok {
		return compression
	}
	return CompressionNone
}

// IsCompressed checks if a file path has a compression extension
func IsCompressed(filePath string) bool {
	return DetectCompression(filePath) != CompressionNone
}

// GetUncompressedPath returns the path without the compression extension
// e.g., "data.csv.gz" -> "data.csv"
func GetUncompressedPath(filePath string) string {
	compression := DetectCompression(filePath)
	if compression == CompressionNone {
		return filePath
	}
	// Remove the compression extension
	return strings.TrimSuffix(filePath, filepath.Ext(filePath))
}

// GetInnerExtension returns the extension of the uncompressed file
// e.g., "data.csv.gz" -> ".csv"
func GetInnerExtension(filePath string) string {
	uncompressedPath := GetUncompressedPath(filePath)
	return filepath.Ext(uncompressedPath)
}

// ResolveFiles decompresses any compressed files and returns paths to the decompressed versions
// Non-compressed files are returned as-is
func (h *CompressionHandler) ResolveFiles(files []string) ([]string, error) {
	result := make([]string, len(files))

	for i, file := range files {
		if IsCompressed(file) {
			decompressedPath, err := h.decompressFile(file)
			if err != nil {
				return nil, fmt.Errorf("failed to decompress %s: %w", file, err)
			}
			result[i] = decompressedPath
		} else {
			result[i] = file
		}
	}

	return result, nil
}

// decompressFile decompresses a single file to a temporary file
func (h *CompressionHandler) decompressFile(filePath string) (string, error) {
	compression := DetectCompression(filePath)
	if compression == CompressionNone {
		return filePath, nil
	}

	// Open the compressed file
	inputFile, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open compressed file: %w", err)
	}
	defer inputFile.Close()

	// Create a temp file with the inner extension
	innerExt := GetInnerExtension(filePath)
	tempFile, err := os.CreateTemp("", "dataql_decompressed_*"+innerExt)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	h.tempFiles = append(h.tempFiles, tempPath)

	// Create appropriate decompressor
	var reader io.Reader
	switch compression {
	case CompressionGzip:
		gzReader, err := gzip.NewReader(inputFile)
		if err != nil {
			tempFile.Close()
			return "", fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	case CompressionBzip2:
		reader = bzip2.NewReader(inputFile)
	case CompressionXZ:
		xzReader, err := xz.NewReader(inputFile)
		if err != nil {
			tempFile.Close()
			return "", fmt.Errorf("failed to create xz reader: %w", err)
		}
		reader = xzReader
	case CompressionZstd:
		tempFile.Close()
		return "", fmt.Errorf("zstd compression not yet supported (install the klauspost/compress library for zstd support)")
	default:
		tempFile.Close()
		return "", fmt.Errorf("unsupported compression: %s", compression)
	}

	// Copy decompressed data to temp file
	if _, err := io.Copy(tempFile, reader); err != nil {
		tempFile.Close()
		return "", fmt.Errorf("failed to decompress file: %w", err)
	}

	tempFile.Close()

	// Track the mapping from decompressed path to original path
	h.originalPaths[tempPath] = filePath

	return tempPath, nil
}

// Cleanup removes all temporary decompressed files
func (h *CompressionHandler) Cleanup() error {
	for _, path := range h.tempFiles {
		os.Remove(path)
	}
	h.tempFiles = nil
	return nil
}

// SupportedCompressions returns a list of supported compression formats
func SupportedCompressions() []string {
	return []string{"gzip (.gz)", "bzip2 (.bz2)", "xz (.xz)"}
}
