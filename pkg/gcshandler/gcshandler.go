package gcshandler

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"cloud.google.com/go/storage"
)

// GCSHandler handles downloading files from Google Cloud Storage
type GCSHandler struct {
	tempDir   string
	tempFiles []string
	client    *storage.Client
}

// GCSLocation represents a parsed GCS URL
type GCSLocation struct {
	Bucket string
	Object string
}

// gcsURLRegex matches gs://bucket/object format
var gcsURLRegex = regexp.MustCompile(`^gs://([^/]+)/(.+)$`)

// NewGCSHandler creates a new GCS handler
func NewGCSHandler() *GCSHandler {
	return &GCSHandler{}
}

// IsGCSURL checks if a string is a GCS URL
func IsGCSURL(path string) bool {
	return strings.HasPrefix(path, "gs://")
}

// ParseGCSURL parses a GCS URL into bucket and object
func ParseGCSURL(gcsURL string) (*GCSLocation, error) {
	matches := gcsURLRegex.FindStringSubmatch(gcsURL)
	if matches == nil {
		return nil, fmt.Errorf("invalid GCS URL format: %s (expected gs://bucket/object)", gcsURL)
	}

	return &GCSLocation{
		Bucket: matches[1],
		Object: matches[2],
	}, nil
}

// ResolveFiles resolves GCS URLs to local temp files
// Returns the modified file paths with GCS URLs replaced by local paths
func (h *GCSHandler) ResolveFiles(filePaths []string) ([]string, error) {
	result := make([]string, 0, len(filePaths))

	for _, path := range filePaths {
		if IsGCSURL(path) {
			localPath, err := h.downloadGCSFile(path)
			if err != nil {
				return nil, fmt.Errorf("failed to download GCS file %s: %w", path, err)
			}
			result = append(result, localPath)
		} else {
			result = append(result, path)
		}
	}

	return result, nil
}

// downloadGCSFile downloads a file from GCS to a temp directory
func (h *GCSHandler) downloadGCSFile(gcsURL string) (string, error) {
	// Parse the GCS URL
	loc, err := ParseGCSURL(gcsURL)
	if err != nil {
		return "", err
	}

	// Initialize client if not already done
	if h.client == nil {
		if err := h.initClient(); err != nil {
			return "", fmt.Errorf("failed to initialize GCS client: %w", err)
		}
	}

	// Create temp directory if needed
	if h.tempDir == "" {
		tempDir, err := os.MkdirTemp("", "dataql-gcs-*")
		if err != nil {
			return "", fmt.Errorf("failed to create temp directory: %w", err)
		}
		h.tempDir = tempDir
	}

	// Determine local file name from the object path
	filename := filepath.Base(loc.Object)
	localPath := filepath.Join(h.tempDir, filename)

	// Download the file
	ctx := context.Background()
	bucket := h.client.Bucket(loc.Bucket)
	obj := bucket.Object(loc.Object)

	reader, err := obj.NewReader(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get GCS object: %w", err)
	}
	defer reader.Close()

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// Copy content
	_, err = io.Copy(file, reader)
	if err != nil {
		return "", fmt.Errorf("failed to write file content: %w", err)
	}

	h.tempFiles = append(h.tempFiles, localPath)
	return localPath, nil
}

// initClient initializes the GCS client using default credentials
func (h *GCSHandler) initClient() error {
	ctx := context.Background()

	// Create GCS client using Application Default Credentials
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}

	h.client = client
	return nil
}

// Cleanup removes all downloaded temp files
func (h *GCSHandler) Cleanup() error {
	if h.client != nil {
		h.client.Close()
	}
	if h.tempDir != "" {
		return os.RemoveAll(h.tempDir)
	}
	return nil
}
