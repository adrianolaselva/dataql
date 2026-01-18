package urlhandler

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// URLHandler handles downloading files from URLs
type URLHandler struct {
	client    *http.Client
	tempDir   string
	tempFiles []string
}

// NewURLHandler creates a new URL handler
func NewURLHandler() *URLHandler {
	return &URLHandler{
		client: &http.Client{
			Timeout: 5 * time.Minute, // 5 minute timeout for large files
		},
		tempFiles: make([]string, 0),
	}
}

// IsURL checks if a path is a URL
func IsURL(path string) bool {
	path = strings.TrimSpace(path)
	return strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://")
}

// ResolveFiles takes a list of file paths and resolves any URLs by downloading them
// Returns the list of local file paths (either original local paths or downloaded temp files)
func (h *URLHandler) ResolveFiles(filePaths []string) ([]string, error) {
	resolvedPaths := make([]string, 0, len(filePaths))

	for _, path := range filePaths {
		if IsURL(path) {
			localPath, err := h.downloadURL(path)
			if err != nil {
				return nil, fmt.Errorf("failed to download %s: %w", path, err)
			}
			resolvedPaths = append(resolvedPaths, localPath)
		} else {
			resolvedPaths = append(resolvedPaths, path)
		}
	}

	return resolvedPaths, nil
}

// downloadURL downloads a file from a URL and returns the local temp file path
func (h *URLHandler) downloadURL(urlStr string) (string, error) {
	// Parse the URL to extract the filename
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}

	// Get filename from URL path or generate one
	filename := filepath.Base(parsedURL.Path)
	if filename == "" || filename == "/" || filename == "." {
		filename = "downloaded_data"
	}

	// Ensure we have a temp directory
	if h.tempDir == "" {
		tempDir, err := os.MkdirTemp("", "dataql_downloads_")
		if err != nil {
			return "", fmt.Errorf("failed to create temp directory: %w", err)
		}
		h.tempDir = tempDir
	}

	// Create the local file path
	localPath := filepath.Join(h.tempDir, filename)

	// Download the file
	resp, err := h.client.Get(urlStr)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP error: status %d", resp.StatusCode)
	}

	// Create the local file
	outFile, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer outFile.Close()

	// Copy the content
	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to download file content: %w", err)
	}

	h.tempFiles = append(h.tempFiles, localPath)
	return localPath, nil
}

// Cleanup removes all downloaded temp files
func (h *URLHandler) Cleanup() error {
	if h.tempDir != "" {
		return os.RemoveAll(h.tempDir)
	}
	return nil
}

// GetTempFiles returns the list of downloaded temp files
func (h *URLHandler) GetTempFiles() []string {
	return h.tempFiles
}
