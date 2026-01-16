package stdinhandler

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// StdinHandler handles reading data from stdin
type StdinHandler struct {
	tempDir   string
	tempFiles []string
}

// NewStdinHandler creates a new stdin handler
func NewStdinHandler() *StdinHandler {
	return &StdinHandler{
		tempFiles: make([]string, 0),
	}
}

// IsStdinInput checks if the input is stdin (represented by "-")
func IsStdinInput(path string) bool {
	return strings.TrimSpace(path) == "-"
}

// ResolveFiles takes a list of file paths and resolves any stdin inputs
// by reading from stdin and writing to a temp file
func (h *StdinHandler) ResolveFiles(filePaths []string, format string) ([]string, error) {
	resolvedPaths := make([]string, 0, len(filePaths))

	for _, path := range filePaths {
		if IsStdinInput(path) {
			localPath, err := h.readStdin(format)
			if err != nil {
				return nil, fmt.Errorf("failed to read from stdin: %w", err)
			}
			resolvedPaths = append(resolvedPaths, localPath)
		} else {
			resolvedPaths = append(resolvedPaths, path)
		}
	}

	return resolvedPaths, nil
}

// readStdin reads all data from stdin and writes it to a temp file
func (h *StdinHandler) readStdin(format string) (string, error) {
	// Determine file extension based on format
	ext := ".csv" // default
	switch strings.ToLower(format) {
	case "json":
		ext = ".json"
	case "jsonl", "ndjson":
		ext = ".jsonl"
	case "xml":
		ext = ".xml"
	case "yaml", "yml":
		ext = ".yaml"
	case "parquet":
		ext = ".parquet"
	case "avro":
		ext = ".avro"
	case "orc":
		ext = ".orc"
	}

	// Ensure we have a temp directory
	if h.tempDir == "" {
		tempDir, err := os.MkdirTemp("", "dataql_stdin_")
		if err != nil {
			return "", fmt.Errorf("failed to create temp directory: %w", err)
		}
		h.tempDir = tempDir
	}

	// Create temp file with appropriate extension
	localPath := filepath.Join(h.tempDir, "stdin_data"+ext)

	// Create the temp file
	outFile, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer outFile.Close()

	// Read from stdin and write to temp file
	reader := bufio.NewReader(os.Stdin)
	_, err = io.Copy(outFile, reader)
	if err != nil {
		return "", fmt.Errorf("failed to read stdin: %w", err)
	}

	h.tempFiles = append(h.tempFiles, localPath)
	return localPath, nil
}

// Cleanup removes all temp files created from stdin
func (h *StdinHandler) Cleanup() error {
	if h.tempDir != "" {
		return os.RemoveAll(h.tempDir)
	}
	return nil
}

// GetTempFiles returns the list of temp files created from stdin
func (h *StdinHandler) GetTempFiles() []string {
	return h.tempFiles
}
