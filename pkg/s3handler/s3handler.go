package s3handler

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Handler handles downloading files from S3
type S3Handler struct {
	tempDir   string
	tempFiles []string
	client    *s3.Client
}

// S3Location represents a parsed S3 URL
type S3Location struct {
	Bucket string
	Key    string
	Region string
}

// s3URLRegex matches s3://bucket/key format
var s3URLRegex = regexp.MustCompile(`^s3://([^/]+)/(.+)$`)

// NewS3Handler creates a new S3 handler
func NewS3Handler() *S3Handler {
	return &S3Handler{}
}

// IsS3URL checks if a string is an S3 URL
func IsS3URL(path string) bool {
	return strings.HasPrefix(path, "s3://")
}

// ParseS3URL parses an S3 URL into bucket and key
func ParseS3URL(s3URL string) (*S3Location, error) {
	matches := s3URLRegex.FindStringSubmatch(s3URL)
	if matches == nil {
		return nil, fmt.Errorf("invalid S3 URL format: %s (expected s3://bucket/key)", s3URL)
	}

	return &S3Location{
		Bucket: matches[1],
		Key:    matches[2],
	}, nil
}

// ResolveFiles resolves S3 URLs to local temp files
// Returns the modified file paths with S3 URLs replaced by local paths
func (h *S3Handler) ResolveFiles(filePaths []string) ([]string, error) {
	result := make([]string, 0, len(filePaths))

	for _, path := range filePaths {
		if IsS3URL(path) {
			localPath, err := h.downloadS3File(path)
			if err != nil {
				return nil, fmt.Errorf("failed to download S3 file %s: %w", path, err)
			}
			result = append(result, localPath)
		} else {
			result = append(result, path)
		}
	}

	return result, nil
}

// downloadS3File downloads a file from S3 to a temp directory
func (h *S3Handler) downloadS3File(s3URL string) (string, error) {
	// Parse the S3 URL
	loc, err := ParseS3URL(s3URL)
	if err != nil {
		return "", err
	}

	// Initialize client if not already done
	if h.client == nil {
		if err := h.initClient(); err != nil {
			return "", fmt.Errorf("failed to initialize S3 client: %w", err)
		}
	}

	// Create temp directory if needed
	if h.tempDir == "" {
		tempDir, err := os.MkdirTemp("", "dataql-s3-*")
		if err != nil {
			return "", fmt.Errorf("failed to create temp directory: %w", err)
		}
		h.tempDir = tempDir
	}

	// Determine local file name from the key
	filename := filepath.Base(loc.Key)
	localPath := filepath.Join(h.tempDir, filename)

	// Download the file
	ctx := context.Background()
	resp, err := h.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &loc.Bucket,
		Key:    &loc.Key,
	})
	if err != nil {
		return "", fmt.Errorf("failed to get S3 object: %w", err)
	}
	defer resp.Body.Close()

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// Copy content
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to write file content: %w", err)
	}

	h.tempFiles = append(h.tempFiles, localPath)
	return localPath, nil
}

// initClient initializes the S3 client using default AWS credentials
func (h *S3Handler) initClient() error {
	ctx := context.Background()

	// Load AWS configuration from environment, shared config, etc.
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	h.client = s3.NewFromConfig(cfg)
	return nil
}

// Cleanup removes all downloaded temp files
func (h *S3Handler) Cleanup() error {
	if h.tempDir != "" {
		return os.RemoveAll(h.tempDir)
	}
	return nil
}
