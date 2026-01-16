package azurehandler

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// AzureHandler handles downloading files from Azure Blob Storage
type AzureHandler struct {
	tempDir   string
	tempFiles []string
	client    *azblob.Client
}

// AzureLocation represents a parsed Azure Blob URL
type AzureLocation struct {
	AccountName   string
	ContainerName string
	BlobName      string
}

// azureURLRegex matches azure://container/blob format
var azureURLRegex = regexp.MustCompile(`^azure://([^/]+)/(.+)$`)

// azureBlobURLRegex matches https://<account>.blob.core.windows.net/<container>/<blob> format
var azureBlobURLRegex = regexp.MustCompile(`^https://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.+)$`)

// NewAzureHandler creates a new Azure handler
func NewAzureHandler() *AzureHandler {
	return &AzureHandler{}
}

// IsAzureURL checks if a string is an Azure Blob URL
func IsAzureURL(path string) bool {
	return strings.HasPrefix(path, "azure://") ||
		strings.Contains(path, ".blob.core.windows.net/")
}

// ParseAzureURL parses an Azure Blob URL into account, container and blob
func ParseAzureURL(azureURL string) (*AzureLocation, error) {
	// Try azure:// format first
	if strings.HasPrefix(azureURL, "azure://") {
		matches := azureURLRegex.FindStringSubmatch(azureURL)
		if matches == nil {
			return nil, fmt.Errorf("invalid Azure URL format: %s (expected azure://container/blob)", azureURL)
		}
		// For azure:// format, we need the account from environment
		return &AzureLocation{
			ContainerName: matches[1],
			BlobName:      matches[2],
		}, nil
	}

	// Try https://<account>.blob.core.windows.net/<container>/<blob> format
	matches := azureBlobURLRegex.FindStringSubmatch(azureURL)
	if matches == nil {
		return nil, fmt.Errorf("invalid Azure Blob URL format: %s (expected https://<account>.blob.core.windows.net/<container>/<blob>)", azureURL)
	}

	return &AzureLocation{
		AccountName:   matches[1],
		ContainerName: matches[2],
		BlobName:      matches[3],
	}, nil
}

// ResolveFiles resolves Azure Blob URLs to local temp files
// Returns the modified file paths with Azure URLs replaced by local paths
func (h *AzureHandler) ResolveFiles(filePaths []string) ([]string, error) {
	result := make([]string, 0, len(filePaths))

	for _, path := range filePaths {
		if IsAzureURL(path) {
			localPath, err := h.downloadAzureFile(path)
			if err != nil {
				return nil, fmt.Errorf("failed to download Azure file %s: %w", path, err)
			}
			result = append(result, localPath)
		} else {
			result = append(result, path)
		}
	}

	return result, nil
}

// downloadAzureFile downloads a file from Azure Blob Storage to a temp directory
func (h *AzureHandler) downloadAzureFile(azureURL string) (string, error) {
	// Parse the Azure URL
	loc, err := ParseAzureURL(azureURL)
	if err != nil {
		return "", err
	}

	// Initialize client if not already done
	if h.client == nil {
		if err := h.initClient(loc); err != nil {
			return "", fmt.Errorf("failed to initialize Azure client: %w", err)
		}
	}

	// Create temp directory if needed
	if h.tempDir == "" {
		tempDir, err := os.MkdirTemp("", "dataql-azure-*")
		if err != nil {
			return "", fmt.Errorf("failed to create temp directory: %w", err)
		}
		h.tempDir = tempDir
	}

	// Determine local file name from the blob path
	filename := filepath.Base(loc.BlobName)
	localPath := filepath.Join(h.tempDir, filename)

	// Download the blob
	ctx := context.Background()

	// Get blob client
	blobClient := h.client.ServiceClient().NewContainerClient(loc.ContainerName).NewBlobClient(loc.BlobName)

	// Download
	downloadResponse, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to download Azure blob: %w", err)
	}
	defer downloadResponse.Body.Close()

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// Copy content
	_, err = io.Copy(file, downloadResponse.Body)
	if err != nil {
		return "", fmt.Errorf("failed to write file content: %w", err)
	}

	h.tempFiles = append(h.tempFiles, localPath)
	return localPath, nil
}

// initClient initializes the Azure Blob client
func (h *AzureHandler) initClient(loc *AzureLocation) error {
	// Try connection string first (from environment)
	connStr := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	if connStr != "" {
		client, err := azblob.NewClientFromConnectionString(connStr, nil)
		if err != nil {
			return fmt.Errorf("failed to create Azure client from connection string: %w", err)
		}
		h.client = client
		return nil
	}

	// Try account name and key
	accountName := loc.AccountName
	if accountName == "" {
		accountName = os.Getenv("AZURE_STORAGE_ACCOUNT")
	}
	accountKey := os.Getenv("AZURE_STORAGE_KEY")

	if accountName != "" && accountKey != "" {
		cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return fmt.Errorf("failed to create Azure credentials: %w", err)
		}

		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
		client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
		if err != nil {
			return fmt.Errorf("failed to create Azure client: %w", err)
		}
		h.client = client
		return nil
	}

	return fmt.Errorf("Azure credentials not found. Set AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY")
}

// Cleanup removes all downloaded temp files
func (h *AzureHandler) Cleanup() error {
	if h.tempDir != "" {
		return os.RemoveAll(h.tempDir)
	}
	return nil
}
