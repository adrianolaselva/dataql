package azurehandler_test

import (
	"testing"

	"github.com/adrianolaselva/dataql/pkg/azurehandler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsAzureURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "azure scheme url", path: "azure://container/blob.csv", want: true},
		{name: "azure scheme nested", path: "azure://container/a/b/c.csv", want: true},
		{name: "blob https url", path: "https://acct.blob.core.windows.net/container/blob.csv", want: true},
		{name: "blob https nested", path: "https://acct.blob.core.windows.net/container/a/b.csv", want: true},
		{name: "local absolute path", path: "/tmp/data.csv", want: false},
		{name: "local relative path", path: "data/file.csv", want: false},
		{name: "plain http other host", path: "http://example.com/file.csv", want: false},
		{name: "plain https other host", path: "https://example.com/file.csv", want: false},
		{name: "gcs url", path: "gs://bucket/object.csv", want: false},
		{name: "s3 url", path: "s3://bucket/object.csv", want: false},
		{name: "empty string", path: "", want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, azurehandler.IsAzureURL(tt.path))
		})
	}
}

func TestParseAzureURL_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		azureURL      string
		wantAccount   string
		wantContainer string
		wantBlob      string
	}{
		{
			name:          "azure scheme simple",
			azureURL:      "azure://my-container/file.csv",
			wantAccount:   "",
			wantContainer: "my-container",
			wantBlob:      "file.csv",
		},
		{
			name:          "azure scheme nested blob",
			azureURL:      "azure://my-container/a/b/c.csv",
			wantAccount:   "",
			wantContainer: "my-container",
			wantBlob:      "a/b/c.csv",
		},
		{
			name:          "azure scheme trailing slash",
			azureURL:      "azure://my-container/folder/",
			wantAccount:   "",
			wantContainer: "my-container",
			wantBlob:      "folder/",
		},
		{
			name:          "blob https simple",
			azureURL:      "https://myaccount.blob.core.windows.net/my-container/file.csv",
			wantAccount:   "myaccount",
			wantContainer: "my-container",
			wantBlob:      "file.csv",
		},
		{
			name:          "blob https nested blob",
			azureURL:      "https://myaccount.blob.core.windows.net/my-container/a/b/c.parquet",
			wantAccount:   "myaccount",
			wantContainer: "my-container",
			wantBlob:      "a/b/c.parquet",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			loc, err := azurehandler.ParseAzureURL(tt.azureURL)
			require.NoError(t, err)
			require.NotNil(t, loc)
			assert.Equal(t, tt.wantAccount, loc.AccountName)
			assert.Equal(t, tt.wantContainer, loc.ContainerName)
			assert.Equal(t, tt.wantBlob, loc.BlobName)
		})
	}
}

func TestParseAzureURL_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		azureURL    string
		wantErrPart string
	}{
		{name: "empty string", azureURL: "", wantErrPart: "invalid Azure Blob URL format"},
		{name: "azure scheme missing blob", azureURL: "azure://container", wantErrPart: "invalid Azure URL format"},
		{name: "azure scheme only", azureURL: "azure://", wantErrPart: "invalid Azure URL format"},
		{name: "azure scheme container slash only", azureURL: "azure://container/", wantErrPart: "invalid Azure URL format"},
		{name: "https wrong host", azureURL: "https://example.com/container/blob.csv", wantErrPart: "invalid Azure Blob URL format"},
		{name: "blob host missing blob", azureURL: "https://acct.blob.core.windows.net/container", wantErrPart: "invalid Azure Blob URL format"},
		{name: "local path", azureURL: "/tmp/file.csv", wantErrPart: "invalid Azure Blob URL format"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			loc, err := azurehandler.ParseAzureURL(tt.azureURL)
			require.Error(t, err)
			assert.Nil(t, loc)
			assert.Contains(t, err.Error(), tt.wantErrPart)
		})
	}
}

func TestNewAzureHandler(t *testing.T) {
	t.Parallel()
	h := azurehandler.NewAzureHandler()
	require.NotNil(t, h)
}

func TestResolveFiles_NoAzureURLsPassThrough(t *testing.T) {
	t.Parallel()

	h := azurehandler.NewAzureHandler()
	input := []string{"/tmp/local.csv", "data/file.parquet", "gs://bucket/x.csv"}

	result, err := h.ResolveFiles(input)
	require.NoError(t, err)
	assert.Equal(t, input, result)
}

func TestResolveFiles_EmptyInput(t *testing.T) {
	t.Parallel()

	h := azurehandler.NewAzureHandler()
	result, err := h.ResolveFiles(nil)
	require.NoError(t, err)
	assert.Empty(t, result)
}

// TestResolveFiles_MissingCredentials exercises the credentials error path.
// With a parsable Azure URL but no credentials configured, client init must
// fail with the "azure credentials not found" error before any network call.
func TestResolveFiles_MissingCredentials(t *testing.T) {
	// Not parallel: mutates process environment via t.Setenv.
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")
	t.Setenv("AZURE_STORAGE_ACCOUNT", "")
	t.Setenv("AZURE_STORAGE_KEY", "")

	h := azurehandler.NewAzureHandler()
	// azure:// form has no account name, so account/key path cannot be satisfied.
	result, err := h.ResolveFiles([]string{"azure://container/blob.csv"})
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "azure credentials not found")
}

func TestCleanup_NoTempDir(t *testing.T) {
	t.Parallel()

	h := azurehandler.NewAzureHandler()
	assert.NoError(t, h.Cleanup())
}
