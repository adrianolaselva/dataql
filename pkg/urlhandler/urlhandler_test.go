package urlhandler_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/adrianolaselva/dataql/pkg/urlhandler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsURL(t *testing.T) {
	assert.True(t, urlhandler.IsURL("http://example.com/data.csv"))
	assert.True(t, urlhandler.IsURL("https://example.com/data.csv"))
	assert.True(t, urlhandler.IsURL("  https://example.com/data.csv  "))
	assert.False(t, urlhandler.IsURL("ftp://example.com/data.csv"))
	assert.False(t, urlhandler.IsURL("/local/path.csv"))
	assert.False(t, urlhandler.IsURL("data.csv"))
	assert.False(t, urlhandler.IsURL(""))
}

func TestNewURLHandler(t *testing.T) {
	h := urlhandler.NewURLHandler()
	require.NotNil(t, h)
	assert.Empty(t, h.GetTempFiles())
}

func TestResolveFiles_PassesThroughNonURLPaths(t *testing.T) {
	h := urlhandler.NewURLHandler()
	defer func() { _ = h.Cleanup() }()

	input := []string{"a.csv", "/abs/b.json"}
	resolved, err := h.ResolveFiles(input)
	require.NoError(t, err)

	assert.Equal(t, input, resolved)
	assert.Empty(t, h.GetTempFiles())
}

func TestResolveFiles_DownloadsCSVSuccessfully(t *testing.T) {
	csvBody := "id,name\n1,Alice\n2,Bob\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/data.csv", r.URL.Path)
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte(csvBody))
	}))
	defer server.Close()

	h := urlhandler.NewURLHandler()
	defer func() { _ = h.Cleanup() }()

	resolved, err := h.ResolveFiles([]string{server.URL + "/data.csv"})
	require.NoError(t, err)
	require.Len(t, resolved, 1)

	// Filename is derived from the URL path.
	assert.Equal(t, "data.csv", filepath.Base(resolved[0]))

	tempFiles := h.GetTempFiles()
	require.Len(t, tempFiles, 1)
	assert.Equal(t, resolved[0], tempFiles[0])

	content, readErr := os.ReadFile(resolved[0])
	require.NoError(t, readErr)
	assert.Equal(t, csvBody, string(content))
}

func TestResolveFiles_MixesURLAndLocalPaths(t *testing.T) {
	body := "a,b\n1,2\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	h := urlhandler.NewURLHandler()
	defer func() { _ = h.Cleanup() }()

	resolved, err := h.ResolveFiles([]string{"local.csv", server.URL + "/remote.csv"})
	require.NoError(t, err)
	require.Len(t, resolved, 2)

	assert.Equal(t, "local.csv", resolved[0])
	assert.Equal(t, "remote.csv", filepath.Base(resolved[1]))

	content, readErr := os.ReadFile(resolved[1])
	require.NoError(t, readErr)
	assert.Equal(t, body, string(content))
}

func TestResolveFiles_DownloadWithQueryString(t *testing.T) {
	body := "col\nval\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "token", r.URL.Query().Get("auth"))
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	h := urlhandler.NewURLHandler()
	defer func() { _ = h.Cleanup() }()

	resolved, err := h.ResolveFiles([]string{server.URL + "/export.csv?auth=token"})
	require.NoError(t, err)
	require.Len(t, resolved, 1)

	// Query string should not be part of the local filename.
	assert.Equal(t, "export.csv", filepath.Base(resolved[0]))

	content, readErr := os.ReadFile(resolved[0])
	require.NoError(t, readErr)
	assert.Equal(t, body, string(content))
}

func TestResolveFiles_NoFilenameInPathUsesFallback(t *testing.T) {
	body := "data"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	h := urlhandler.NewURLHandler()
	defer func() { _ = h.Cleanup() }()

	resolved, err := h.ResolveFiles([]string{server.URL})
	require.NoError(t, err)
	require.Len(t, resolved, 1)

	assert.Equal(t, "downloaded_data", filepath.Base(resolved[0]))
}

func TestResolveFiles_ReturnsErrorOn404(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	h := urlhandler.NewURLHandler()
	defer func() { _ = h.Cleanup() }()

	resolved, err := h.ResolveFiles([]string{server.URL + "/missing.csv"})
	require.Error(t, err)
	assert.Nil(t, resolved)
	assert.Contains(t, err.Error(), "status 404")
}

func TestResolveFiles_ReturnsErrorOnServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer server.Close()

	h := urlhandler.NewURLHandler()
	defer func() { _ = h.Cleanup() }()

	_, err := h.ResolveFiles([]string{server.URL + "/data.csv"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestResolveFiles_ReturnsErrorWhenServerUnreachable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := server.URL + "/data.csv"
	server.Close() // close immediately so the connection fails

	h := urlhandler.NewURLHandler()
	defer func() { _ = h.Cleanup() }()

	_, err := h.ResolveFiles([]string{url})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to download")
}

func TestCleanup_RemovesDownloadedFiles(t *testing.T) {
	body := "x,y\n1,2\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	h := urlhandler.NewURLHandler()

	resolved, err := h.ResolveFiles([]string{server.URL + "/data.csv"})
	require.NoError(t, err)
	require.Len(t, resolved, 1)

	_, statErr := os.Stat(resolved[0])
	require.NoError(t, statErr)

	require.NoError(t, h.Cleanup())

	_, statErr = os.Stat(resolved[0])
	assert.True(t, os.IsNotExist(statErr))
}

func TestCleanup_NoTempDirIsNoOp(t *testing.T) {
	h := urlhandler.NewURLHandler()
	assert.NoError(t, h.Cleanup())
}
