package stdinhandler_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/adrianolaselva/dataql/pkg/stdinhandler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withStdin replaces os.Stdin with a pipe that yields the provided input,
// runs fn, and restores the original os.Stdin afterwards.
func withStdin(t *testing.T, input string, fn func()) {
	t.Helper()

	orig := os.Stdin
	r, w, err := os.Pipe()
	require.NoError(t, err)

	os.Stdin = r
	defer func() {
		os.Stdin = orig
		_ = r.Close()
	}()

	go func() {
		_, _ = w.Write([]byte(input))
		_ = w.Close()
	}()

	fn()
}

func TestIsStdinInput(t *testing.T) {
	assert.True(t, stdinhandler.IsStdinInput("-"))
	assert.True(t, stdinhandler.IsStdinInput("  -  "))
	assert.False(t, stdinhandler.IsStdinInput("file.csv"))
	assert.False(t, stdinhandler.IsStdinInput(""))
	assert.False(t, stdinhandler.IsStdinInput("--"))
}

func TestNewStdinHandler(t *testing.T) {
	h := stdinhandler.NewStdinHandler()
	require.NotNil(t, h)
	assert.Empty(t, h.GetTempFiles())
}

func TestResolveFiles_PassesThroughNonStdinPaths(t *testing.T) {
	h := stdinhandler.NewStdinHandler()
	defer func() { _ = h.Cleanup() }()

	input := []string{"a.csv", "b.json", "/abs/path.csv"}
	resolved, err := h.ResolveFiles(input, "csv")
	require.NoError(t, err)

	assert.Equal(t, input, resolved)
	// No stdin entries means no temp files created.
	assert.Empty(t, h.GetTempFiles())
}

func TestResolveFiles_ReadsCSVFromStdin(t *testing.T) {
	csvData := "id,name,email\n1,John,john@example.com\n2,Jane,jane@example.com\n"

	h := stdinhandler.NewStdinHandler()
	defer func() { _ = h.Cleanup() }()

	var resolved []string
	var err error
	withStdin(t, csvData, func() {
		resolved, err = h.ResolveFiles([]string{"-"}, "csv")
	})
	require.NoError(t, err)
	require.Len(t, resolved, 1)

	// Resolved path should be a temp file with a .csv extension.
	assert.Equal(t, ".csv", filepath.Ext(resolved[0]))

	tempFiles := h.GetTempFiles()
	require.Len(t, tempFiles, 1)
	assert.Equal(t, resolved[0], tempFiles[0])

	content, readErr := os.ReadFile(resolved[0])
	require.NoError(t, readErr)
	assert.Equal(t, csvData, string(content))
}

func TestResolveFiles_MixesStdinAndRegularPaths(t *testing.T) {
	csvData := "a,b\n1,2\n"

	h := stdinhandler.NewStdinHandler()
	defer func() { _ = h.Cleanup() }()

	var resolved []string
	var err error
	withStdin(t, csvData, func() {
		resolved, err = h.ResolveFiles([]string{"first.csv", "-", "last.csv"}, "csv")
	})
	require.NoError(t, err)
	require.Len(t, resolved, 3)

	assert.Equal(t, "first.csv", resolved[0])
	assert.Equal(t, "last.csv", resolved[2])
	// Middle entry is the resolved stdin temp file.
	assert.Equal(t, ".csv", filepath.Ext(resolved[1]))

	content, readErr := os.ReadFile(resolved[1])
	require.NoError(t, readErr)
	assert.Equal(t, csvData, string(content))
}

func TestResolveFiles_EmptyStdinHandledGracefully(t *testing.T) {
	h := stdinhandler.NewStdinHandler()
	defer func() { _ = h.Cleanup() }()

	var resolved []string
	var err error
	withStdin(t, "", func() {
		resolved, err = h.ResolveFiles([]string{"-"}, "csv")
	})
	require.NoError(t, err)
	require.Len(t, resolved, 1)

	content, readErr := os.ReadFile(resolved[0])
	require.NoError(t, readErr)
	assert.Empty(t, content)
}

func TestResolveFiles_FormatExtensions(t *testing.T) {
	tests := []struct {
		format   string
		expected string
	}{
		{"csv", ".csv"},
		{"", ".csv"},        // default
		{"unknown", ".csv"}, // falls back to default
		{"json", ".json"},
		{"JSON", ".json"}, // case-insensitive
		{"jsonl", ".jsonl"},
		{"ndjson", ".jsonl"},
		{"xml", ".xml"},
		{"yaml", ".yaml"},
		{"yml", ".yaml"},
		{"parquet", ".parquet"},
		{"avro", ".avro"},
		{"orc", ".orc"},
	}

	for _, tt := range tests {
		t.Run("format_"+tt.format, func(t *testing.T) {
			h := stdinhandler.NewStdinHandler()
			defer func() { _ = h.Cleanup() }()

			var resolved []string
			var err error
			withStdin(t, "payload-data", func() {
				resolved, err = h.ResolveFiles([]string{"-"}, tt.format)
			})
			require.NoError(t, err)
			require.Len(t, resolved, 1)

			assert.Equal(t, tt.expected, filepath.Ext(resolved[0]))
			assert.True(t, strings.Contains(filepath.Base(resolved[0]), "stdin_data"))

			content, readErr := os.ReadFile(resolved[0])
			require.NoError(t, readErr)
			assert.Equal(t, "payload-data", string(content))
		})
	}
}

func TestCleanup_RemovesTempFiles(t *testing.T) {
	h := stdinhandler.NewStdinHandler()

	var resolved []string
	var err error
	withStdin(t, "x,y\n1,2\n", func() {
		resolved, err = h.ResolveFiles([]string{"-"}, "csv")
	})
	require.NoError(t, err)
	require.Len(t, resolved, 1)

	_, statErr := os.Stat(resolved[0])
	require.NoError(t, statErr)

	require.NoError(t, h.Cleanup())

	_, statErr = os.Stat(resolved[0])
	assert.True(t, os.IsNotExist(statErr))
}

func TestCleanup_NoTempDirIsNoOp(t *testing.T) {
	h := stdinhandler.NewStdinHandler()
	assert.NoError(t, h.Cleanup())
}
