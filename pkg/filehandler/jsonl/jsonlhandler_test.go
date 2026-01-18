package jsonl_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/adrianolaselva/dataql/pkg/filehandler/jsonl"
	"github.com/adrianolaselva/dataql/pkg/storage/sqlite"
	"github.com/schollz/progressbar/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fileModeDefault os.FileMode = 0644

func createTestJSONL(t *testing.T, dir, filename, content string) string {
	t.Helper()
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	filePath := filepath.Join(dir, filename)
	err = os.WriteFile(filePath, []byte(content), fileModeDefault)
	require.NoError(t, err)

	return filePath
}

func createProgressBar() *progressbar.ProgressBar {
	return progressbar.NewOptions(0,
		progressbar.OptionSetWriter(bytes.NewBuffer(nil)),
	)
}

func TestJsonlHandler_Import_Success(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_test_basic")
	defer os.RemoveAll(tmpDir)

	content := `{"id": "1", "name": "John", "email": "john@example.com"}
{"id": "2", "name": "Jane", "email": "jane@example.com"}
{"id": "3", "name": "Bob", "email": "bob@example.com"}`
	filePath := createTestJSONL(t, tmpDir, "users.jsonl", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := jsonl.NewJsonlHandler([]string{filePath}, bar, storage, 0, "")

	err = handler.Import()
	assert.NoError(t, err)

	rows, err := storage.Query("SELECT COUNT(*) FROM users")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	rows.Next()
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestJsonlHandler_Import_WithEmptyLines(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_test_empty")
	defer os.RemoveAll(tmpDir)

	content := `{"id": "1"}

{"id": "2"}

{"id": "3"}`
	filePath := createTestJSONL(t, tmpDir, "empty_lines.jsonl", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := jsonl.NewJsonlHandler([]string{filePath}, bar, storage, 0, "")

	err = handler.Import()
	assert.NoError(t, err)

	rows, err := storage.Query("SELECT COUNT(*) FROM empty_lines")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	rows.Next()
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestJsonlHandler_Import_WithCollection(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_test_collection")
	defer os.RemoveAll(tmpDir)

	content := `{"id": "1"}
{"id": "2"}`
	filePath := createTestJSONL(t, tmpDir, "data.jsonl", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := jsonl.NewJsonlHandler([]string{filePath}, bar, storage, 0, "my_custom_table")

	err = handler.Import()
	assert.NoError(t, err)

	rows, err := storage.Query("SELECT COUNT(*) FROM my_custom_table")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	rows.Next()
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestJsonlHandler_Import_WithLineLimit(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_test_limit")
	defer os.RemoveAll(tmpDir)

	content := `{"id": "1"}
{"id": "2"}
{"id": "3"}
{"id": "4"}
{"id": "5"}`
	filePath := createTestJSONL(t, tmpDir, "limited.jsonl", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := jsonl.NewJsonlHandler([]string{filePath}, bar, storage, 2, "")

	err = handler.Import()
	assert.NoError(t, err)

	rows, err := storage.Query("SELECT COUNT(*) FROM limited")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	rows.Next()
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestJsonlHandler_Import_NdjsonExtension(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_test_ndjson")
	defer os.RemoveAll(tmpDir)

	content := `{"id": "1"}
{"id": "2"}`
	filePath := createTestJSONL(t, tmpDir, "data.ndjson", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := jsonl.NewJsonlHandler([]string{filePath}, bar, storage, 0, "")

	err = handler.Import()
	assert.NoError(t, err)

	rows, err := storage.Query("SELECT COUNT(*) FROM data")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	rows.Next()
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestJsonlHandler_Import_FileNotFound(t *testing.T) {
	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := jsonl.NewJsonlHandler([]string{"/nonexistent/file.jsonl"}, bar, storage, 0, "")

	err = handler.Import()
	assert.Error(t, err)
}

func TestJsonlHandler_Lines(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_test_lines")
	defer os.RemoveAll(tmpDir)

	content := `{"id": "1"}
{"id": "2"}
{"id": "3"}
{"id": "4"}`
	filePath := createTestJSONL(t, tmpDir, "lines.jsonl", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := jsonl.NewJsonlHandler([]string{filePath}, bar, storage, 0, "")

	err = handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 4, handler.Lines())
}

func TestJsonlHandler_Import_MultipleFiles(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_test_multi")
	defer os.RemoveAll(tmpDir)

	content1 := `{"id": "1"}
{"id": "2"}`
	content2 := `{"id": "3"}
{"id": "4"}`

	filePath1 := createTestJSONL(t, tmpDir, "file1.jsonl", content1)
	filePath2 := createTestJSONL(t, tmpDir, "file2.jsonl", content2)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := jsonl.NewJsonlHandler([]string{filePath1, filePath2}, bar, storage, 0, "")

	err = handler.Import()
	assert.NoError(t, err)

	// Each file creates its own table
	rows1, err := storage.Query("SELECT COUNT(*) FROM file1")
	require.NoError(t, err)
	var count1 int
	rows1.Next()
	require.NoError(t, rows1.Scan(&count1))
	rows1.Close()
	assert.Equal(t, 2, count1)

	rows2, err := storage.Query("SELECT COUNT(*) FROM file2")
	require.NoError(t, err)
	var count2 int
	rows2.Next()
	require.NoError(t, rows2.Scan(&count2))
	rows2.Close()
	assert.Equal(t, 2, count2)
}

func TestJsonlHandler_Close(t *testing.T) {
	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := jsonl.NewJsonlHandler([]string{}, bar, storage, 0, "")

	err = handler.Close()
	assert.NoError(t, err)
}
