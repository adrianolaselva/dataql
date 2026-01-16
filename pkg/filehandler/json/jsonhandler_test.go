package json_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/adrianolaselva/dataql/pkg/filehandler/json"
	"github.com/adrianolaselva/dataql/pkg/storage/sqlite"
	"github.com/schollz/progressbar/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fileModeDefault os.FileMode = 0644

func createTestJSON(t *testing.T, dir, filename, content string) string {
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

func TestJsonHandler_Import_Array(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_json_test_array")
	defer os.RemoveAll(tmpDir)

	content := `[
		{"id": "1", "name": "John", "email": "john@example.com"},
		{"id": "2", "name": "Jane", "email": "jane@example.com"}
	]`
	filePath := createTestJSON(t, tmpDir, "users.json", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := json.NewJsonHandler([]string{filePath}, bar, storage, 0, "")

	err = handler.Import()
	assert.NoError(t, err)

	rows, err := storage.Query("SELECT COUNT(*) FROM users")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	rows.Next()
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestJsonHandler_Import_SingleObject(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_json_test_object")
	defer os.RemoveAll(tmpDir)

	content := `{"id": "1", "name": "John", "email": "john@example.com"}`
	filePath := createTestJSON(t, tmpDir, "user.json", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := json.NewJsonHandler([]string{filePath}, bar, storage, 0, "")

	err = handler.Import()
	assert.NoError(t, err)

	rows, err := storage.Query("SELECT COUNT(*) FROM user")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	rows.Next()
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestJsonHandler_Import_WithCollection(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_json_test_collection")
	defer os.RemoveAll(tmpDir)

	content := `[{"id": "1", "value": "test"}]`
	filePath := createTestJSON(t, tmpDir, "data.json", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := json.NewJsonHandler([]string{filePath}, bar, storage, 0, "my_custom_table")

	err = handler.Import()
	assert.NoError(t, err)

	rows, err := storage.Query("SELECT COUNT(*) FROM my_custom_table")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	rows.Next()
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestJsonHandler_Import_WithLineLimit(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_json_test_limit")
	defer os.RemoveAll(tmpDir)

	content := `[
		{"id": "1"},
		{"id": "2"},
		{"id": "3"},
		{"id": "4"},
		{"id": "5"}
	]`
	filePath := createTestJSON(t, tmpDir, "limited.json", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := json.NewJsonHandler([]string{filePath}, bar, storage, 2, "")

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

func TestJsonHandler_Import_FileNotFound(t *testing.T) {
	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := json.NewJsonHandler([]string{"/nonexistent/file.json"}, bar, storage, 0, "")

	err = handler.Import()
	assert.Error(t, err)
}

func TestJsonHandler_Lines(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_json_test_lines")
	defer os.RemoveAll(tmpDir)

	content := `[{"id": "1"}, {"id": "2"}, {"id": "3"}]`
	filePath := createTestJSON(t, tmpDir, "lines.json", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := json.NewJsonHandler([]string{filePath}, bar, storage, 0, "")

	err = handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 3, handler.Lines())
}

func TestJsonHandler_Close(t *testing.T) {
	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := json.NewJsonHandler([]string{}, bar, storage, 0, "")

	err = handler.Close()
	assert.NoError(t, err)
}
