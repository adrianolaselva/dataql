package jsonl_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/adrianolaselva/dataql/pkg/exportdata/jsonl"
	"github.com/adrianolaselva/dataql/pkg/storage/sqlite"
	"github.com/schollz/progressbar/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createProgressBar() *progressbar.ProgressBar {
	return progressbar.NewOptions(0,
		progressbar.OptionSetWriter(bytes.NewBuffer(nil)),
	)
}

func TestJsonlExport_Export_Success(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_test")
	defer os.RemoveAll(tmpDir)
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	// Setup test data in SQLite
	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	err = storage.BuildStructure("test_table", []string{"id", "name", "value"})
	require.NoError(t, err)

	err = storage.InsertRow("test_table", []string{"id", "name", "value"}, []any{"1", "John", "100"})
	require.NoError(t, err)
	err = storage.InsertRow("test_table", []string{"id", "name", "value"}, []any{"2", "Jane", "200"})
	require.NoError(t, err)

	rows, err := storage.Query("SELECT * FROM test_table")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "output.jsonl")
	bar := createProgressBar()

	exporter := jsonl.NewJsonlExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	// Verify exported file
	file, err := os.Open(exportPath)
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []map[string]interface{}
	for scanner.Scan() {
		var record map[string]interface{}
		err = json.Unmarshal(scanner.Bytes(), &record)
		require.NoError(t, err)
		lines = append(lines, record)
	}

	assert.Len(t, lines, 2)
	assert.Equal(t, "1", lines[0]["id"])
	assert.Equal(t, "John", lines[0]["name"])
	assert.Equal(t, "100", lines[0]["value"])
	assert.Equal(t, "2", lines[1]["id"])
	assert.Equal(t, "Jane", lines[1]["name"])
	assert.Equal(t, "200", lines[1]["value"])
}

func TestJsonlExport_Export_EmptyResult(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_empty")
	defer os.RemoveAll(tmpDir)
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	err = storage.BuildStructure("empty_table", []string{"id", "name"})
	require.NoError(t, err)

	rows, err := storage.Query("SELECT * FROM empty_table")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "empty.jsonl")
	bar := createProgressBar()

	exporter := jsonl.NewJsonlExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	// Verify file is empty
	content, err := os.ReadFile(exportPath)
	require.NoError(t, err)
	assert.Empty(t, content)
}

func TestJsonlExport_Export_OverwritesExistingFile(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_overwrite")
	defer os.RemoveAll(tmpDir)
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "existing.jsonl")
	err = os.WriteFile(exportPath, []byte("old content\n"), 0644)
	require.NoError(t, err)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	err = storage.BuildStructure("overwrite_test", []string{"col1"})
	require.NoError(t, err)
	err = storage.InsertRow("overwrite_test", []string{"col1"}, []any{"new_value"})
	require.NoError(t, err)

	rows, err := storage.Query("SELECT * FROM overwrite_test")
	require.NoError(t, err)

	bar := createProgressBar()
	exporter := jsonl.NewJsonlExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	// Verify old content is replaced
	content, err := os.ReadFile(exportPath)
	require.NoError(t, err)
	assert.NotContains(t, string(content), "old content")
	assert.Contains(t, string(content), "new_value")
}

func TestJsonlExport_Export_CreatesDirectory(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_mkdir", "nested", "dir")
	defer os.RemoveAll(filepath.Join(os.TempDir(), "dataql_jsonl_mkdir"))

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	err = storage.BuildStructure("mkdir_test", []string{"id"})
	require.NoError(t, err)
	err = storage.InsertRow("mkdir_test", []string{"id"}, []any{"1"})
	require.NoError(t, err)

	rows, err := storage.Query("SELECT * FROM mkdir_test")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "output.jsonl")
	bar := createProgressBar()

	exporter := jsonl.NewJsonlExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	// Verify file was created
	_, err = os.Stat(exportPath)
	assert.NoError(t, err)
}

func TestJsonlExport_Export_SpecialCharacters(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_jsonl_special")
	defer os.RemoveAll(tmpDir)
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	err = storage.BuildStructure("special_chars", []string{"name", "description"})
	require.NoError(t, err)
	err = storage.InsertRow("special_chars", []string{"name", "description"}, []any{"Test \"Quotes\"", "Line1\nLine2"})
	require.NoError(t, err)

	rows, err := storage.Query("SELECT * FROM special_chars")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "special.jsonl")
	bar := createProgressBar()

	exporter := jsonl.NewJsonlExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	// Verify JSON encoding handles special characters
	content, err := os.ReadFile(exportPath)
	require.NoError(t, err)

	var record map[string]interface{}
	err = json.Unmarshal(content, &record)
	require.NoError(t, err)

	assert.Equal(t, "Test \"Quotes\"", record["name"])
	assert.Equal(t, "Line1\nLine2", record["description"])
}
