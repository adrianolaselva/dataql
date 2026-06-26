package json_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	jsonExport "github.com/adrianolaselva/dataql/pkg/exportdata/json"
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

func TestJsonExport_Export_Success(t *testing.T) {
	tmpDir := t.TempDir()

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

	exportPath := filepath.Join(tmpDir, "output.json")
	bar := createProgressBar()

	exporter := jsonExport.NewJsonExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	content, err := os.ReadFile(exportPath)
	require.NoError(t, err)

	var records []map[string]any
	err = json.Unmarshal(content, &records)
	require.NoError(t, err)

	assert.Len(t, records, 2)
	assert.Equal(t, "John", toString(records[0]["name"]))
	assert.Equal(t, "100", toString(records[0]["value"]))
	assert.Equal(t, "Jane", toString(records[1]["name"]))

	assert.Contains(t, string(content), "John")
	assert.Contains(t, string(content), "100")
}

func TestJsonExport_Export_EmptyResult(t *testing.T) {
	tmpDir := t.TempDir()

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	err = storage.BuildStructure("test_table", []string{"id", "name", "value"})
	require.NoError(t, err)
	err = storage.InsertRow("test_table", []string{"id", "name", "value"}, []any{"1", "John", "100"})
	require.NoError(t, err)

	rows, err := storage.Query("SELECT * FROM test_table WHERE 1=0")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "empty.json")
	bar := createProgressBar()

	exporter := jsonExport.NewJsonExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	content, err := os.ReadFile(exportPath)
	require.NoError(t, err)

	var records []map[string]any
	err = json.Unmarshal(content, &records)
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestJsonExport_Export_SpecialCharacters(t *testing.T) {
	tmpDir := t.TempDir()

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	err = storage.BuildStructure("test_table", []string{"id", "name", "value"})
	require.NoError(t, err)
	err = storage.InsertRow("test_table", []string{"id", "name", "value"}, []any{"1", "a,b", `<x>"quoted"`})
	require.NoError(t, err)

	rows, err := storage.Query("SELECT * FROM test_table")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "special.json")
	bar := createProgressBar()

	exporter := jsonExport.NewJsonExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	content, err := os.ReadFile(exportPath)
	require.NoError(t, err)

	var records []map[string]any
	err = json.Unmarshal(content, &records)
	require.NoError(t, err)

	require.Len(t, records, 1)
	assert.Equal(t, "a,b", toString(records[0]["name"]))
	assert.Equal(t, `<x>"quoted"`, toString(records[0]["value"]))
}

// toString normalizes scanned SQLite values (which may be string or []byte)
// to a comparable string.
func toString(v any) string {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case string:
		return t
	default:
		return ""
	}
}
