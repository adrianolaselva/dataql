package csv_test

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	csvExport "github.com/adrianolaselva/dataql/pkg/exportdata/csv"
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

func TestCsvExport_Export_Success(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_csv_export_test")
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

	exportPath := filepath.Join(tmpDir, "output.csv")
	bar := createProgressBar()

	exporter := csvExport.NewCsvExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	// Verify exported file
	file, err := os.Open(exportPath)
	require.NoError(t, err)
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	assert.Len(t, records, 3) // 1 header + 2 data rows

	// Verify header
	assert.Equal(t, []string{"id", "name", "value"}, records[0])

	// Verify data rows
	assert.Equal(t, []string{"1", "John", "100"}, records[1])
	assert.Equal(t, []string{"2", "Jane", "200"}, records[2])
}

func TestCsvExport_Export_EmptyResult(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_csv_empty")
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

	exportPath := filepath.Join(tmpDir, "empty.csv")
	bar := createProgressBar()

	exporter := csvExport.NewCsvExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	// Verify file has only headers
	file, err := os.Open(exportPath)
	require.NoError(t, err)
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	assert.Len(t, records, 1) // Only header
	assert.Equal(t, []string{"id", "name"}, records[0])
}

func TestCsvExport_Export_OverwritesExistingFile(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_csv_overwrite")
	defer os.RemoveAll(tmpDir)
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "existing.csv")
	err = os.WriteFile(exportPath, []byte("old,content\ndata,here\n"), 0644)
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
	exporter := csvExport.NewCsvExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	// Verify old content is replaced
	file, err := os.Open(exportPath)
	require.NoError(t, err)
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	assert.Len(t, records, 2)
	assert.Equal(t, []string{"col1"}, records[0])
	assert.Equal(t, []string{"new_value"}, records[1])
}

func TestCsvExport_Export_CreatesDirectory(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_csv_mkdir", "nested", "dir")
	defer os.RemoveAll(filepath.Join(os.TempDir(), "dataql_csv_mkdir"))

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	err = storage.BuildStructure("mkdir_test", []string{"id"})
	require.NoError(t, err)
	err = storage.InsertRow("mkdir_test", []string{"id"}, []any{"1"})
	require.NoError(t, err)

	rows, err := storage.Query("SELECT * FROM mkdir_test")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "output.csv")
	bar := createProgressBar()

	exporter := csvExport.NewCsvExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	// Verify file was created
	_, err = os.Stat(exportPath)
	assert.NoError(t, err)
}

func TestCsvExport_Export_WithQuery(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_csv_query")
	defer os.RemoveAll(tmpDir)
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	err = storage.BuildStructure("products", []string{"id", "name", "price"})
	require.NoError(t, err)
	err = storage.InsertRow("products", []string{"id", "name", "price"}, []any{"1", "Product A", "100"})
	require.NoError(t, err)
	err = storage.InsertRow("products", []string{"id", "name", "price"}, []any{"2", "Product B", "200"})
	require.NoError(t, err)
	err = storage.InsertRow("products", []string{"id", "name", "price"}, []any{"3", "Product C", "300"})
	require.NoError(t, err)

	// Query with alias
	rows, err := storage.Query("SELECT name AS product_name, price AS cost FROM products WHERE id > '1'")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "filtered.csv")
	bar := createProgressBar()

	exporter := csvExport.NewCsvExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	file, err := os.Open(exportPath)
	require.NoError(t, err)
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	assert.Len(t, records, 3) // Header + 2 filtered rows
	assert.Equal(t, []string{"product_name", "cost"}, records[0])
}
