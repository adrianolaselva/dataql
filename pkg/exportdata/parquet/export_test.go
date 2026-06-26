package parquet_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	parquetExport "github.com/adrianolaselva/dataql/pkg/exportdata/parquet"
	parquetHandler "github.com/adrianolaselva/dataql/pkg/filehandler/parquet"
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

// assertParquetMagic asserts the file exists, is non-empty and carries the PAR1 magic header.
func assertParquetMagic(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), 4, "parquet file too small")
	assert.Equal(t, "PAR1", string(data[:4]), "missing parquet magic header")
}

func TestParquetExport_Export_Success(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_parquet_export_test")
	defer os.RemoveAll(tmpDir)
	require.NoError(t, os.MkdirAll(tmpDir, os.ModePerm))

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	require.NoError(t, storage.BuildStructure("test_table", []string{"id", "name", "value"}))
	require.NoError(t, storage.InsertRow("test_table", []string{"id", "name", "value"}, []any{"1", "John", "100"}))
	require.NoError(t, storage.InsertRow("test_table", []string{"id", "name", "value"}, []any{"2", "Jane", "200"}))

	rows, err := storage.Query("SELECT * FROM test_table")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "output.parquet")
	exporter := parquetExport.NewParquetExport(rows, exportPath, createProgressBar())
	defer exporter.Close()

	require.NoError(t, exporter.Export())
	assertParquetMagic(t, exportPath)

	// Round-trip: read the exported file back through DataQL's own parquet handler.
	storage2, err := sqlite.NewSqLiteStorage(filepath.Join(tmpDir, "roundtrip.db"))
	require.NoError(t, err)
	defer storage2.Close()

	handler := parquetHandler.NewParquetHandler([]string{exportPath}, createProgressBar(), storage2, 0, "roundtrip")
	defer handler.Close()
	require.NoError(t, handler.Import())

	countRows, err := storage2.Query("SELECT COUNT(*) AS c FROM roundtrip")
	require.NoError(t, err)
	defer countRows.Close()
	require.True(t, countRows.Next())
	var count int
	require.NoError(t, countRows.Scan(&count))
	assert.Equal(t, 2, count)

	valRows, err := storage2.Query("SELECT name FROM roundtrip WHERE id = '1'")
	require.NoError(t, err)
	defer valRows.Close()
	require.True(t, valRows.Next())
	var name string
	require.NoError(t, valRows.Scan(&name))
	assert.Equal(t, "John", name)
}

func TestParquetExport_Export_EmptyResult(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_parquet_empty")
	defer os.RemoveAll(tmpDir)
	require.NoError(t, os.MkdirAll(tmpDir, os.ModePerm))

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	require.NoError(t, storage.BuildStructure("empty_table", []string{"id", "name"}))

	rows, err := storage.Query("SELECT * FROM empty_table WHERE 1=0")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "empty.parquet")
	exporter := parquetExport.NewParquetExport(rows, exportPath, createProgressBar())
	defer exporter.Close()

	require.NoError(t, exporter.Export())
	assertParquetMagic(t, exportPath)

	// Round-trip an empty file: handler creates a placeholder table with zero rows.
	storage2, err := sqlite.NewSqLiteStorage(filepath.Join(tmpDir, "roundtrip.db"))
	require.NoError(t, err)
	defer storage2.Close()

	handler := parquetHandler.NewParquetHandler([]string{exportPath}, createProgressBar(), storage2, 0, "roundtrip")
	defer handler.Close()
	require.NoError(t, handler.Import())

	countRows, err := storage2.Query("SELECT COUNT(*) AS c FROM roundtrip")
	require.NoError(t, err)
	defer countRows.Close()
	require.True(t, countRows.Next())
	var count int
	require.NoError(t, countRows.Scan(&count))
	assert.Equal(t, 0, count)
}

func TestParquetExport_Export_TypedValues(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_parquet_typed")
	defer os.RemoveAll(tmpDir)
	require.NoError(t, os.MkdirAll(tmpDir, os.ModePerm))

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	require.NoError(t, storage.BuildStructure("products", []string{"id", "name", "price"}))
	require.NoError(t, storage.InsertRow("products", []string{"id", "name", "price"}, []any{"10", "Widget", "9.99"}))
	require.NoError(t, storage.InsertRow("products", []string{"id", "name", "price"}, []any{"20", "Gadget", "19.50"}))
	require.NoError(t, storage.InsertRow("products", []string{"id", "name", "price"}, []any{"30", "Gizmo", "5"}))

	rows, err := storage.Query("SELECT name AS product_name, price AS cost FROM products WHERE id > '10'")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "typed.parquet")
	exporter := parquetExport.NewParquetExport(rows, exportPath, createProgressBar())
	defer exporter.Close()

	require.NoError(t, exporter.Export())
	assertParquetMagic(t, exportPath)

	storage2, err := sqlite.NewSqLiteStorage(filepath.Join(tmpDir, "roundtrip.db"))
	require.NoError(t, err)
	defer storage2.Close()

	handler := parquetHandler.NewParquetHandler([]string{exportPath}, createProgressBar(), storage2, 0, "roundtrip")
	defer handler.Close()
	require.NoError(t, handler.Import())

	countRows, err := storage2.Query("SELECT COUNT(*) AS c FROM roundtrip")
	require.NoError(t, err)
	defer countRows.Close()
	require.True(t, countRows.Next())
	var count int
	require.NoError(t, countRows.Scan(&count))
	assert.Equal(t, 2, count)

	valRows, err := storage2.Query("SELECT cost FROM roundtrip WHERE product_name = 'Gadget'")
	require.NoError(t, err)
	defer valRows.Close()
	require.True(t, valRows.Next())
	var cost string
	require.NoError(t, valRows.Scan(&cost))
	assert.Equal(t, "19.50", cost)
}
