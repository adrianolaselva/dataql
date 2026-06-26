package excel_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	excelExport "github.com/adrianolaselva/dataql/pkg/exportdata/excel"
	"github.com/adrianolaselva/dataql/pkg/storage/sqlite"
	"github.com/schollz/progressbar/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

func createProgressBar() *progressbar.ProgressBar {
	return progressbar.NewOptions(0,
		progressbar.OptionSetWriter(bytes.NewBuffer(nil)),
	)
}

func TestExcelExport_Export_Success(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_excel_export_test")
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

	exportPath := filepath.Join(tmpDir, "output.xlsx")
	exporter := excelExport.NewExcelExport(rows, exportPath, createProgressBar())
	defer exporter.Close()

	require.NoError(t, exporter.Export())

	f, err := excelize.OpenFile(exportPath)
	require.NoError(t, err)
	defer f.Close()

	sheetName := f.GetSheetName(f.GetActiveSheetIndex())
	records, err := f.GetRows(sheetName)
	require.NoError(t, err)

	assert.Len(t, records, 3) // 1 header + 2 data rows
	assert.Equal(t, []string{"id", "name", "value"}, records[0])
	assert.Equal(t, []string{"1", "John", "100"}, records[1])
	assert.Equal(t, []string{"2", "Jane", "200"}, records[2])
}

func TestExcelExport_Export_EmptyResult(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_excel_empty")
	defer os.RemoveAll(tmpDir)
	require.NoError(t, os.MkdirAll(tmpDir, os.ModePerm))

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	require.NoError(t, storage.BuildStructure("empty_table", []string{"id", "name"}))

	rows, err := storage.Query("SELECT * FROM empty_table WHERE 1=0")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "empty.xlsx")
	exporter := excelExport.NewExcelExport(rows, exportPath, createProgressBar())
	defer exporter.Close()

	require.NoError(t, exporter.Export())

	f, err := excelize.OpenFile(exportPath)
	require.NoError(t, err)
	defer f.Close()

	sheetName := f.GetSheetName(f.GetActiveSheetIndex())
	records, err := f.GetRows(sheetName)
	require.NoError(t, err)

	assert.Len(t, records, 1) // header only
	assert.Equal(t, []string{"id", "name"}, records[0])
}

func TestExcelExport_Export_TypedValues(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_excel_typed")
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

	exportPath := filepath.Join(tmpDir, "typed.xlsx")
	exporter := excelExport.NewExcelExport(rows, exportPath, createProgressBar())
	defer exporter.Close()

	require.NoError(t, exporter.Export())

	f, err := excelize.OpenFile(exportPath)
	require.NoError(t, err)
	defer f.Close()

	sheetName := f.GetSheetName(f.GetActiveSheetIndex())
	records, err := f.GetRows(sheetName)
	require.NoError(t, err)

	assert.Len(t, records, 3) // header + 2 filtered rows
	assert.Equal(t, []string{"product_name", "cost"}, records[0])
	assert.Equal(t, "Gadget", records[1][0])
	assert.Equal(t, "Gizmo", records[2][0])
}
