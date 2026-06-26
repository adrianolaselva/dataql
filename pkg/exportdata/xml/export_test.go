package xml_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	xmlExport "github.com/adrianolaselva/dataql/pkg/exportdata/xml"
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

func TestXmlExport_Export_Success(t *testing.T) {
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

	exportPath := filepath.Join(tmpDir, "output.xml")
	bar := createProgressBar()

	exporter := xmlExport.NewXmlExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	content, err := os.ReadFile(exportPath)
	require.NoError(t, err)
	out := string(content)

	assert.Contains(t, out, "<data>")
	assert.Contains(t, out, "<row>")
	assert.Contains(t, out, "<name>John</name>")
	assert.Contains(t, out, "<value>100</value>")
	assert.Contains(t, out, "<name>Jane</name>")
	assert.Contains(t, out, "<value>200</value>")
}

func TestXmlExport_Export_EmptyResult(t *testing.T) {
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

	exportPath := filepath.Join(tmpDir, "empty.xml")
	bar := createProgressBar()

	exporter := xmlExport.NewXmlExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	content, err := os.ReadFile(exportPath)
	require.NoError(t, err)
	out := string(content)

	assert.Contains(t, out, "<data>")
	assert.NotContains(t, out, "<row>")
}

func TestXmlExport_Export_SpecialCharacters(t *testing.T) {
	tmpDir := t.TempDir()

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	err = storage.BuildStructure("test_table", []string{"id", "name", "value"})
	require.NoError(t, err)
	err = storage.InsertRow("test_table", []string{"id", "name", "value"}, []any{"1", "a,b", "<x> & y"})
	require.NoError(t, err)

	rows, err := storage.Query("SELECT * FROM test_table")
	require.NoError(t, err)

	exportPath := filepath.Join(tmpDir, "special.xml")
	bar := createProgressBar()

	exporter := xmlExport.NewXmlExport(rows, exportPath, bar)
	defer exporter.Close()

	err = exporter.Export()
	assert.NoError(t, err)

	content, err := os.ReadFile(exportPath)
	require.NoError(t, err)
	out := string(content)

	assert.Contains(t, out, "<name>a,b</name>")
	// XML special characters must be escaped on output.
	assert.Contains(t, out, "&lt;x&gt; &amp; y")
}
