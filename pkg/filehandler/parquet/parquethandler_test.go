package parquet_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/adrianolaselva/dataql/pkg/filehandler/parquet"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/adrianolaselva/dataql/pkg/storage/sqlite"
	"github.com/schollz/progressbar/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fixturePath(name string) string {
	return filepath.Join("..", "..", "..", "tests", "fixtures", "parquet", name)
}

func newProgressBar() *progressbar.ProgressBar {
	return progressbar.NewOptions(0,
		progressbar.OptionSetWriter(bytes.NewBuffer(nil)),
	)
}

func newStorage(t *testing.T) storage.Storage {
	t.Helper()
	s, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	return s
}

func queryInt(t *testing.T, storage storage.Storage, query string) int {
	t.Helper()
	rows, err := storage.Query(query)
	require.NoError(t, err)
	defer rows.Close()

	var v int
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	return v
}

func queryString(t *testing.T, storage storage.Storage, query string) string {
	t.Helper()
	rows, err := storage.Query(query)
	require.NoError(t, err)
	defer rows.Close()

	var v string
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	return v
}

func TestParquetHandler_Import_Products(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	bar := newProgressBar()
	handler := parquet.NewParquetHandler([]string{fixturePath("products.parquet")}, bar, storage, 0, "")

	require.NoError(t, handler.Import())

	assert.Equal(t, 3, queryInt(t, storage, "SELECT COUNT(*) FROM products"))
	assert.Equal(t, "Laptop", queryString(t, storage, "SELECT name FROM products WHERE id = '101'"))
	assert.Equal(t, 3, handler.Lines())
	assert.NoError(t, handler.Close())
}

func TestParquetHandler_Import_Users(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	bar := newProgressBar()
	handler := parquet.NewParquetHandler([]string{fixturePath("users.parquet")}, bar, storage, 0, "")

	require.NoError(t, handler.Import())

	assert.Equal(t, 3, queryInt(t, storage, "SELECT COUNT(*) FROM users"))
	assert.Equal(t, "alice@example.com", queryString(t, storage, "SELECT email FROM users WHERE name = 'Alice'"))
}

func TestParquetHandler_Import_Empty(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	bar := newProgressBar()
	handler := parquet.NewParquetHandler([]string{fixturePath("empty.parquet")}, bar, storage, 0, "")

	require.NoError(t, handler.Import())

	assert.Equal(t, 0, queryInt(t, storage, "SELECT COUNT(*) FROM empty"))
}

func TestParquetHandler_Import_FileNotFound(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	bar := newProgressBar()
	handler := parquet.NewParquetHandler([]string{"/nonexistent/file.parquet"}, bar, storage, 0, "")

	assert.Error(t, handler.Import())
}

func TestParquetHandler_Import_WithLineLimit(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	bar := newProgressBar()
	handler := parquet.NewParquetHandler([]string{fixturePath("products.parquet")}, bar, storage, 2, "")

	require.NoError(t, handler.Import())

	assert.Equal(t, 2, queryInt(t, storage, "SELECT COUNT(*) FROM products"))
	assert.Equal(t, 2, handler.Lines())
}

func TestParquetHandler_Import_CustomCollection(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	bar := newProgressBar()
	handler := parquet.NewParquetHandler([]string{fixturePath("products.parquet")}, bar, storage, 0, "my_catalog")

	require.NoError(t, handler.Import())

	assert.Equal(t, 3, queryInt(t, storage, "SELECT COUNT(*) FROM my_catalog"))
	assert.Equal(t, "Laptop", queryString(t, storage, "SELECT name FROM my_catalog WHERE id = '101'"))
}
