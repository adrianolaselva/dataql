package avro_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/adrianolaselva/dataql/pkg/filehandler/avro"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/adrianolaselva/dataql/pkg/storage/sqlite"
	"github.com/schollz/progressbar/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fixturePath(name string) string {
	return filepath.Join("..", "..", "..", "tests", "fixtures", "avro", name)
}

func newProgressBar() *progressbar.ProgressBar {
	return progressbar.NewOptions(0,
		progressbar.OptionSetWriter(bytes.NewBuffer(nil)),
	)
}

func newStorage(t *testing.T) *sqliteCloser {
	t.Helper()
	st, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	return &sqliteCloser{Storage: st, t: t}
}

// sqliteCloser is a thin wrapper to make queries terser in tests.
type sqliteCloser struct {
	storage.Storage
	t *testing.T
}

func (s *sqliteCloser) countRows(query string) int {
	rows, err := s.Query(query)
	require.NoError(s.t, err)
	defer rows.Close()

	var count int
	require.True(s.t, rows.Next())
	require.NoError(s.t, rows.Scan(&count))
	return count
}

func (s *sqliteCloser) scanString(query string) string {
	rows, err := s.Query(query)
	require.NoError(s.t, err)
	defer rows.Close()

	var value string
	require.True(s.t, rows.Next())
	require.NoError(s.t, rows.Scan(&value))
	return value
}

func TestAvroHandler_Import_Users(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := avro.NewAvroHandler(
		[]string{fixturePath("users.avro")},
		newProgressBar(),
		storage,
		0,
		"",
	)

	require.NoError(t, handler.Import())

	assert.Equal(t, 3, storage.countRows("SELECT COUNT(*) FROM users"))
	assert.Equal(t, "alice@example.com", storage.scanString("SELECT email FROM users WHERE name = 'Alice'"))
	assert.Equal(t, 3, handler.Lines())
	assert.NoError(t, handler.Close())
}

func TestAvroHandler_Import_Products(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := avro.NewAvroHandler(
		[]string{fixturePath("products.avro")},
		newProgressBar(),
		storage,
		0,
		"",
	)

	require.NoError(t, handler.Import())

	assert.Equal(t, 3, storage.countRows("SELECT COUNT(*) FROM products"))
	assert.Equal(t, "Laptop", storage.scanString("SELECT name FROM products WHERE id = '1'"))
}

func TestAvroHandler_Import_Empty(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := avro.NewAvroHandler(
		[]string{fixturePath("empty.avro")},
		newProgressBar(),
		storage,
		0,
		"",
	)

	require.NoError(t, handler.Import())
	assert.Equal(t, 0, storage.countRows("SELECT COUNT(*) FROM empty"))
}

func TestAvroHandler_Import_FileNotFound(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := avro.NewAvroHandler(
		[]string{"/nonexistent/file.avro"},
		newProgressBar(),
		storage,
		0,
		"",
	)

	assert.Error(t, handler.Import())
}

func TestAvroHandler_Import_WithLineLimit(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := avro.NewAvroHandler(
		[]string{fixturePath("users.avro")},
		newProgressBar(),
		storage,
		2,
		"",
	)

	require.NoError(t, handler.Import())
	assert.Equal(t, 2, storage.countRows("SELECT COUNT(*) FROM users"))
	assert.Equal(t, 2, handler.Lines())
}

func TestAvroHandler_Import_CustomCollection(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := avro.NewAvroHandler(
		[]string{fixturePath("users.avro")},
		newProgressBar(),
		storage,
		0,
		"my_users",
	)

	require.NoError(t, handler.Import())
	assert.Equal(t, 3, storage.countRows("SELECT COUNT(*) FROM my_users"))
}

func TestAvroHandler_Import_MultipleFiles(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := avro.NewAvroHandler(
		[]string{fixturePath("users.avro"), fixturePath("products.avro")},
		newProgressBar(),
		storage,
		0,
		"",
	)

	require.NoError(t, handler.Import())
	assert.Equal(t, 3, storage.countRows("SELECT COUNT(*) FROM users"))
	assert.Equal(t, 3, storage.countRows("SELECT COUNT(*) FROM products"))
}
