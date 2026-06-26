package xml_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/adrianolaselva/dataql/pkg/filehandler/xml"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/adrianolaselva/dataql/pkg/storage/sqlite"
	"github.com/schollz/progressbar/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fixturePath(name string) string {
	return filepath.Join("..", "..", "..", "tests", "fixtures", "xml", name)
}

func newProgressBar() *progressbar.ProgressBar {
	return progressbar.NewOptions(0,
		progressbar.OptionSetWriter(bytes.NewBuffer(nil)),
	)
}

func newStorage(t *testing.T) storage.Storage {
	t.Helper()
	st, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	return st
}

func queryInt(t *testing.T, storage storage.Storage, query string) int {
	t.Helper()
	rows, err := storage.Query(query)
	require.NoError(t, err)
	defer rows.Close()

	var value int
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&value))
	return value
}

func queryString(t *testing.T, storage storage.Storage, query string) string {
	t.Helper()
	rows, err := storage.Query(query)
	require.NoError(t, err)
	defer rows.Close()

	var value string
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&value))
	return value
}

func TestXmlHandler_Import_Collection(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := xml.NewXmlHandler([]string{fixturePath("users.xml")}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 3, queryInt(t, storage, "SELECT COUNT(*) FROM users"))
	assert.Equal(t, "alice@example.com",
		queryString(t, storage, "SELECT email FROM users WHERE name = 'Alice'"))

	require.NoError(t, handler.Close())
}

func TestXmlHandler_Import_Nested(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := xml.NewXmlHandler([]string{fixturePath("nested.xml")}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 2, queryInt(t, storage, "SELECT COUNT(*) FROM nested"))
	// Nested elements are flattened with underscore-separated column names.
	assert.Equal(t, "john@test.com",
		queryString(t, storage, "SELECT person_contact_email FROM nested WHERE person_name = 'John'"))
}

func TestXmlHandler_Import_Attributes(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	// products.xml carries data both as attributes (id, category) and elements.
	handler := xml.NewXmlHandler([]string{fixturePath("products.xml")}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 3, queryInt(t, storage, "SELECT COUNT(*) FROM products"))
	assert.Equal(t, "clothing",
		queryString(t, storage, "SELECT category FROM products WHERE name = 'T-Shirt'"))
}

func TestXmlHandler_Import_SingleObject(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := xml.NewXmlHandler([]string{fixturePath("single.xml")}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 1, queryInt(t, storage, "SELECT COUNT(*) FROM single"))
	assert.Equal(t, "localhost",
		queryString(t, storage, "SELECT host FROM single"))
}

func TestXmlHandler_Import_Empty(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := xml.NewXmlHandler([]string{fixturePath("empty.xml")}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	require.NoError(t, err)

	// Empty XML builds a placeholder table that is still queryable.
	assert.Equal(t, 0, queryInt(t, storage, "SELECT COUNT(*) FROM empty"))
}

func TestXmlHandler_Import_Invalid(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	// invalid.xml has mismatched tags and must surface a parse error.
	handler := xml.NewXmlHandler([]string{fixturePath("invalid.xml")}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	assert.Error(t, err)
}

func TestXmlHandler_Import_FileNotFound(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := xml.NewXmlHandler([]string{"/nonexistent/file.xml"}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	assert.Error(t, err)
}

func TestXmlHandler_Import_LineLimit(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := xml.NewXmlHandler([]string{fixturePath("users.xml")}, newProgressBar(), storage, 2, "")

	err := handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 2, queryInt(t, storage, "SELECT COUNT(*) FROM users"))
	assert.Equal(t, 2, handler.Lines())
}

func TestXmlHandler_Import_CustomCollection(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := xml.NewXmlHandler([]string{fixturePath("users.xml")}, newProgressBar(), storage, 0, "My Collection")

	err := handler.Import()
	require.NoError(t, err)

	// Collection name is lowercased with spaces converted to underscores.
	assert.Equal(t, 3, queryInt(t, storage, "SELECT COUNT(*) FROM my_collection"))
}
