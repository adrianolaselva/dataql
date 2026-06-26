package yaml_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/adrianolaselva/dataql/pkg/filehandler/yaml"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/adrianolaselva/dataql/pkg/storage/sqlite"
	"github.com/schollz/progressbar/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fixturePath(name string) string {
	return filepath.Join("..", "..", "..", "tests", "fixtures", "yaml", name)
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

func TestYamlHandler_Import_ArrayOfObjects(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := yaml.NewYamlHandler([]string{fixturePath("users.yaml")}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 3, queryInt(t, storage, "SELECT COUNT(*) FROM users"))
	assert.Equal(t, "alice@example.com",
		queryString(t, storage, "SELECT email FROM users WHERE name = 'Alice'"))

	require.NoError(t, handler.Close())
}

func TestYamlHandler_Import_Nested(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := yaml.NewYamlHandler([]string{fixturePath("nested.yaml")}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 2, queryInt(t, storage, "SELECT COUNT(*) FROM nested"))
	// Nested maps are flattened with underscore-separated keys.
	assert.Equal(t, "New York",
		queryString(t, storage, "SELECT address_city FROM nested WHERE user_name = 'John'"))
}

func TestYamlHandler_Import_ObjectWithArray(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	// products.yml is a single object whose value is an array of records.
	handler := yaml.NewYamlHandler([]string{fixturePath("products.yml")}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 3, queryInt(t, storage, "SELECT COUNT(*) FROM products"))
	assert.Equal(t, "Furniture",
		queryString(t, storage, "SELECT category FROM products WHERE name = 'Desk'"))
}

func TestYamlHandler_Import_SingleObject(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := yaml.NewYamlHandler([]string{fixturePath("single.yaml")}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 1, queryInt(t, storage, "SELECT COUNT(*) FROM single"))
	assert.Equal(t, "active",
		queryString(t, storage, "SELECT status FROM single WHERE name = 'Single User'"))
}

func TestYamlHandler_Import_Empty(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := yaml.NewYamlHandler([]string{fixturePath("empty.yaml")}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	require.NoError(t, err)

	// Empty data builds a placeholder table that is still queryable.
	assert.Equal(t, 0, queryInt(t, storage, "SELECT COUNT(*) FROM empty"))
}

func TestYamlHandler_Import_FileNotFound(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := yaml.NewYamlHandler([]string{"/nonexistent/file.yaml"}, newProgressBar(), storage, 0, "")

	err := handler.Import()
	assert.Error(t, err)
}

func TestYamlHandler_Import_LineLimit(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := yaml.NewYamlHandler([]string{fixturePath("users.yaml")}, newProgressBar(), storage, 2, "")

	err := handler.Import()
	require.NoError(t, err)

	assert.Equal(t, 2, queryInt(t, storage, "SELECT COUNT(*) FROM users"))
	assert.Equal(t, 2, handler.Lines())
}

func TestYamlHandler_Import_CustomCollection(t *testing.T) {
	storage := newStorage(t)
	defer storage.Close()

	handler := yaml.NewYamlHandler([]string{fixturePath("users.yaml")}, newProgressBar(), storage, 0, "My Collection")

	err := handler.Import()
	require.NoError(t, err)

	// Collection name is lowercased with spaces converted to underscores.
	assert.Equal(t, 3, queryInt(t, storage, "SELECT COUNT(*) FROM my_collection"))
}
