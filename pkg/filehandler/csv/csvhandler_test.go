package csv_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/adrianolaselva/dataql/pkg/filehandler/csv"
	"github.com/adrianolaselva/dataql/pkg/storage/sqlite"
	"github.com/schollz/progressbar/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fileModeDefault os.FileMode = 0644
)

func createTestCSV(t *testing.T, dir, filename, content string) string {
	t.Helper()
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	filePath := filepath.Join(dir, filename)
	err = os.WriteFile(filePath, []byte(content), fileModeDefault)
	require.NoError(t, err)

	return filePath
}

func createProgressBar() *progressbar.ProgressBar {
	return progressbar.NewOptions(0,
		progressbar.OptionSetWriter(bytes.NewBuffer(nil)),
	)
}

func TestCsvHandler_Import_Success(t *testing.T) {
	tests := []struct {
		name       string
		filename   string
		content    string
		delimiter  rune
		collection string
		tableName  string
		query      string
		expected   int
	}{
		{
			name:      "simple CSV with comma delimiter",
			filename:  "testcomma.csv",
			content:   "id,name,email\n1,John,john@example.com\n2,Jane,jane@example.com\n",
			delimiter: ',',
			tableName: "testcomma",
			query:     "SELECT COUNT(*) FROM testcomma",
			expected:  2,
		},
		{
			name:      "CSV with semicolon delimiter",
			filename:  "testsemicolon.csv",
			content:   "id;name;value\n1;Product A;100\n2;Product B;200\n3;Product C;300\n",
			delimiter: ';',
			tableName: "testsemicolon",
			query:     "SELECT COUNT(*) FROM testsemicolon",
			expected:  3,
		},
		{
			name:       "CSV with custom collection name",
			filename:   "datafile.csv",
			content:    "col1,col2\nval1,val2\n",
			delimiter:  ',',
			collection: "mycustomtable",
			tableName:  "mycustomtable",
			query:      "SELECT COUNT(*) FROM mycustomtable",
			expected:   1,
		},
		{
			name:       "CSV with collection name containing spaces",
			filename:   "anotherfile.csv",
			content:    "a,b\n1,2\n",
			delimiter:  ',',
			collection: "My Table Name",
			tableName:  "my_table_name",
			query:      "SELECT COUNT(*) FROM my_table_name",
			expected:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := filepath.Join(os.TempDir(), "dataql_test", tt.name)
			defer os.RemoveAll(tmpDir)

			filePath := createTestCSV(t, tmpDir, tt.filename, tt.content)

			storage, err := sqlite.NewSqLiteStorage(":memory:")
			require.NoError(t, err)
			defer storage.Close()

			bar := createProgressBar()
			handler := csv.NewCsvHandler([]string{filePath}, tt.delimiter, bar, storage, 0, tt.collection)

			err = handler.Import()
			assert.NoError(t, err)

			rows, err := storage.Query(tt.query)
			require.NoError(t, err)
			defer rows.Close()

			var count int
			rows.Next()
			err = rows.Scan(&count)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, count)

			err = handler.Close()
			assert.NoError(t, err)
		})
	}
}

func TestCsvHandler_Import_WithLineLimit(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_test_limit")
	defer os.RemoveAll(tmpDir)

	content := "id,name\n1,A\n2,B\n3,C\n4,D\n5,E\n"
	filePath := createTestCSV(t, tmpDir, "testlimit.csv", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := csv.NewCsvHandler([]string{filePath}, ',', bar, storage, 2, "")

	err = handler.Import()
	assert.NoError(t, err)

	rows, err := storage.Query("SELECT COUNT(*) FROM testlimit")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	rows.Next()
	err = rows.Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 2, count)
}

func TestCsvHandler_Import_MultipleFiles(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_test_multi")
	defer os.RemoveAll(tmpDir)

	content1 := "id,value\n1,100\n2,200\n"
	content2 := "id,value\n3,300\n4,400\n"

	filePath1 := createTestCSV(t, tmpDir, "file1.csv", content1)
	filePath2 := createTestCSV(t, tmpDir, "file2.csv", content2)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := csv.NewCsvHandler([]string{filePath1, filePath2}, ',', bar, storage, 0, "")

	err = handler.Import()
	assert.NoError(t, err)

	// Verify both tables exist
	rows1, err := storage.Query("SELECT COUNT(*) FROM file1")
	require.NoError(t, err)

	var count1 int
	rows1.Next()
	err = rows1.Scan(&count1)
	require.NoError(t, err)
	rows1.Close()
	assert.Equal(t, 2, count1)

	rows2, err := storage.Query("SELECT COUNT(*) FROM file2")
	require.NoError(t, err)

	var count2 int
	rows2.Next()
	err = rows2.Scan(&count2)
	require.NoError(t, err)
	rows2.Close()
	assert.Equal(t, 2, count2)
}

func TestCsvHandler_Import_FileNotFound(t *testing.T) {
	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := csv.NewCsvHandler([]string{"/nonexistent/file.csv"}, ',', bar, storage, 0, "")

	err = handler.Import()
	assert.Error(t, err)
}

func TestCsvHandler_Lines(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_test_lines")
	defer os.RemoveAll(tmpDir)

	// 4 lines in total (including header line counts as newline)
	content := "id,name\n1,A\n2,B\n3,C\n"
	filePath := createTestCSV(t, tmpDir, "testlines.csv", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := csv.NewCsvHandler([]string{filePath}, ',', bar, storage, 0, "")

	err = handler.Import()
	require.NoError(t, err)

	lines := handler.Lines()
	// Lines counts newlines, which is 4 in this content
	assert.Equal(t, 4, lines)
}

func TestCsvHandler_SpecialCharactersInFilename(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_test_special")
	defer os.RemoveAll(tmpDir)

	content := "id,name\n1,Test\n"
	// Table name after processing: testfile2024 (removes hyphen and underscore)
	filePath := createTestCSV(t, tmpDir, "testfile2024.csv", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := csv.NewCsvHandler([]string{filePath}, ',', bar, storage, 0, "")

	err = handler.Import()
	assert.NoError(t, err)

	rows, err := storage.Query("SELECT COUNT(*) FROM testfile2024")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	rows.Next()
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestCsvHandler_QueryAfterImport(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dataql_test_query")
	defer os.RemoveAll(tmpDir)

	content := "id,name,price\n1,Product A,100\n2,Product B,200\n3,Product C,150\n"
	filePath := createTestCSV(t, tmpDir, "products.csv", content)

	storage, err := sqlite.NewSqLiteStorage(":memory:")
	require.NoError(t, err)
	defer storage.Close()

	bar := createProgressBar()
	handler := csv.NewCsvHandler([]string{filePath}, ',', bar, storage, 0, "")

	err = handler.Import()
	require.NoError(t, err)

	// Test various SQL queries
	t.Run("SELECT with WHERE", func(t *testing.T) {
		rows, err := storage.Query("SELECT name FROM products WHERE price > '150'")
		require.NoError(t, err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			err = rows.Scan(&name)
			require.NoError(t, err)
			names = append(names, name)
		}
		assert.Equal(t, []string{"Product B"}, names)
	})

	t.Run("SELECT with ORDER BY", func(t *testing.T) {
		rows, err := storage.Query("SELECT name FROM products ORDER BY name ASC")
		require.NoError(t, err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			err = rows.Scan(&name)
			require.NoError(t, err)
			names = append(names, name)
		}
		assert.Equal(t, []string{"Product A", "Product B", "Product C"}, names)
	})
}
