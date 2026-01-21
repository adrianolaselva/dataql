package duckdb_test

import (
	"testing"

	"github.com/adrianolaselva/dataql/pkg/storage/duckdb"
	"github.com/stretchr/testify/assert"
)

func TestShouldBuildStructureWithSuccess(t *testing.T) {
	tests := []struct {
		name          string
		columns       []string
		query         string
		rows          [][]any
		columnExpects []string
		rowsExpects   [][]any
	}{
		{
			name:    "basic two columns",
			columns: []string{"column_1", "column_2"},
			query:   "SELECT * FROM rows;",
			rows: [][]any{
				{"value_1", "value_2"},
			},
			columnExpects: []string{"column_1", "column_2"},
			rowsExpects: [][]any{
				{"value_1", "value_2"},
			},
		},
		{
			name:    "three columns",
			columns: []string{"column_1", "column_2", "column_3"},
			query:   "SELECT * FROM rows;",
			rows: [][]any{
				{"value_1", "value_2", "value_3"},
			},
			columnExpects: []string{"column_1", "column_2", "column_3"},
			rowsExpects: [][]any{
				{"value_1", "value_2", "value_3"},
			},
		},
		{
			name:    "three columns with different order values",
			columns: []string{"column_1", "column_2", "column_3"},
			query:   "SELECT * FROM rows;",
			rows: [][]any{
				{"value_3", "value_1", "value_2"},
			},
			columnExpects: []string{"column_1", "column_2", "column_3"},
			rowsExpects: [][]any{
				{"value_3", "value_1", "value_2"},
			},
		},
		{
			name:    "select specific columns in different order",
			columns: []string{"column_1", "column_2", "column_3"},
			query:   "SELECT column_3, column_1, column_2 FROM rows;",
			rows: [][]any{
				{"value_1", "value_2", "value_3"},
			},
			columnExpects: []string{"column_3", "column_1", "column_2"},
			rowsExpects: [][]any{
				{"value_3", "value_1", "value_2"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storage, err := duckdb.NewDuckDBStorage("")
			assert.NoError(t, err)
			defer storage.Close()

			err = storage.BuildStructure("rows", test.columns)
			assert.NoError(t, err)

			for _, row := range test.rows {
				err = storage.InsertRow("rows", test.columns, row)
				assert.NoError(t, err)
			}

			rows, err := storage.Query(test.query)
			assert.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			assert.NoError(t, err)
			assert.Equal(t, test.columnExpects, cols)

			for _, expected := range test.rowsExpects {
				rs := rows.Next()
				assert.True(t, rs)

				values := make([]interface{}, len(test.columnExpects))
				pointers := make([]interface{}, len(test.columnExpects))
				for i := range values {
					pointers[i] = &values[i]
				}

				err = rows.Scan(pointers...)
				assert.NoError(t, err)

				assert.Equal(t, expected, values)
			}
		})
	}
}

func TestShouldCountRowsWithSuccess(t *testing.T) {
	tests := []struct {
		name        string
		columns     []string
		rows        [][]any
		expectCount int64
	}{
		{
			name:    "count single row",
			columns: []string{"column_1", "column_2", "column_3"},
			rows: [][]any{
				{"value_1", "value_2", "value_3"},
			},
			expectCount: 1,
		},
		{
			name:    "count multiple rows",
			columns: []string{"column_1", "column_2", "column_3"},
			rows: [][]any{
				{"value_11", "value_21", "value_31"},
				{"value_12", "value_22", "value_32"},
				{"value_13", "value_23", "value_33"},
			},
			expectCount: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storage, err := duckdb.NewDuckDBStorage("")
			assert.NoError(t, err)
			defer storage.Close()

			err = storage.BuildStructure("rows", test.columns)
			assert.NoError(t, err)

			for _, row := range test.rows {
				err = storage.InsertRow("rows", test.columns, row)
				assert.NoError(t, err)
			}

			rows, err := storage.Query("SELECT COUNT(*) AS total FROM rows;")
			assert.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			assert.NoError(t, err)
			assert.Equal(t, []string{"total"}, cols)

			rs := rows.Next()
			assert.True(t, rs)

			var count int64
			err = rows.Scan(&count)
			assert.NoError(t, err)
			assert.Equal(t, test.expectCount, count)
		})
	}
}

func TestShouldGroupByWithSuccess(t *testing.T) {
	storage, err := duckdb.NewDuckDBStorage("")
	assert.NoError(t, err)
	defer storage.Close()

	columns := []string{"column_1"}
	err = storage.BuildStructure("rows", columns)
	assert.NoError(t, err)

	// Insert 3 rows with same value
	for i := 0; i < 3; i++ {
		err = storage.InsertRow("rows", columns, []any{"Value Test"})
		assert.NoError(t, err)
	}

	rows, err := storage.Query("SELECT column_1, COUNT(*) AS total FROM rows GROUP BY column_1;")
	assert.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	assert.NoError(t, err)
	assert.Equal(t, []string{"column_1", "total"}, cols)

	rs := rows.Next()
	assert.True(t, rs)

	var colValue string
	var count int64
	err = rows.Scan(&colValue, &count)
	assert.NoError(t, err)
	assert.Equal(t, "Value Test", colValue)
	assert.Equal(t, int64(3), count)
}

func TestNewDuckDBStorage_InMemory(t *testing.T) {
	// Empty string creates in-memory database
	storage, err := duckdb.NewDuckDBStorage("")
	assert.NoError(t, err)
	assert.NotNil(t, storage)
	defer storage.Close()

	// Should be able to create and query tables
	err = storage.BuildStructure("test", []string{"col1"})
	assert.NoError(t, err)
}

func TestNewDuckDBStorage_MemoryKeyword(t *testing.T) {
	// :memory: should also create in-memory database (SQLite compatibility)
	storage, err := duckdb.NewDuckDBStorage(":memory:")
	assert.NoError(t, err)
	assert.NotNil(t, storage)
	defer storage.Close()

	// Should be able to create and query tables
	err = storage.BuildStructure("test", []string{"col1"})
	assert.NoError(t, err)
}

func TestShowTables(t *testing.T) {
	storage, err := duckdb.NewDuckDBStorage("")
	assert.NoError(t, err)
	defer storage.Close()

	// Create a table
	err = storage.BuildStructure("test_table", []string{"col1", "col2"})
	assert.NoError(t, err)

	// Show tables should return our table in the schemas
	rows, err := storage.ShowTables()
	assert.NoError(t, err)
	defer rows.Close()

	// Verify test_table appears in schemas
	rs := rows.Next()
	assert.True(t, rs)

	var id int64
	var name, columns string
	var totalColumns int64
	err = rows.Scan(&id, &name, &columns, &totalColumns)
	assert.NoError(t, err)
	assert.Equal(t, "test_table", name)
	assert.Equal(t, int64(2), totalColumns)
}

func TestSpecialCharactersInColumnNames(t *testing.T) {
	storage, err := duckdb.NewDuckDBStorage("")
	assert.NoError(t, err)
	defer storage.Close()

	// Column names with special characters
	columns := []string{"column-with-dash", "column.with.dot", "column with space"}
	err = storage.BuildStructure("test", columns)
	assert.NoError(t, err)

	err = storage.InsertRow("test", columns, []any{"value1", "value2", "value3"})
	assert.NoError(t, err)

	rows, err := storage.Query("SELECT * FROM test;")
	assert.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	assert.NoError(t, err)
	assert.Equal(t, columns, cols)
}

func TestMultipleTables(t *testing.T) {
	storage, err := duckdb.NewDuckDBStorage("")
	assert.NoError(t, err)
	defer storage.Close()

	// Create first table
	err = storage.BuildStructure("users", []string{"id", "name"})
	assert.NoError(t, err)

	// Create second table
	err = storage.BuildStructure("orders", []string{"id", "user_id", "product"})
	assert.NoError(t, err)

	// Insert data
	err = storage.InsertRow("users", []string{"id", "name"}, []any{"1", "Alice"})
	assert.NoError(t, err)

	err = storage.InsertRow("orders", []string{"id", "user_id", "product"}, []any{"100", "1", "Laptop"})
	assert.NoError(t, err)

	// Query with JOIN
	rows, err := storage.Query(`
		SELECT u.name, o.product
		FROM users u
		JOIN orders o ON u.id = o.user_id;
	`)
	assert.NoError(t, err)
	defer rows.Close()

	rs := rows.Next()
	assert.True(t, rs)

	var name, product string
	err = rows.Scan(&name, &product)
	assert.NoError(t, err)
	assert.Equal(t, "Alice", name)
	assert.Equal(t, "Laptop", product)
}
