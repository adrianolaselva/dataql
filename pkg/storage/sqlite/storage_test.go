package sqlite_test

import (
	"github.com/adrianolaselva/dataql/pkg/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShouldBuildStructureWithSuccess(t *testing.T) {
	tests := []struct {
		columns       []string
		query         string
		rows          [][]any
		columnExpects []string
		rowsExpects   [][]any
	}{
		{
			columns: []string{"column_1", "column_2"},
			query:   "select * from rows;",
			rows: [][]any{
				{"value_1", "value_2"},
			},
			columnExpects: []string{"column_1", "column_2"},
			rowsExpects: [][]any{
				{"value_1", "value_2"},
			},
		},
		{
			columns: []string{"column_1", "column_2", "column_3"},
			query:   "select * from rows;",
			rows: [][]any{
				{"value_1", "value_2", "value_3"},
			},
			columnExpects: []string{"column_1", "column_2", "column_3"},
			rowsExpects: [][]any{
				{"value_1", "value_2", "value_3"},
			},
		},
		{
			columns: []string{"column_1", "column_2", "column_3"},
			query:   "select * from rows;",
			rows: [][]any{
				{"value_3", "value_1", "value_2"},
			},
			columnExpects: []string{"column_1", "column_2", "column_3"},
			rowsExpects: [][]any{
				{"value_3", "value_1", "value_2"},
			},
		},
		{
			columns: []string{"column_1", "column_2", "column_3"},
			query:   "select column_3, column_1, column_2 from rows;",
			rows: [][]any{
				{"value_1", "value_2", "value_3"},
			},
			columnExpects: []string{"column_3", "column_1", "column_2"},
			rowsExpects: [][]any{
				{"value_3", "value_1", "value_2"},
			},
		},
		{
			columns: []string{"column_1", "column_2", "column_3"},
			query:   "select count(1) total from rows;",
			rows: [][]any{
				{"value_1", "value_2", "value_3"},
			},
			columnExpects: []string{"total"},
			rowsExpects: [][]any{
				{int64(1)},
			},
		},
		{
			columns: []string{"column_1", "column_2", "column_3"},
			query:   "select count(1) total from rows;",
			rows: [][]any{
				{"value_11", "value_21", "value_31"},
				{"value_12", "value_22", "value_32"},
				{"value_13", "value_23", "value_33"},
			},
			columnExpects: []string{"total"},
			rowsExpects: [][]any{
				{int64(3)},
			},
		},
		{
			columns: []string{"column_1"},
			query:   "select column_1, count(1) total from rows group by column_1;",
			rows: [][]any{
				{"Value Test"},
				{"Value Test"},
				{"Value Test"},
			},
			columnExpects: []string{"column_1", "total"},
			rowsExpects: [][]any{
				{"Value Test", int64(3)},
			},
		},
	}

	for _, test := range tests {
		storage, err := sqlite.NewSqLiteStorage(":memory:")
		assert.NoError(t, err)

		err = storage.BuildStructure("rows", test.columns)
		assert.NoError(t, err)

		for _, row := range test.rows {
			err = storage.InsertRow("rows", test.columns, row)
			assert.NoError(t, err)
		}

		rows, err := storage.Query(test.query)
		assert.NoError(t, err)

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

		err = rows.Close()
		assert.NoError(t, err)

		err = storage.Close()
		assert.NoError(t, err)
	}
}
