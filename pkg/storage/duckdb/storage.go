package duckdb

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/adrianolaselva/dataql/pkg/storage"
	_ "github.com/marcboeker/go-duckdb"
)

const (
	sqlCreateTableTemplate        = "CREATE TABLE IF NOT EXISTS %s (%s\n);"
	sqlInsertTemplate             = "INSERT INTO %s (%s) VALUES (%s);"
	sqlInsertDefaultTableTemplate = `INSERT INTO "schemas" ("id", "name", "columns", "total_columns") VALUES ((SELECT COALESCE(MAX(id), 0)+1 FROM "schemas"), $1, $2, $3);`
	sqlShowTablesTemplate         = `SELECT * FROM "schemas";`
	sqlDefaultTableTemplate       = `CREATE TABLE IF NOT EXISTS "schemas" ("id" INTEGER, "name" VARCHAR, "columns" VARCHAR, "total_columns" INTEGER);`
	dataSourceNameDefault         = ""
)

type duckDBStorage struct {
	db *sql.DB
}

// NewDuckDBStorage creates a new DuckDB storage instance.
// If datasource is empty or ":memory:", it creates an in-memory database.
// Otherwise, it creates a persistent database at the specified path.
func NewDuckDBStorage(datasource string) (storage.Storage, error) {
	// DuckDB uses empty string for in-memory, normalize :memory: for compatibility
	if datasource == ":memory:" {
		datasource = ""
	}

	db, err := sql.Open("duckdb", datasource)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection with duckdb: %w", err)
	}

	return &duckDBStorage{db: db}, nil
}

// BuildStructure creates a table with the given name and columns.
// All columns are created as VARCHAR type for flexibility.
// For better type support, use BuildStructureWithTypes instead.
func (s *duckDBStorage) BuildStructure(tableName string, columns []string) error {
	// Convert to ColumnDef with default VARCHAR type
	columnDefs := make([]storage.ColumnDef, len(columns))
	for i, col := range columns {
		columnDefs[i] = storage.ColumnDef{Name: col, Type: storage.TypeVarchar}
	}
	return s.BuildStructureWithTypes(tableName, columnDefs)
}

// BuildStructureWithTypes creates a table with typed columns.
// This allows for proper type handling in queries (e.g., numeric comparisons).
func (s *duckDBStorage) BuildStructureWithTypes(tableName string, columns []storage.ColumnDef) error {
	var tableAttrsRaw strings.Builder

	// Create quoted column names for SQL
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col.Name)
	}

	for i, col := range columns {
		colType := string(col.Type)
		if colType == "" {
			colType = "VARCHAR"
		}
		tableAttrsRaw.WriteString(fmt.Sprintf("\n\t%s %s", quotedColumns[i], colType))
		if len(columns)-1 > i {
			tableAttrsRaw.WriteString(",")
		}
	}

	query := fmt.Sprintf(sqlCreateTableTemplate, quoteIdentifier(tableName), tableAttrsRaw.String())
	if _, err := s.db.Exec(query); err != nil {
		return fmt.Errorf("failed to create structure: %w (sql: %s)", err, query)
	}

	if _, err := s.db.Exec(sqlDefaultTableTemplate); err != nil {
		return fmt.Errorf("failed to create tables schemas structure: %w", err)
	}

	columnsRaw := fmt.Sprintf("[%v]", strings.Join(quotedColumns, ","))
	if _, err := s.db.Exec(sqlInsertDefaultTableTemplate, tableName, columnsRaw, len(columns)); err != nil {
		return fmt.Errorf("failed to execute insert: %w", err)
	}

	return nil
}

// InsertRow inserts a row into the specified table.
func (s *duckDBStorage) InsertRow(tableName string, columns []string, values []any) error {
	// Quote column names for SQL
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}
	columnsRaw := strings.Join(quotedColumns, ", ")

	// DuckDB uses $1, $2, ... for placeholders
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	paramsRaw := strings.Join(placeholders, ", ")

	query := fmt.Sprintf(sqlInsertTemplate, quoteIdentifier(tableName), columnsRaw, paramsRaw)

	if _, err := s.db.Exec(query, values...); err != nil {
		return fmt.Errorf("failed to execute insert: %w (sql: %s)", err, query)
	}

	return nil
}

// InsertRowWithCoercion inserts a row into the specified table, attempting to coerce
// values to the expected column types. If coercion fails, the value becomes NULL.
// This method is more flexible than InsertRow when dealing with mixed type data.
func (s *duckDBStorage) InsertRowWithCoercion(tableName string, columns []string, values []any, columnDefs []storage.ColumnDef) error {
	coercedValues := make([]any, len(values))

	for i, val := range values {
		if i < len(columnDefs) {
			converted, ok := storage.TryConvertValue(val, columnDefs[i].Type)
			if ok {
				coercedValues[i] = converted
			} else {
				// Fallback to NULL if conversion fails
				coercedValues[i] = nil
			}
		} else {
			coercedValues[i] = val
		}
	}

	return s.InsertRow(tableName, columns, coercedValues)
}

// Query executes the given SQL query and returns the result rows.
func (s *duckDBStorage) Query(cmd string) (*sql.Rows, error) {
	rows, err := s.db.Query(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

// ShowTables returns the metadata about all loaded tables.
func (s *duckDBStorage) ShowTables() (*sql.Rows, error) {
	rows, err := s.db.Query(sqlShowTablesTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

// Close closes the database connection.
func (s *duckDBStorage) Close() error {
	err := s.db.Close()
	if err != nil {
		return fmt.Errorf("failed to close duckdb connection: %w", err)
	}

	return nil
}

// quoteIdentifier quotes an identifier (table or column name) for safe use in DuckDB.
// DuckDB uses double quotes for identifiers.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
