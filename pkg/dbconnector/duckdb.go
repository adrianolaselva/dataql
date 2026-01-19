//go:build !noduckdb

package dbconnector

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

// DuckDBConnector implements the Connector interface for DuckDB
type DuckDBConnector struct {
	config Config
	db     *sql.DB
}

// NewDuckDBConnector creates a new DuckDB connector
func NewDuckDBConnector(config Config) (*DuckDBConnector, error) {
	return &DuckDBConnector{
		config: config,
	}, nil
}

// Connect establishes a connection to DuckDB
func (d *DuckDBConnector) Connect() error {
	// DuckDB connection string is just the path to the database file
	// or empty/:memory: for in-memory database
	connStr := d.config.Database
	if connStr == "" || connStr == ":memory:" {
		connStr = ""
	}

	db, err := sql.Open("duckdb", connStr)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping DuckDB: %w", err)
	}

	d.db = db
	return nil
}

// Close closes the database connection
func (d *DuckDBConnector) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// ListTables lists all tables in the database
func (d *DuckDBConnector) ListTables() ([]string, error) {
	if d.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// GetTableSchema returns the schema for a table
func (d *DuckDBConnector) GetTableSchema(tableName string) ([]ColumnInfo, error) {
	if d.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = 'main' AND table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := d.db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var nullable string
		if err := rows.Scan(&col.Name, &col.DataType, &nullable); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		col.Nullable = nullable == "YES"
		columns = append(columns, col)
	}

	return columns, nil
}

// ReadTable reads all rows from a table
func (d *DuckDBConnector) ReadTable(tableName string, limit int) (*sql.Rows, error) {
	if d.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	query := fmt.Sprintf("SELECT * FROM %s", quoteIdentifierDuckDB(tableName))
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}

	return rows, nil
}

// Query executes a custom query
func (d *DuckDBConnector) Query(query string) (*sql.Rows, error) {
	if d.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

// CreateTable creates a new table
func (d *DuckDBConnector) CreateTable(tableName string, columns []ColumnInfo) error {
	if d.db == nil {
		return fmt.Errorf("database not connected")
	}

	var colDefs []string
	for _, col := range columns {
		dataType := mapToDuckDBType(col.DataType)
		nullability := ""
		if !col.Nullable {
			nullability = " NOT NULL"
		}
		colDefs = append(colDefs, fmt.Sprintf("%s %s%s", quoteIdentifierDuckDB(col.Name), dataType, nullability))
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		quoteIdentifierDuckDB(tableName),
		strings.Join(colDefs, ", "))

	_, err := d.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// InsertRow inserts a row into a table
func (d *DuckDBConnector) InsertRow(tableName string, columns []string, values []any) error {
	if d.db == nil {
		return fmt.Errorf("database not connected")
	}

	// Build column list
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = quoteIdentifierDuckDB(col)
	}

	// Build placeholder list ($1, $2, ...)
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifierDuckDB(tableName),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "))

	_, err := d.db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	return nil
}

// quoteIdentifierDuckDB quotes an identifier for safe use in DuckDB
func quoteIdentifierDuckDB(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// mapToDuckDBType maps a generic type to a DuckDB type
func mapToDuckDBType(dataType string) string {
	switch strings.ToLower(dataType) {
	case "text", "string", "varchar":
		return "VARCHAR"
	case "integer", "int", "int32":
		return "INTEGER"
	case "bigint", "int64":
		return "BIGINT"
	case "float", "double", "float64":
		return "DOUBLE"
	case "boolean", "bool":
		return "BOOLEAN"
	case "timestamp", "datetime":
		return "TIMESTAMP"
	case "date":
		return "DATE"
	default:
		return "VARCHAR" // Default to VARCHAR
	}
}
