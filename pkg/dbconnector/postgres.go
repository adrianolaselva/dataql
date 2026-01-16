package dbconnector

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// PostgresConnector implements the Connector interface for PostgreSQL
type PostgresConnector struct {
	config Config
	db     *sql.DB
}

// NewPostgresConnector creates a new PostgreSQL connector
func NewPostgresConnector(config Config) (*PostgresConnector, error) {
	return &PostgresConnector{
		config: config,
	}, nil
}

// Connect establishes a connection to the PostgreSQL database
func (p *PostgresConnector) Connect() error {
	connStr := p.buildConnectionString()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	p.db = db
	return nil
}

// buildConnectionString builds the PostgreSQL connection string
func (p *PostgresConnector) buildConnectionString() string {
	sslMode := p.config.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.config.Host,
		p.config.Port,
		p.config.User,
		p.config.Password,
		p.config.Database,
		sslMode,
	)
}

// Close closes the database connection
func (p *PostgresConnector) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// ListTables lists all tables in the database
func (p *PostgresConnector) ListTables() ([]string, error) {
	if p.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	rows, err := p.db.Query(query)
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
func (p *PostgresConnector) GetTableSchema(tableName string) ([]ColumnInfo, error) {
	if p.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := p.db.Query(query, tableName)
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
func (p *PostgresConnector) ReadTable(tableName string, limit int) (*sql.Rows, error) {
	if p.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	query := fmt.Sprintf("SELECT * FROM %s", quoteIdentifier(tableName))
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := p.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}

	return rows, nil
}

// Query executes a custom query
func (p *PostgresConnector) Query(query string) (*sql.Rows, error) {
	if p.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	rows, err := p.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

// CreateTable creates a new table
func (p *PostgresConnector) CreateTable(tableName string, columns []ColumnInfo) error {
	if p.db == nil {
		return fmt.Errorf("database not connected")
	}

	var colDefs []string
	for _, col := range columns {
		dataType := mapToPostgresType(col.DataType)
		nullability := ""
		if !col.Nullable {
			nullability = " NOT NULL"
		}
		colDefs = append(colDefs, fmt.Sprintf("%s %s%s", quoteIdentifier(col.Name), dataType, nullability))
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		quoteIdentifier(tableName),
		strings.Join(colDefs, ", "))

	_, err := p.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// InsertRow inserts a row into a table
func (p *PostgresConnector) InsertRow(tableName string, columns []string, values []any) error {
	if p.db == nil {
		return fmt.Errorf("database not connected")
	}

	// Build column list
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = quoteIdentifier(col)
	}

	// Build placeholder list ($1, $2, ...)
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(tableName),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "))

	_, err := p.db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	return nil
}

// quoteIdentifier quotes an identifier for safe use in SQL
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// mapToPostgresType maps a generic type to a PostgreSQL type
func mapToPostgresType(dataType string) string {
	switch strings.ToLower(dataType) {
	case "text", "string", "varchar":
		return "TEXT"
	case "integer", "int", "int32":
		return "INTEGER"
	case "bigint", "int64":
		return "BIGINT"
	case "float", "double", "float64":
		return "DOUBLE PRECISION"
	case "boolean", "bool":
		return "BOOLEAN"
	case "timestamp", "datetime":
		return "TIMESTAMP"
	case "date":
		return "DATE"
	default:
		return "TEXT" // Default to TEXT
	}
}
