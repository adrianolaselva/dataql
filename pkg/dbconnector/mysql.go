package dbconnector

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLConnector implements the Connector interface for MySQL
type MySQLConnector struct {
	config Config
	db     *sql.DB
}

// NewMySQLConnector creates a new MySQL connector
func NewMySQLConnector(config Config) (*MySQLConnector, error) {
	return &MySQLConnector{
		config: config,
	}, nil
}

// Connect establishes a connection to the MySQL database
func (m *MySQLConnector) Connect() error {
	connStr := m.buildConnectionString()

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	m.db = db
	return nil
}

// buildConnectionString builds the MySQL connection string (DSN)
func (m *MySQLConnector) buildConnectionString() string {
	// Format: user:password@tcp(host:port)/dbname?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true
	// charset=utf8mb4: Ensures proper UTF-8 encoding (prevents binary []byte returns for VARCHAR)
	// collation=utf8mb4_unicode_ci: Standard Unicode collation for utf8mb4
	// parseTime=true: Parses DATE and DATETIME to time.Time instead of []byte
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		m.config.User,
		m.config.Password,
		m.config.Host,
		m.config.Port,
		m.config.Database,
	)
}

// Close closes the database connection
func (m *MySQLConnector) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// ListTables lists all tables in the database
func (m *MySQLConnector) ListTables() ([]string, error) {
	if m.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	query := "SHOW TABLES"

	rows, err := m.db.Query(query)
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
func (m *MySQLConnector) GetTableSchema(tableName string) ([]ColumnInfo, error) {
	if m.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	query := fmt.Sprintf("DESCRIBE %s", quoteIdentifierMySQL(tableName))

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var nullable, key, defaultVal, extra sql.NullString
		if err := rows.Scan(&col.Name, &col.DataType, &nullable, &key, &defaultVal, &extra); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		col.Nullable = nullable.Valid && nullable.String == "YES"
		columns = append(columns, col)
	}

	return columns, nil
}

// ReadTable reads all rows from a table
func (m *MySQLConnector) ReadTable(tableName string, limit int) (*sql.Rows, error) {
	if m.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	query := fmt.Sprintf("SELECT * FROM %s", quoteIdentifierMySQL(tableName))
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}

	return rows, nil
}

// Query executes a custom query
func (m *MySQLConnector) Query(query string) (*sql.Rows, error) {
	if m.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

// CreateTable creates a new table
func (m *MySQLConnector) CreateTable(tableName string, columns []ColumnInfo) error {
	if m.db == nil {
		return fmt.Errorf("database not connected")
	}

	var colDefs []string
	for _, col := range columns {
		dataType := mapToMySQLType(col.DataType)
		nullability := ""
		if !col.Nullable {
			nullability = " NOT NULL"
		}
		colDefs = append(colDefs, fmt.Sprintf("%s %s%s", quoteIdentifierMySQL(col.Name), dataType, nullability))
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		quoteIdentifierMySQL(tableName),
		strings.Join(colDefs, ", "))

	_, err := m.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// InsertRow inserts a row into a table
func (m *MySQLConnector) InsertRow(tableName string, columns []string, values []any) error {
	if m.db == nil {
		return fmt.Errorf("database not connected")
	}

	// Build column list
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = quoteIdentifierMySQL(col)
	}

	// Build placeholder list (?, ?, ...)
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifierMySQL(tableName),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "))

	_, err := m.db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	return nil
}

// quoteIdentifierMySQL quotes an identifier for safe use in MySQL
func quoteIdentifierMySQL(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// mapToMySQLType maps a generic type to a MySQL type
func mapToMySQLType(dataType string) string {
	switch strings.ToLower(dataType) {
	case "text", "string", "varchar":
		return "TEXT"
	case "integer", "int", "int32":
		return "INT"
	case "bigint", "int64":
		return "BIGINT"
	case "float", "double", "float64":
		return "DOUBLE"
	case "boolean", "bool":
		return "TINYINT(1)"
	case "timestamp", "datetime":
		return "DATETIME"
	case "date":
		return "DATE"
	default:
		return "TEXT" // Default to TEXT
	}
}
