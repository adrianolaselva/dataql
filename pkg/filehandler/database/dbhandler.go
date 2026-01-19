package database

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/adrianolaselva/dataql/pkg/dbconnector"
	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/schollz/progressbar/v3"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_ ]+`)

// ConnectionInfo holds parsed connection information
type ConnectionInfo struct {
	Type     dbconnector.DBType
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Table    string
	SSLMode  string
}

type dbHandler struct {
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	connInfo    ConnectionInfo
	totalLines  int
	limitLines  int
	currentLine int
	collection  string
}

// NewDBHandler creates a new database file handler
func NewDBHandler(connInfo ConnectionInfo, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &dbHandler{
		connInfo:   connInfo,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
	}
}

// Import imports data from the database
func (d *dbHandler) Import() error {
	// Create connector
	config := dbconnector.Config{
		Type:     d.connInfo.Type,
		Host:     d.connInfo.Host,
		Port:     d.connInfo.Port,
		User:     d.connInfo.User,
		Password: d.connInfo.Password,
		Database: d.connInfo.Database,
		SSLMode:  d.connInfo.SSLMode,
	}

	connector, err := dbconnector.NewConnector(config)
	if err != nil {
		return fmt.Errorf("failed to create connector: %w", err)
	}
	defer connector.Close()

	if err := connector.Connect(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// Get table name
	tableName := d.connInfo.Table
	if d.collection != "" {
		tableName = d.collection
	}

	// Get table schema
	schema, err := connector.GetTableSchema(d.connInfo.Table)
	if err != nil {
		return fmt.Errorf("failed to get table schema: %w", err)
	}

	if len(schema) == 0 {
		// Empty table - create placeholder
		if err := d.storage.BuildStructure(d.sanitizeTableName(tableName), []string{"_empty"}); err != nil {
			return fmt.Errorf("failed to build structure for empty table: %w", err)
		}
		return nil
	}

	// Convert schema to column names
	columns := make([]string, len(schema))
	for i, col := range schema {
		columns[i] = d.sanitizeColumnName(col.Name)
	}

	// Build table structure
	if err := d.storage.BuildStructure(d.sanitizeTableName(tableName), columns); err != nil {
		return fmt.Errorf("failed to build structure: %w", err)
	}

	// Read data from the table
	rows, err := connector.ReadTable(d.connInfo.Table, d.limitLines)
	if err != nil {
		return fmt.Errorf("failed to read table: %w", err)
	}
	defer rows.Close()

	// Get column types for proper scanning
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to get column types: %w", err)
	}

	// Prepare row scanning
	scanValues := make([]interface{}, len(colTypes))
	for i := range scanValues {
		scanValues[i] = new(interface{})
	}

	// Read and insert rows
	rowCount := 0
	for rows.Next() {
		if d.limitLines > 0 && rowCount >= d.limitLines {
			break
		}

		if err := rows.Scan(scanValues...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		values := make([]any, len(columns))
		for i, sv := range scanValues {
			val := *(sv.(*interface{}))
			if val == nil {
				values[i] = ""
			} else if bytes, ok := val.([]byte); ok {
				// Convert []byte to string for proper handling of VARCHAR/TEXT columns
				// This is needed as some database drivers return text as []byte
				values[i] = string(bytes)
			} else {
				values[i] = fmt.Sprintf("%v", val)
			}
		}

		if err := d.storage.InsertRow(d.sanitizeTableName(tableName), columns, values); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}

		rowCount++
		d.currentLine++
		_ = d.bar.Add(1)
	}

	d.totalLines = rowCount
	return nil
}

// sanitizeColumnName sanitizes a string to be used as a SQL column name
func (d *dbHandler) sanitizeColumnName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// sanitizeTableName sanitizes a table name
func (d *dbHandler) sanitizeTableName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// Lines returns total lines count
func (d *dbHandler) Lines() int {
	return d.totalLines
}

// Close cleans up resources
func (d *dbHandler) Close() error {
	return nil
}

// ParseDatabaseURL parses a database URL and returns connection info
// Format: postgres://user:password@host:port/database/table
//
//	mysql://user:password@host:port/database/table
func ParseDatabaseURL(urlStr string) (*ConnectionInfo, error) {
	var dbType dbconnector.DBType
	var rest string

	if strings.HasPrefix(urlStr, "postgres://") || strings.HasPrefix(urlStr, "postgresql://") {
		dbType = dbconnector.DBTypePostgres
		rest = strings.TrimPrefix(strings.TrimPrefix(urlStr, "postgres://"), "postgresql://")
	} else if strings.HasPrefix(urlStr, "mysql://") {
		dbType = dbconnector.DBTypeMySQL
		rest = strings.TrimPrefix(urlStr, "mysql://")
	} else if strings.HasPrefix(urlStr, "duckdb://") {
		rest = strings.TrimPrefix(urlStr, "duckdb://")
		return parseDuckDBURL(rest)
	} else {
		return nil, fmt.Errorf("unsupported database URL scheme")
	}

	info := &ConnectionInfo{
		Type:    dbType,
		Port:    5432, // Default PostgreSQL port
		SSLMode: "disable",
	}

	if dbType == dbconnector.DBTypeMySQL {
		info.Port = 3306 // Default MySQL port
	}

	// Parse user:password@host:port/database/table
	if idx := strings.Index(rest, "@"); idx != -1 {
		userPass := rest[:idx]
		rest = rest[idx+1:]

		if colonIdx := strings.Index(userPass, ":"); colonIdx != -1 {
			info.User = userPass[:colonIdx]
			info.Password = userPass[colonIdx+1:]
		} else {
			info.User = userPass
		}
	}

	// Parse host:port/database/table
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid database URL: missing database name")
	}

	hostPort := parts[0]
	info.Database = parts[1]

	if len(parts) >= 3 {
		info.Table = parts[2]
	}

	// Parse host:port
	if colonIdx := strings.LastIndex(hostPort, ":"); colonIdx != -1 {
		info.Host = hostPort[:colonIdx]
		_, _ = fmt.Sscanf(hostPort[colonIdx+1:], "%d", &info.Port)
	} else {
		info.Host = hostPort
	}

	return info, nil
}

// parseDuckDBURL parses a DuckDB URL
// Format: duckdb:///path/to/file.db/table or duckdb://:memory:/table
func parseDuckDBURL(rest string) (*ConnectionInfo, error) {
	info := &ConnectionInfo{
		Type: dbconnector.DBTypeDuckDB,
	}

	// Handle :memory: special case
	if strings.HasPrefix(rest, ":memory:") {
		info.Database = ":memory:"
		rest = strings.TrimPrefix(rest, ":memory:")
		if strings.HasPrefix(rest, "/") {
			info.Table = strings.TrimPrefix(rest, "/")
		}
		if info.Table == "" {
			return nil, fmt.Errorf("invalid DuckDB URL: missing table name (format: duckdb://:memory:/table)")
		}
		return info, nil
	}

	// Regular file path: /path/to/file.db/table
	// Look for .db or .duckdb extension to find the database file boundary
	dbExt := -1
	for _, ext := range []string{".duckdb", ".db"} {
		idx := strings.Index(rest, ext)
		if idx != -1 {
			dbExt = idx + len(ext)
			break
		}
	}

	if dbExt == -1 {
		return nil, fmt.Errorf("invalid DuckDB URL: database file must have .db or .duckdb extension")
	}

	info.Database = rest[:dbExt]

	// The rest after the .db extension should be /tablename
	remaining := rest[dbExt:]
	if !strings.HasPrefix(remaining, "/") || len(remaining) <= 1 {
		return nil, fmt.Errorf("invalid DuckDB URL: missing table name (format: duckdb:///path/to/file.db/table)")
	}

	info.Table = strings.TrimPrefix(remaining, "/")

	if info.Table == "" {
		return nil, fmt.Errorf("invalid DuckDB URL: missing table name")
	}

	return info, nil
}

// IsDatabaseURL checks if a string is a database URL
func IsDatabaseURL(str string) bool {
	return strings.HasPrefix(str, "postgres://") ||
		strings.HasPrefix(str, "postgresql://") ||
		strings.HasPrefix(str, "mysql://") ||
		strings.HasPrefix(str, "duckdb://")
}
