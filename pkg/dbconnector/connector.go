package dbconnector

import (
	"database/sql"
	"fmt"
)

// DBType represents the type of database
type DBType string

const (
	DBTypePostgres DBType = "postgres"
	DBTypeMySQL    DBType = "mysql"
	DBTypeDuckDB   DBType = "duckdb"
	DBTypeMongoDB  DBType = "mongodb"
)

// Connector interface for database operations
type Connector interface {
	Connect() error
	Close() error
	ListTables() ([]string, error)
	GetTableSchema(tableName string) ([]ColumnInfo, error)
	ReadTable(tableName string, limit int) (*sql.Rows, error)
	Query(query string) (*sql.Rows, error)
	CreateTable(tableName string, columns []ColumnInfo) error
	InsertRow(tableName string, columns []string, values []any) error
}

// ColumnInfo represents column metadata
type ColumnInfo struct {
	Name     string
	DataType string
	Nullable bool
}

// Config holds database connection configuration
type Config struct {
	Type     DBType
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// NewConnector creates a new database connector based on the type
func NewConnector(config Config) (Connector, error) {
	switch config.Type {
	case DBTypePostgres:
		return NewPostgresConnector(config)
	case DBTypeMySQL:
		return NewMySQLConnector(config)
	case DBTypeDuckDB:
		return NewDuckDBConnector(config)
	case DBTypeMongoDB:
		return NewMongoDBConnector(config)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}
}

// ParseConnectionString parses a connection string into Config
func ParseConnectionString(connStr string, dbType DBType) (*Config, error) {
	// For now, we'll use the connection string directly
	// In a more complete implementation, we would parse the string
	return &Config{
		Type: dbType,
	}, nil
}
