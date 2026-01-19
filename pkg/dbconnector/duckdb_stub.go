//go:build noduckdb

package dbconnector

import (
	"database/sql"
	"fmt"
)

// DuckDBConnector is a stub when DuckDB support is not compiled
type DuckDBConnector struct {
	config Config
}

// NewDuckDBConnector returns an error when DuckDB is not compiled
func NewDuckDBConnector(config Config) (*DuckDBConnector, error) {
	return nil, fmt.Errorf("DuckDB support is not available in this build. Build from source without -tags noduckdb to enable DuckDB")
}

// Connect is not available in stub
func (d *DuckDBConnector) Connect() error {
	return fmt.Errorf("DuckDB not available")
}

// Close is not available in stub
func (d *DuckDBConnector) Close() error {
	return nil
}

// ListTables is not available in stub
func (d *DuckDBConnector) ListTables() ([]string, error) {
	return nil, fmt.Errorf("DuckDB not available")
}

// GetTableSchema is not available in stub
func (d *DuckDBConnector) GetTableSchema(tableName string) ([]ColumnInfo, error) {
	return nil, fmt.Errorf("DuckDB not available")
}

// ReadTable is not available in stub
func (d *DuckDBConnector) ReadTable(tableName string, limit int) (*sql.Rows, error) {
	return nil, fmt.Errorf("DuckDB not available")
}

// Query is not available in stub
func (d *DuckDBConnector) Query(query string) (*sql.Rows, error) {
	return nil, fmt.Errorf("DuckDB not available")
}

// CreateTable is not available in stub
func (d *DuckDBConnector) CreateTable(tableName string, columns []ColumnInfo) error {
	return fmt.Errorf("DuckDB not available")
}

// InsertRow is not available in stub
func (d *DuckDBConnector) InsertRow(tableName string, columns []string, values []any) error {
	return fmt.Errorf("DuckDB not available")
}
