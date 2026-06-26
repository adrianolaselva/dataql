package database

import (
	"testing"

	"github.com/adrianolaselva/dataql/pkg/dbconnector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDatabaseURL_Postgres(t *testing.T) {
	info, err := ParseDatabaseURL("postgres://user:secret@localhost:5432/mydb/users")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, dbconnector.DBTypePostgres, info.Type)
	assert.Equal(t, "localhost", info.Host)
	assert.Equal(t, 5432, info.Port)
	assert.Equal(t, "user", info.User)
	assert.Equal(t, "secret", info.Password)
	assert.Equal(t, "mydb", info.Database)
	assert.Equal(t, "users", info.Table)
	assert.Equal(t, "disable", info.SSLMode)
}

func TestParseDatabaseURL_PostgresqlScheme(t *testing.T) {
	info, err := ParseDatabaseURL("postgresql://user:secret@db.example.com:6543/analytics/events")
	require.NoError(t, err)
	assert.Equal(t, dbconnector.DBTypePostgres, info.Type)
	assert.Equal(t, "db.example.com", info.Host)
	assert.Equal(t, 6543, info.Port)
	assert.Equal(t, "analytics", info.Database)
	assert.Equal(t, "events", info.Table)
}

func TestParseDatabaseURL_PostgresDefaultPort(t *testing.T) {
	// No explicit port -> default PostgreSQL port preserved.
	info, err := ParseDatabaseURL("postgres://user:secret@localhost/mydb/users")
	require.NoError(t, err)
	assert.Equal(t, "localhost", info.Host)
	assert.Equal(t, 5432, info.Port)
}

func TestParseDatabaseURL_PostgresUserWithoutPassword(t *testing.T) {
	info, err := ParseDatabaseURL("postgres://admin@localhost:5432/mydb/users")
	require.NoError(t, err)
	assert.Equal(t, "admin", info.User)
	assert.Equal(t, "", info.Password)
}

func TestParseDatabaseURL_PostgresNoCredentials(t *testing.T) {
	info, err := ParseDatabaseURL("postgres://localhost:5432/mydb/users")
	require.NoError(t, err)
	assert.Equal(t, "", info.User)
	assert.Equal(t, "", info.Password)
	assert.Equal(t, "localhost", info.Host)
	assert.Equal(t, "mydb", info.Database)
	assert.Equal(t, "users", info.Table)
}

func TestParseDatabaseURL_PostgresWithoutTable(t *testing.T) {
	// Table is optional; database must be present.
	info, err := ParseDatabaseURL("postgres://user:secret@localhost:5432/mydb")
	require.NoError(t, err)
	assert.Equal(t, "mydb", info.Database)
	assert.Equal(t, "", info.Table)
}

func TestParseDatabaseURL_MySQL(t *testing.T) {
	info, err := ParseDatabaseURL("mysql://root:pass@127.0.0.1:3306/shop/orders")
	require.NoError(t, err)
	assert.Equal(t, dbconnector.DBTypeMySQL, info.Type)
	assert.Equal(t, "127.0.0.1", info.Host)
	assert.Equal(t, 3306, info.Port)
	assert.Equal(t, "root", info.User)
	assert.Equal(t, "pass", info.Password)
	assert.Equal(t, "shop", info.Database)
	assert.Equal(t, "orders", info.Table)
}

func TestParseDatabaseURL_MySQLDefaultPort(t *testing.T) {
	info, err := ParseDatabaseURL("mysql://root:pass@127.0.0.1/shop/orders")
	require.NoError(t, err)
	assert.Equal(t, dbconnector.DBTypeMySQL, info.Type)
	assert.Equal(t, 3306, info.Port)
}

func TestParseDatabaseURL_UnsupportedScheme(t *testing.T) {
	tests := []string{
		"redis://localhost:6379/0",
		"sqlite:///tmp/data.db/users",
		"",
		"not-a-url",
		"http://example.com/db/table",
	}
	for _, urlStr := range tests {
		t.Run(urlStr, func(t *testing.T) {
			info, err := ParseDatabaseURL(urlStr)
			require.Error(t, err)
			assert.Nil(t, info)
			assert.Contains(t, err.Error(), "unsupported database URL scheme")
		})
	}
}

func TestParseDatabaseURL_MissingDatabase(t *testing.T) {
	// Only host present, no /database segment.
	info, err := ParseDatabaseURL("postgres://user:secret@localhost:5432")
	require.Error(t, err)
	assert.Nil(t, info)
	assert.Contains(t, err.Error(), "missing database name")
}

func TestParseDatabaseURL_DuckDBFilePath(t *testing.T) {
	info, err := ParseDatabaseURL("duckdb:///path/to/file.db/mytable")
	require.NoError(t, err)
	assert.Equal(t, dbconnector.DBTypeDuckDB, info.Type)
	assert.Equal(t, "/path/to/file.db", info.Database)
	assert.Equal(t, "mytable", info.Table)
}

func TestParseDatabaseURL_DuckDBDuckdbExtension(t *testing.T) {
	info, err := ParseDatabaseURL("duckdb:///data/warehouse.duckdb/sales")
	require.NoError(t, err)
	assert.Equal(t, dbconnector.DBTypeDuckDB, info.Type)
	assert.Equal(t, "/data/warehouse.duckdb", info.Database)
	assert.Equal(t, "sales", info.Table)
}

func TestParseDatabaseURL_DuckDBMemory(t *testing.T) {
	info, err := ParseDatabaseURL("duckdb://:memory:/mytable")
	require.NoError(t, err)
	assert.Equal(t, dbconnector.DBTypeDuckDB, info.Type)
	assert.Equal(t, ":memory:", info.Database)
	assert.Equal(t, "mytable", info.Table)
}

func TestParseDatabaseURL_DuckDBMemoryMissingTable(t *testing.T) {
	info, err := ParseDatabaseURL("duckdb://:memory:")
	require.Error(t, err)
	assert.Nil(t, info)
	assert.Contains(t, err.Error(), "missing table name")
}

func TestParseDatabaseURL_DuckDBMemoryMissingTableTrailingSlash(t *testing.T) {
	info, err := ParseDatabaseURL("duckdb://:memory:/")
	require.Error(t, err)
	assert.Nil(t, info)
	assert.Contains(t, err.Error(), "missing table name")
}

func TestParseDatabaseURL_DuckDBMissingExtension(t *testing.T) {
	info, err := ParseDatabaseURL("duckdb:///path/to/file/mytable")
	require.Error(t, err)
	assert.Nil(t, info)
	assert.Contains(t, err.Error(), ".db or .duckdb extension")
}

func TestParseDatabaseURL_DuckDBMissingTableName(t *testing.T) {
	// Has the file extension but no /table after it.
	info, err := ParseDatabaseURL("duckdb:///path/to/file.db")
	require.Error(t, err)
	assert.Nil(t, info)
	assert.Contains(t, err.Error(), "missing table name")
}

func TestParseDatabaseURL_DuckDBTrailingSlashNoTable(t *testing.T) {
	info, err := ParseDatabaseURL("duckdb:///path/to/file.db/")
	require.Error(t, err)
	assert.Nil(t, info)
	assert.Contains(t, err.Error(), "missing table name")
}

func TestParseDuckDBURL_Direct(t *testing.T) {
	// Exercise parseDuckDBURL directly (unexported).
	info, err := parseDuckDBURL("/var/lib/app.duckdb/records")
	require.NoError(t, err)
	assert.Equal(t, dbconnector.DBTypeDuckDB, info.Type)
	assert.Equal(t, "/var/lib/app.duckdb", info.Database)
	assert.Equal(t, "records", info.Table)
}

func TestIsDatabaseURL(t *testing.T) {
	tests := []struct {
		name string
		str  string
		want bool
	}{
		{"postgres", "postgres://localhost/db/t", true},
		{"postgresql", "postgresql://localhost/db/t", true},
		{"mysql", "mysql://localhost/db/t", true},
		{"duckdb", "duckdb:///file.db/t", true},
		{"plain path", "/path/to/file.csv", false},
		{"http", "http://example.com", false},
		{"empty", "", false},
		{"redis", "redis://localhost", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsDatabaseURL(tt.str))
		})
	}
}

func TestSanitizeColumnName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple", "name", "name"},
		{"uppercase", "FirstName", "firstname"},
		{"spaces", "first name", "first_name"},
		{"dots", "user.email", "user_email"},
		{"dashes", "created-at", "created_at"},
		{"trim", "  padded  ", "padded"},
		{"special chars stripped", "price($)", "price"},
		{"mixed", "User Full-Name.value", "user_full_name_value"},
		{"keeps digits and underscore", "col_1", "col_1"},
		{"only special", "@#$%", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &dbHandler{}
			assert.Equal(t, tt.want, d.sanitizeColumnName(tt.input))
		})
	}
}

func TestSanitizeTableName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple", "users", "users"},
		{"uppercase", "Orders", "orders"},
		{"spaces", "order items", "order_items"},
		{"trim", "  spaced  ", "spaced"},
		{"special chars stripped", "tbl!@#", "tbl"},
		{"mixed", "My Table#1", "my_table1"},
		{"dots kept removed", "schema.table", "schematable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &dbHandler{}
			assert.Equal(t, tt.want, d.sanitizeTableName(tt.input))
		})
	}
}
