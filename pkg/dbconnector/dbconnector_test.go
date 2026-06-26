package dbconnector

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConnectionString(t *testing.T) {
	tests := []struct {
		name    string
		connStr string
		dbType  DBType
	}{
		{name: "postgres", connStr: "postgres://localhost/db", dbType: DBTypePostgres},
		{name: "mysql", connStr: "mysql://localhost/db", dbType: DBTypeMySQL},
		{name: "duckdb", connStr: ":memory:", dbType: DBTypeDuckDB},
		{name: "empty string", connStr: "", dbType: DBTypeDynamoDB},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseConnectionString(tt.connStr, tt.dbType)
			require.NoError(t, err)
			require.NotNil(t, cfg)
			assert.Equal(t, tt.dbType, cfg.Type)
		})
	}
}

func TestNewConnector_UnsupportedType(t *testing.T) {
	_, err := NewConnector(Config{Type: DBType("unknown")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported database type")
}

func TestNewConnector_SupportedTypes(t *testing.T) {
	// These constructors only build the struct; they do not open a connection.
	for _, dbType := range []DBType{DBTypePostgres, DBTypeMySQL, DBTypeDuckDB} {
		t.Run(string(dbType), func(t *testing.T) {
			conn, err := NewConnector(Config{Type: dbType})
			require.NoError(t, err)
			require.NotNil(t, conn)
		})
	}
}

func TestMapToPostgresType(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"text", "TEXT"},
		{"string", "TEXT"},
		{"varchar", "TEXT"},
		{"VARCHAR", "TEXT"},
		{"integer", "INTEGER"},
		{"int", "INTEGER"},
		{"int32", "INTEGER"},
		{"bigint", "BIGINT"},
		{"int64", "BIGINT"},
		{"float", "DOUBLE PRECISION"},
		{"double", "DOUBLE PRECISION"},
		{"float64", "DOUBLE PRECISION"},
		{"boolean", "BOOLEAN"},
		{"bool", "BOOLEAN"},
		{"timestamp", "TIMESTAMP"},
		{"datetime", "TIMESTAMP"},
		{"date", "DATE"},
		{"unknown_type", "TEXT"},
		{"", "TEXT"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, mapToPostgresType(tt.in))
		})
	}
}

func TestMapToDuckDBType(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"text", "VARCHAR"},
		{"string", "VARCHAR"},
		{"varchar", "VARCHAR"},
		{"integer", "INTEGER"},
		{"int", "INTEGER"},
		{"int32", "INTEGER"},
		{"bigint", "BIGINT"},
		{"int64", "BIGINT"},
		{"float", "DOUBLE"},
		{"double", "DOUBLE"},
		{"float64", "DOUBLE"},
		{"boolean", "BOOLEAN"},
		{"bool", "BOOLEAN"},
		{"timestamp", "TIMESTAMP"},
		{"datetime", "TIMESTAMP"},
		{"date", "DATE"},
		{"unknown_type", "VARCHAR"},
		{"", "VARCHAR"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, mapToDuckDBType(tt.in))
		})
	}
}

func TestQuoteIdentifierPostgres(t *testing.T) {
	assert.Equal(t, `"col"`, quoteIdentifier("col"))
	assert.Equal(t, `"a""b"`, quoteIdentifier(`a"b`))
}

func TestQuoteIdentifierDuckDB(t *testing.T) {
	assert.Equal(t, `"col"`, quoteIdentifierDuckDB("col"))
	assert.Equal(t, `"a""b"`, quoteIdentifierDuckDB(`a"b`))
}

func TestPostgresConnector_buildConnectionString(t *testing.T) {
	c, err := NewPostgresConnector(Config{
		Host:     "localhost",
		Port:     5432,
		User:     "admin",
		Password: "secret",
		Database: "mydb",
	})
	require.NoError(t, err)
	got := c.buildConnectionString()
	assert.Equal(t,
		"host=localhost port=5432 user=admin password=secret dbname=mydb sslmode=disable",
		got,
	)
}

func TestPostgresConnector_buildConnectionString_CustomSSLMode(t *testing.T) {
	c, err := NewPostgresConnector(Config{
		Host:     "db.example.com",
		Port:     5432,
		User:     "u",
		Database: "d",
		SSLMode:  "require",
	})
	require.NoError(t, err)
	assert.Contains(t, c.buildConnectionString(), "sslmode=require")
}

func TestDuckDBConnector_NotConnectedErrors(t *testing.T) {
	c, err := NewDuckDBConnector(Config{})
	require.NoError(t, err)

	_, err = c.ListTables()
	assert.Error(t, err)
	_, err = c.GetTableSchema("t")
	assert.Error(t, err)
	_, err = c.Query("SELECT 1")
	assert.Error(t, err)
	assert.Error(t, c.CreateTable("t", nil))
	assert.Error(t, c.InsertRow("t", []string{"a"}, []any{1}))
	// Close on a never-connected connector is a no-op.
	assert.NoError(t, c.Close())
}

func TestPostgresConnector_NotConnectedErrors(t *testing.T) {
	c, err := NewPostgresConnector(Config{})
	require.NoError(t, err)

	_, err = c.ListTables()
	assert.Error(t, err)
	_, err = c.GetTableSchema("t")
	assert.Error(t, err)
	_, err = c.Query("SELECT 1")
	assert.Error(t, err)
	assert.Error(t, c.CreateTable("t", nil))
	assert.Error(t, c.InsertRow("t", []string{"a"}, []any{1}))
	assert.NoError(t, c.Close())
}
