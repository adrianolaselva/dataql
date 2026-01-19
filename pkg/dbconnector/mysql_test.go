package dbconnector

import (
	"strings"
	"testing"
)

func TestMySQLConnector_buildConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		contains []string
	}{
		{
			name: "should include charset utf8mb4",
			config: Config{
				Type:     DBTypeMySQL,
				Host:     "localhost",
				Port:     3306,
				User:     "testuser",
				Password: "testpass",
				Database: "testdb",
			},
			contains: []string{
				"charset=utf8mb4",
				"collation=utf8mb4_unicode_ci",
				"parseTime=true",
			},
		},
		{
			name: "should format DSN correctly",
			config: Config{
				Type:     DBTypeMySQL,
				Host:     "mysql.example.com",
				Port:     3307,
				User:     "admin",
				Password: "secret",
				Database: "production",
			},
			contains: []string{
				"admin:secret@tcp(mysql.example.com:3307)/production",
				"charset=utf8mb4",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector := &MySQLConnector{
				config: tt.config,
			}

			dsn := connector.buildConnectionString()

			for _, expected := range tt.contains {
				if !strings.Contains(dsn, expected) {
					t.Errorf("DSN should contain %q, got: %s", expected, dsn)
				}
			}
		})
	}
}

func TestMySQLConnector_buildConnectionString_CharsetPreventsEncodingIssue(t *testing.T) {
	// This test documents the encoding issue fix
	// Without charset=utf8mb4, MySQL driver returns VARCHAR columns as []byte
	// which fmt.Sprintf("%v", val) converts to "[65 108 105 99 101]" instead of "Alice"
	connector := &MySQLConnector{
		config: Config{
			Type:     DBTypeMySQL,
			Host:     "localhost",
			Port:     3306,
			User:     "user",
			Password: "pass",
			Database: "db",
		},
	}

	dsn := connector.buildConnectionString()

	// Verify charset is present to prevent the encoding issue
	if !strings.Contains(dsn, "charset=utf8mb4") {
		t.Error("DSN must contain charset=utf8mb4 to prevent VARCHAR being returned as []byte")
	}

	// Verify parseTime is present for proper datetime handling
	if !strings.Contains(dsn, "parseTime=true") {
		t.Error("DSN must contain parseTime=true for proper datetime handling")
	}
}

func TestNewMySQLConnector(t *testing.T) {
	config := Config{
		Type:     DBTypeMySQL,
		Host:     "localhost",
		Port:     3306,
		User:     "user",
		Password: "pass",
		Database: "testdb",
	}

	connector, err := NewMySQLConnector(config)

	if err != nil {
		t.Errorf("NewMySQLConnector should not return error, got: %v", err)
	}

	if connector == nil {
		t.Error("NewMySQLConnector should return a connector")
	}

	if connector.config.Host != "localhost" {
		t.Errorf("Expected host localhost, got: %s", connector.config.Host)
	}

	if connector.config.Port != 3306 {
		t.Errorf("Expected port 3306, got: %d", connector.config.Port)
	}
}

func TestQuoteIdentifierMySQL(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"users", "`users`"},
		{"my_table", "`my_table`"},
		{"table`name", "`table``name`"},
		{"", "``"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := quoteIdentifierMySQL(tt.input)
			if result != tt.expected {
				t.Errorf("quoteIdentifierMySQL(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestMapToMySQLType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"text", "TEXT"},
		{"string", "TEXT"},
		{"varchar", "TEXT"},
		{"integer", "INT"},
		{"int", "INT"},
		{"bigint", "BIGINT"},
		{"float", "DOUBLE"},
		{"double", "DOUBLE"},
		{"boolean", "TINYINT(1)"},
		{"bool", "TINYINT(1)"},
		{"timestamp", "DATETIME"},
		{"datetime", "DATETIME"},
		{"date", "DATE"},
		{"unknown", "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := mapToMySQLType(tt.input)
			if result != tt.expected {
				t.Errorf("mapToMySQLType(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}
