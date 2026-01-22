package dataql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseFileInput(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedPath  string
		expectedAlias string
	}{
		{
			name:          "simple path without alias",
			input:         "data.csv",
			expectedPath:  "data.csv",
			expectedAlias: "",
		},
		{
			name:          "simple path with alias",
			input:         "data.csv:users",
			expectedPath:  "data.csv",
			expectedAlias: "users",
		},
		{
			name:          "absolute path with alias",
			input:         "/path/to/file.csv:my_table",
			expectedPath:  "/path/to/file.csv",
			expectedAlias: "my_table",
		},
		{
			name:          "relative path with alias",
			input:         "./data/file.json:records",
			expectedPath:  "./data/file.json",
			expectedAlias: "records",
		},
		{
			name:          "path with multiple dots and alias",
			input:         "file.backup.csv:backup_data",
			expectedPath:  "file.backup.csv",
			expectedAlias: "backup_data",
		},
		{
			name:          "Windows path without alias",
			input:         "C:\\Users\\data\\file.csv",
			expectedPath:  "C:\\Users\\data\\file.csv",
			expectedAlias: "",
		},
		{
			name:          "URL-like path without alias",
			input:         "https://example.com/data.csv",
			expectedPath:  "https://example.com/data.csv",
			expectedAlias: "",
		},
		{
			name:          "S3 path without alias",
			input:         "s3://bucket/path/file.csv",
			expectedPath:  "s3://bucket/path/file.csv",
			expectedAlias: "",
		},
		{
			name:          "S3 path with alias",
			input:         "s3://bucket/path/file.csv:s3_data",
			expectedPath:  "s3://bucket/path/file.csv",
			expectedAlias: "s3_data",
		},
		{
			name:          "empty input",
			input:         "",
			expectedPath:  "",
			expectedAlias: "",
		},
		{
			name:          "alias with underscore",
			input:         "data.csv:my_custom_table",
			expectedPath:  "data.csv",
			expectedAlias: "my_custom_table",
		},
		{
			name:          "alias with numbers",
			input:         "data.csv:table123",
			expectedPath:  "data.csv",
			expectedAlias: "table123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseFileInput(tt.input)
			assert.Equal(t, tt.expectedPath, result.Path)
			assert.Equal(t, tt.expectedAlias, result.Alias)
		})
	}
}

func TestParseFileInputs(t *testing.T) {
	inputs := []string{
		"file1.csv",
		"file2.csv:users",
		"/path/to/file3.json:orders",
	}

	result := ParseFileInputs(inputs)

	assert.Len(t, result, 3)
	assert.Equal(t, "file1.csv", result[0].Path)
	assert.Equal(t, "", result[0].Alias)
	assert.Equal(t, "file2.csv", result[1].Path)
	assert.Equal(t, "users", result[1].Alias)
	assert.Equal(t, "/path/to/file3.json", result[2].Path)
	assert.Equal(t, "orders", result[2].Alias)
}

func TestGetPaths(t *testing.T) {
	inputs := []FileInput{
		{Path: "file1.csv", Alias: ""},
		{Path: "file2.csv", Alias: "users"},
		{Path: "/path/to/file3.json", Alias: "orders"},
	}

	paths := GetPaths(inputs)

	assert.Len(t, paths, 3)
	assert.Equal(t, []string{"file1.csv", "file2.csv", "/path/to/file3.json"}, paths)
}

func TestGetAliasMap(t *testing.T) {
	inputs := []FileInput{
		{Path: "file1.csv", Alias: ""},
		{Path: "file2.csv", Alias: "users"},
		{Path: "/path/to/file3.json", Alias: "orders"},
	}

	aliases := GetAliasMap(inputs)

	assert.Len(t, aliases, 2) // Only non-empty aliases
	assert.Equal(t, "users", aliases["file2.csv"])
	assert.Equal(t, "orders", aliases["/path/to/file3.json"])
	_, exists := aliases["file1.csv"]
	assert.False(t, exists)
}
