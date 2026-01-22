package filehandler_test

import (
	"testing"

	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/stretchr/testify/assert"
)

func TestDetectFormat(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		expected filehandler.Format
		wantErr  bool
	}{
		{
			name:     "CSV file",
			filePath: "/path/to/file.csv",
			expected: filehandler.FormatCSV,
			wantErr:  false,
		},
		{
			name:     "JSON file",
			filePath: "/path/to/file.json",
			expected: filehandler.FormatJSON,
			wantErr:  false,
		},
		{
			name:     "JSONL file",
			filePath: "/path/to/file.jsonl",
			expected: filehandler.FormatJSONL,
			wantErr:  false,
		},
		{
			name:     "NDJSON file",
			filePath: "/path/to/file.ndjson",
			expected: filehandler.FormatJSONL,
			wantErr:  false,
		},
		{
			name:     "uppercase extension",
			filePath: "/path/to/file.CSV",
			expected: filehandler.FormatCSV,
			wantErr:  false,
		},
		{
			name:     "unsupported format",
			filePath: "/path/to/file.xyz",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "no extension",
			filePath: "/path/to/file",
			expected: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			format, err := filehandler.DetectFormat(tt.filePath)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, format)
			}
		})
	}
}

func TestDetectFormatFromFiles(t *testing.T) {
	tests := []struct {
		name     string
		files    []string
		expected filehandler.Format
		wantErr  bool
	}{
		{
			name:     "single CSV file",
			files:    []string{"/path/to/file.csv"},
			expected: filehandler.FormatCSV,
			wantErr:  false,
		},
		{
			name:     "multiple CSV files",
			files:    []string{"/path/to/file1.csv", "/path/to/file2.csv"},
			expected: filehandler.FormatCSV,
			wantErr:  false,
		},
		{
			name:     "mixed formats",
			files:    []string{"/path/to/file.csv", "/path/to/file.json"},
			expected: filehandler.FormatMixed,
			wantErr:  false,
		},
		{
			name:     "empty list",
			files:    []string{},
			expected: "",
			wantErr:  true,
		},
		{
			name:     "unsupported format in list",
			files:    []string{"/path/to/file.xyz"},
			expected: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			format, err := filehandler.DetectFormatFromFiles(tt.files)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, format)
			}
		})
	}
}

func TestSupportedFormats(t *testing.T) {
	formats := filehandler.SupportedFormats()
	assert.Contains(t, formats, filehandler.FormatCSV)
	assert.Contains(t, formats, filehandler.FormatJSON)
	assert.Contains(t, formats, filehandler.FormatJSONL)
}

func TestIsFormatSupported(t *testing.T) {
	tests := []struct {
		format   string
		expected bool
	}{
		{"csv", true},
		{"CSV", true},
		{"json", true},
		{"jsonl", true},
		{"xml", true},
		{"parquet", true},
		{"yaml", true},
		{"avro", true},
		{"orc", true},
		{"excel", true},
		{"xyz", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			result := filehandler.IsFormatSupported(tt.format)
			assert.Equal(t, tt.expected, result)
		})
	}
}
