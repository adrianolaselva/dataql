package parquet

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/adrianolaselva/dataql/pkg/exportdata"
	"github.com/schollz/progressbar/v3"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type parquetExport struct {
	rows       *sql.Rows
	bar        *progressbar.ProgressBar
	exportPath string
	columns    []string
}

// NewParquetExport creates a new Parquet exporter
func NewParquetExport(rows *sql.Rows, exportPath string, bar *progressbar.ProgressBar) exportdata.Export {
	return &parquetExport{rows: rows, exportPath: exportPath, bar: bar}
}

// Export exports rows to a Parquet file
func (p *parquetExport) Export() error {
	if err := p.loadColumns(); err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(p.exportPath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %w", err)
	}

	// Remove existing file if exists
	if _, err := os.Stat(p.exportPath); !os.IsNotExist(err) {
		if err := os.Remove(p.exportPath); err != nil {
			return fmt.Errorf("failed to remove existing file: %w", err)
		}
	}

	// Create local file writer
	fw, err := local.NewLocalFileWriter(p.exportPath)
	if err != nil {
		return fmt.Errorf("failed to create file writer: %w", err)
	}
	defer fw.Close()

	// Create schema metadata for CSV writer
	// Each column needs to be in format: "name=colname, type=BYTE_ARRAY, convertedtype=UTF8"
	schemaCols := make([]string, len(p.columns))
	for i, col := range p.columns {
		schemaCols[i] = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8", col)
	}

	// Create CSV writer for Parquet - handles dynamic schemas better
	pw, err := writer.NewCSVWriter(schemaCols, fw, 4)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	// Write rows
	for p.rows.Next() {
		_ = p.bar.Add(1)

		values := make([]interface{}, len(p.columns))
		pointers := make([]interface{}, len(p.columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := p.rows.Scan(pointers...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert to string slice for CSV writer
		row := make([]*string, len(p.columns))
		for i := range p.columns {
			if values[i] == nil {
				empty := ""
				row[i] = &empty
			} else {
				s := fmt.Sprintf("%v", values[i])
				row[i] = &s
			}
		}

		if err := pw.WriteString(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to finalize Parquet file: %w", err)
	}

	return nil
}

// Close execute in defer
func (p *parquetExport) Close() error {
	return nil
}

// loadColumns load columns
func (p *parquetExport) loadColumns() error {
	columns, err := p.rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	// Sanitize column names for Parquet schema
	sanitized := make([]string, len(columns))
	for i, col := range columns {
		sanitized[i] = p.sanitizeColumnName(col)
	}

	p.columns = sanitized

	return nil
}

// sanitizeColumnName makes a column name safe for Parquet schema
func (p *parquetExport) sanitizeColumnName(name string) string {
	// Replace spaces and special characters with underscores
	result := ""
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			result += string(c)
		} else {
			result += "_"
		}
	}
	// Ensure it doesn't start with a number
	if len(result) > 0 && result[0] >= '0' && result[0] <= '9' {
		result = "_" + result
	}
	if result == "" {
		result = "column"
	}
	return result
}
