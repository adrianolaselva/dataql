package markdown

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/adrianolaselva/dataql/pkg/exportdata"
	"github.com/schollz/progressbar/v3"
)

const (
	fileModeDefault os.FileMode = 0644
)

type markdownExport struct {
	rows       *sql.Rows
	bar        *progressbar.ProgressBar
	file       *os.File
	exportPath string
	columns    []string
}

// NewMarkdownExport creates a new Markdown table exporter
func NewMarkdownExport(rows *sql.Rows, exportPath string, bar *progressbar.ProgressBar) exportdata.Export {
	return &markdownExport{rows: rows, exportPath: exportPath, bar: bar}
}

// Export exports rows to a Markdown table format
func (m *markdownExport) Export() error {
	if err := m.loadColumns(); err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	if err := m.openFile(); err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	// Write header row
	if err := m.writeHeader(); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write separator row
	if err := m.writeSeparator(); err != nil {
		return fmt.Errorf("failed to write separator: %w", err)
	}

	// Write data rows
	for m.rows.Next() {
		_ = m.bar.Add(1)
		if err := m.writeDataRow(); err != nil {
			return fmt.Errorf("failed to write data row: %w", err)
		}
	}

	return nil
}

// writeHeader writes the Markdown table header row
func (m *markdownExport) writeHeader() error {
	header := "| " + strings.Join(m.columns, " | ") + " |\n"
	if _, err := m.file.WriteString(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	return nil
}

// writeSeparator writes the Markdown table separator row
func (m *markdownExport) writeSeparator() error {
	separators := make([]string, len(m.columns))
	for i := range separators {
		separators[i] = "---"
	}
	separator := "| " + strings.Join(separators, " | ") + " |\n"
	if _, err := m.file.WriteString(separator); err != nil {
		return fmt.Errorf("failed to write separator: %w", err)
	}
	return nil
}

// writeDataRow writes a single data row to the Markdown table
func (m *markdownExport) writeDataRow() error {
	values := make([]interface{}, len(m.columns))
	pointers := make([]interface{}, len(m.columns))
	for i := range values {
		pointers[i] = &values[i]
	}

	if err := m.rows.Scan(pointers...); err != nil {
		return fmt.Errorf("failed to scan row: %w", err)
	}

	// Convert to strings and escape pipe characters
	stringValues := make([]string, len(values))
	for i, v := range values {
		if v == nil {
			stringValues[i] = ""
		} else {
			str := fmt.Sprintf("%v", v)
			// Escape pipe characters in values
			str = strings.ReplaceAll(str, "|", "\\|")
			stringValues[i] = str
		}
	}

	row := "| " + strings.Join(stringValues, " | ") + " |\n"
	if _, err := m.file.WriteString(row); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}

	return nil
}

// Close closes the file
func (m *markdownExport) Close() error {
	if m.file != nil {
		return m.file.Close()
	}
	return nil
}

// openFile opens or creates the output file
func (m *markdownExport) openFile() error {
	if _, err := os.Stat(m.exportPath); !os.IsNotExist(err) {
		if err := os.Remove(m.exportPath); err != nil {
			return fmt.Errorf("failed to remove existing file: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(m.exportPath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.OpenFile(m.exportPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, fileModeDefault)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", m.exportPath, err)
	}

	m.file = file
	return nil
}

// loadColumns loads column names from the result set
func (m *markdownExport) loadColumns() error {
	columns, err := m.rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}
	m.columns = columns
	return nil
}
