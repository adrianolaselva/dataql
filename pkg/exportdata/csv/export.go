package csv

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/adrianolaselva/dataql/pkg/exportdata"
	"github.com/schollz/progressbar/v3"
	"os"
	"path/filepath"
)

const (
	fileModeDefault os.FileMode = 0644
)

type csvExport struct {
	rows       *sql.Rows
	bar        *progressbar.ProgressBar
	file       *os.File
	exportPath string
	columns    []string
}

func NewCsvExport(rows *sql.Rows, exportPath string, bar *progressbar.ProgressBar) exportdata.Export {
	return &csvExport{rows: rows, exportPath: exportPath, bar: bar}
}

// Export rows in file
func (c *csvExport) Export() error {
	if err := c.loadColumns(); err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	if err := c.openFile(); err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	w := csv.NewWriter(c.file)
	defer w.Flush()

	if err := w.Write(c.columns); err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	for c.rows.Next() {
		_ = c.bar.Add(1)
		if err := c.readAndAppendFile(w); err != nil {
			return fmt.Errorf("failed to read and append line in file: %w", err)
		}
	}

	return nil
}

// readAndAppendFile read line and append in file
func (c *csvExport) readAndAppendFile(w *csv.Writer) error {
	values := make([]interface{}, len(c.columns))
	pointers := make([]interface{}, len(c.columns))
	for i := range values {
		pointers[i] = &values[i]
	}

	if err := c.rows.Scan(pointers...); err != nil {
		return fmt.Errorf("failed to load row: %w", err)
	}

	if err := w.Write(c.convertToStringArray(values)); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}

	return nil
}

// convertToStringArray convert string array to string array
func (c *csvExport) convertToStringArray(records []interface{}) []string {
	values := make([]string, 0, len(records))
	for _, r := range records {
		values = append(values, r.(string))
	}

	return values
}

// Close execute in defer
func (c *csvExport) Close() error {
	defer func(file *os.File) {
		_ = file.Close()
	}(c.file)

	return nil
}

// openFile open file
func (c *csvExport) openFile() error {
	if _, err := os.Stat(c.exportPath); !os.IsNotExist(err) {
		err := os.Remove(c.exportPath)
		if err != nil {
			return fmt.Errorf("failed to remove file: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(c.exportPath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %w", err)
	}

	file, err := os.OpenFile(c.exportPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, fileModeDefault)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", c.exportPath, err)
	}

	c.file = file

	return nil
}

// loadColumns load columns
func (c *csvExport) loadColumns() error {
	columns, err := c.rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	c.columns = columns

	return nil
}
