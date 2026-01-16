package json

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/adrianolaselva/dataql/pkg/exportdata"
	"github.com/schollz/progressbar/v3"
)

const (
	fileModeDefault os.FileMode = 0644
)

type jsonExport struct {
	rows       *sql.Rows
	bar        *progressbar.ProgressBar
	file       *os.File
	exportPath string
	columns    []string
	data       []map[string]interface{}
}

// NewJsonExport creates a new JSON exporter
func NewJsonExport(rows *sql.Rows, exportPath string, bar *progressbar.ProgressBar) exportdata.Export {
	return &jsonExport{rows: rows, exportPath: exportPath, bar: bar, data: make([]map[string]interface{}, 0)}
}

// Export exports rows to a JSON array file
func (j *jsonExport) Export() error {
	if err := j.loadColumns(); err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	// Read all rows into memory
	for j.rows.Next() {
		_ = j.bar.Add(1)
		if err := j.readRow(); err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}
	}

	// Write the JSON array to file
	if err := j.writeFile(); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// Close execute in defer
func (j *jsonExport) Close() error {
	if j.file != nil {
		return j.file.Close()
	}
	return nil
}

// readRow reads a row and appends it to the data slice
func (j *jsonExport) readRow() error {
	values := make([]interface{}, len(j.columns))
	pointers := make([]interface{}, len(j.columns))
	for i := range values {
		pointers[i] = &values[i]
	}

	if err := j.rows.Scan(pointers...); err != nil {
		return fmt.Errorf("failed to load row: %w", err)
	}

	row := make(map[string]interface{})
	for i, c := range j.columns {
		row[c] = values[i]
	}

	j.data = append(j.data, row)

	return nil
}

// writeFile writes the JSON array to the output file
func (j *jsonExport) writeFile() error {
	if _, err := os.Stat(j.exportPath); !os.IsNotExist(err) {
		err := os.Remove(j.exportPath)
		if err != nil {
			return fmt.Errorf("failed to remove file: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(j.exportPath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %w", err)
	}

	file, err := os.OpenFile(j.exportPath, os.O_CREATE|os.O_WRONLY, fileModeDefault)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", j.exportPath, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(j.data); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	return nil
}

// loadColumns load columns
func (j *jsonExport) loadColumns() error {
	columns, err := j.rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	j.columns = columns

	return nil
}
