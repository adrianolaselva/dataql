package jsonl

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/adrianolaselva/dataql/pkg/exportdata"
	"github.com/schollz/progressbar/v3"
	"os"
	"path/filepath"
)

const (
	fileModeDefault os.FileMode = 0644
)

type jsonlExport struct {
	rows       *sql.Rows
	bar        *progressbar.ProgressBar
	file       *os.File
	exportPath string
	columns    []string
}

func NewJsonlExport(rows *sql.Rows, exportPath string, bar *progressbar.ProgressBar) exportdata.Export {
	return &jsonlExport{rows: rows, exportPath: exportPath, bar: bar}
}

// Export rows in file
func (j *jsonlExport) Export() error {
	if err := j.loadColumns(); err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	if err := j.openFile(); err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	for j.rows.Next() {
		_ = j.bar.Add(1)
		if err := j.readAndAppendFile(); err != nil {
			return fmt.Errorf("failed to read and append line in file: %w", err)
		}
	}

	return nil
}

// Close execute in defer
func (j *jsonlExport) Close() error {
	defer func(file *os.File) {
		_ = file.Close()
	}(j.file)

	return nil
}

// readAndAppendFile read line and append in file
func (j *jsonlExport) readAndAppendFile() error {
	values := make([]interface{}, len(j.columns))
	pointers := make([]interface{}, len(j.columns))
	for i := range values {
		pointers[i] = &values[i]
	}

	if err := j.rows.Scan(pointers...); err != nil {
		return fmt.Errorf("failed to load row: %w", err)
	}

	attr := map[string]interface{}{}
	for i, c := range j.columns {
		attr[c] = values[i]
	}

	payload, err := json.Marshal(attr)
	if err != nil {
		return fmt.Errorf("failed to serialize row: %w", err)
	}

	buffer := new(bytes.Buffer)
	if err := json.Compact(buffer, payload); err != nil {
		return fmt.Errorf("failed to compact payload: %w", err)
	}

	if _, err := j.file.WriteString(fmt.Sprintf("%s\n", buffer.String())); err != nil {
		return fmt.Errorf("failed to write file %s: %w", j.exportPath, err)
	}

	return nil
}

// openFile open file
func (j *jsonlExport) openFile() error {
	if _, err := os.Stat(j.exportPath); !os.IsNotExist(err) {
		err := os.Remove(j.exportPath)
		if err != nil {
			return fmt.Errorf("failed to remove file: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(j.exportPath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %w", err)
	}

	file, err := os.OpenFile(j.exportPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, fileModeDefault)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", j.exportPath, err)
	}

	j.file = file

	return nil
}

// loadColumns load columns
func (j *jsonlExport) loadColumns() error {
	columns, err := j.rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	j.columns = columns

	return nil
}
