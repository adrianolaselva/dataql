package yaml

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/adrianolaselva/dataql/pkg/exportdata"
	"github.com/schollz/progressbar/v3"
	"gopkg.in/yaml.v3"
)

const (
	fileModeDefault os.FileMode = 0644
)

type yamlExport struct {
	rows       *sql.Rows
	bar        *progressbar.ProgressBar
	file       *os.File
	exportPath string
	columns    []string
	data       []map[string]interface{}
}

// NewYamlExport creates a new YAML exporter
func NewYamlExport(rows *sql.Rows, exportPath string, bar *progressbar.ProgressBar) exportdata.Export {
	return &yamlExport{
		rows:       rows,
		exportPath: exportPath,
		bar:        bar,
		data:       make([]map[string]interface{}, 0),
	}
}

// Export exports rows to a YAML file
func (y *yamlExport) Export() error {
	if err := y.loadColumns(); err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	// Read all rows into memory
	for y.rows.Next() {
		_ = y.bar.Add(1)
		if err := y.readRow(); err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}
	}

	// Write the YAML to file
	if err := y.writeFile(); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// Close execute in defer
func (y *yamlExport) Close() error {
	if y.file != nil {
		return y.file.Close()
	}
	return nil
}

// readRow reads a row and appends it to the data slice
func (y *yamlExport) readRow() error {
	values := make([]interface{}, len(y.columns))
	pointers := make([]interface{}, len(y.columns))
	for i := range values {
		pointers[i] = &values[i]
	}

	if err := y.rows.Scan(pointers...); err != nil {
		return fmt.Errorf("failed to load row: %w", err)
	}

	row := make(map[string]interface{})
	for i, c := range y.columns {
		row[c] = values[i]
	}

	y.data = append(y.data, row)

	return nil
}

// writeFile writes the YAML to the output file
func (y *yamlExport) writeFile() error {
	if _, err := os.Stat(y.exportPath); !os.IsNotExist(err) {
		err := os.Remove(y.exportPath)
		if err != nil {
			return fmt.Errorf("failed to remove file: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(y.exportPath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %w", err)
	}

	file, err := os.OpenFile(y.exportPath, os.O_CREATE|os.O_WRONLY, fileModeDefault)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", y.exportPath, err)
	}
	defer file.Close()

	encoder := yaml.NewEncoder(file)
	encoder.SetIndent(2)
	defer encoder.Close()

	if err := encoder.Encode(y.data); err != nil {
		return fmt.Errorf("failed to encode YAML: %w", err)
	}

	return nil
}

// loadColumns load columns
func (y *yamlExport) loadColumns() error {
	columns, err := y.rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	y.columns = columns

	return nil
}
