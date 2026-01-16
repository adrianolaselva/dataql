package xml

import (
	"database/sql"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"

	"github.com/adrianolaselva/dataql/pkg/exportdata"
	"github.com/schollz/progressbar/v3"
)

const (
	fileModeDefault os.FileMode = 0644
)

// Row represents a single row in XML
type Row struct {
	XMLName xml.Name
	Fields  []Field `xml:",any"`
}

// Field represents a field in a row
type Field struct {
	XMLName xml.Name
	Value   string `xml:",chardata"`
}

// Data represents the root XML element
type Data struct {
	XMLName xml.Name `xml:"data"`
	Rows    []Row    `xml:"row"`
}

type xmlExport struct {
	rows       *sql.Rows
	bar        *progressbar.ProgressBar
	file       *os.File
	exportPath string
	columns    []string
	data       Data
}

// NewXmlExport creates a new XML exporter
func NewXmlExport(rows *sql.Rows, exportPath string, bar *progressbar.ProgressBar) exportdata.Export {
	return &xmlExport{
		rows:       rows,
		exportPath: exportPath,
		bar:        bar,
		data:       Data{Rows: make([]Row, 0)},
	}
}

// Export exports rows to an XML file
func (x *xmlExport) Export() error {
	if err := x.loadColumns(); err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	// Read all rows into memory
	for x.rows.Next() {
		_ = x.bar.Add(1)
		if err := x.readRow(); err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}
	}

	// Write the XML to file
	if err := x.writeFile(); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// Close execute in defer
func (x *xmlExport) Close() error {
	if x.file != nil {
		return x.file.Close()
	}
	return nil
}

// readRow reads a row and appends it to the data slice
func (x *xmlExport) readRow() error {
	values := make([]interface{}, len(x.columns))
	pointers := make([]interface{}, len(x.columns))
	for i := range values {
		pointers[i] = &values[i]
	}

	if err := x.rows.Scan(pointers...); err != nil {
		return fmt.Errorf("failed to load row: %w", err)
	}

	row := Row{
		XMLName: xml.Name{Local: "row"},
		Fields:  make([]Field, len(x.columns)),
	}

	for i, col := range x.columns {
		value := ""
		if values[i] != nil {
			value = fmt.Sprintf("%v", values[i])
		}
		row.Fields[i] = Field{
			XMLName: xml.Name{Local: col},
			Value:   value,
		}
	}

	x.data.Rows = append(x.data.Rows, row)

	return nil
}

// writeFile writes the XML to the output file
func (x *xmlExport) writeFile() error {
	if _, err := os.Stat(x.exportPath); !os.IsNotExist(err) {
		err := os.Remove(x.exportPath)
		if err != nil {
			return fmt.Errorf("failed to remove file: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(x.exportPath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %w", err)
	}

	file, err := os.OpenFile(x.exportPath, os.O_CREATE|os.O_WRONLY, fileModeDefault)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", x.exportPath, err)
	}
	defer file.Close()

	// Write XML header
	if _, err := file.WriteString(xml.Header); err != nil {
		return fmt.Errorf("failed to write XML header: %w", err)
	}

	encoder := xml.NewEncoder(file)
	encoder.Indent("", "  ")

	if err := encoder.Encode(x.data); err != nil {
		return fmt.Errorf("failed to encode XML: %w", err)
	}

	return nil
}

// loadColumns load columns
func (x *xmlExport) loadColumns() error {
	columns, err := x.rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	x.columns = columns

	return nil
}
