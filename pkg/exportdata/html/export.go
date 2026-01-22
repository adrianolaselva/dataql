package html

import (
	"database/sql"
	"fmt"
	"html"
	"os"
	"path/filepath"

	"github.com/adrianolaselva/dataql/pkg/exportdata"
	"github.com/schollz/progressbar/v3"
)

const (
	fileModeDefault os.FileMode = 0644
)

type htmlExport struct {
	rows       *sql.Rows
	bar        *progressbar.ProgressBar
	file       *os.File
	exportPath string
	columns    []string
}

// NewHTMLExport creates a new HTML table exporter
func NewHTMLExport(rows *sql.Rows, exportPath string, bar *progressbar.ProgressBar) exportdata.Export {
	return &htmlExport{rows: rows, exportPath: exportPath, bar: bar}
}

// Export exports rows to an HTML table format
func (h *htmlExport) Export() error {
	if err := h.loadColumns(); err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	if err := h.openFile(); err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	// Write HTML document start and table header
	if err := h.writeDocumentStart(); err != nil {
		return fmt.Errorf("failed to write document start: %w", err)
	}

	// Write table header row
	if err := h.writeTableHeader(); err != nil {
		return fmt.Errorf("failed to write table header: %w", err)
	}

	// Write tbody start
	if _, err := h.file.WriteString("  <tbody>\n"); err != nil {
		return fmt.Errorf("failed to write tbody start: %w", err)
	}

	// Write data rows
	for h.rows.Next() {
		_ = h.bar.Add(1)
		if err := h.writeDataRow(); err != nil {
			return fmt.Errorf("failed to write data row: %w", err)
		}
	}

	// Write document end
	if err := h.writeDocumentEnd(); err != nil {
		return fmt.Errorf("failed to write document end: %w", err)
	}

	return nil
}

// writeDocumentStart writes the HTML document header and table start
func (h *htmlExport) writeDocumentStart() error {
	docStart := `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>DataQL Export</title>
  <style>
    table { border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; }
    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    th { background-color: #4CAF50; color: white; }
    tr:nth-child(even) { background-color: #f2f2f2; }
    tr:hover { background-color: #ddd; }
  </style>
</head>
<body>
<table>
`
	if _, err := h.file.WriteString(docStart); err != nil {
		return fmt.Errorf("failed to write document start: %w", err)
	}
	return nil
}

// writeTableHeader writes the HTML table header row
func (h *htmlExport) writeTableHeader() error {
	if _, err := h.file.WriteString("  <thead>\n    <tr>\n"); err != nil {
		return err
	}

	for _, col := range h.columns {
		escapedCol := html.EscapeString(col)
		if _, err := h.file.WriteString(fmt.Sprintf("      <th>%s</th>\n", escapedCol)); err != nil {
			return err
		}
	}

	if _, err := h.file.WriteString("    </tr>\n  </thead>\n"); err != nil {
		return err
	}

	return nil
}

// writeDataRow writes a single data row to the HTML table
func (h *htmlExport) writeDataRow() error {
	values := make([]interface{}, len(h.columns))
	pointers := make([]interface{}, len(h.columns))
	for i := range values {
		pointers[i] = &values[i]
	}

	if err := h.rows.Scan(pointers...); err != nil {
		return fmt.Errorf("failed to scan row: %w", err)
	}

	if _, err := h.file.WriteString("    <tr>\n"); err != nil {
		return err
	}

	for _, v := range values {
		var cellValue string
		if v == nil {
			cellValue = ""
		} else {
			cellValue = html.EscapeString(fmt.Sprintf("%v", v))
		}
		if _, err := h.file.WriteString(fmt.Sprintf("      <td>%s</td>\n", cellValue)); err != nil {
			return err
		}
	}

	if _, err := h.file.WriteString("    </tr>\n"); err != nil {
		return err
	}

	return nil
}

// writeDocumentEnd writes the HTML document footer
func (h *htmlExport) writeDocumentEnd() error {
	docEnd := `  </tbody>
</table>
</body>
</html>
`
	if _, err := h.file.WriteString(docEnd); err != nil {
		return fmt.Errorf("failed to write document end: %w", err)
	}
	return nil
}

// Close closes the file
func (h *htmlExport) Close() error {
	if h.file != nil {
		return h.file.Close()
	}
	return nil
}

// openFile opens or creates the output file
func (h *htmlExport) openFile() error {
	if _, err := os.Stat(h.exportPath); !os.IsNotExist(err) {
		if err := os.Remove(h.exportPath); err != nil {
			return fmt.Errorf("failed to remove existing file: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(h.exportPath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.OpenFile(h.exportPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, fileModeDefault)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", h.exportPath, err)
	}

	h.file = file
	return nil
}

// loadColumns loads column names from the result set
func (h *htmlExport) loadColumns() error {
	columns, err := h.rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}
	h.columns = columns
	return nil
}
