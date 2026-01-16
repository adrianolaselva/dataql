package excel

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/adrianolaselva/dataql/pkg/exportdata"
	"github.com/schollz/progressbar/v3"
	"github.com/xuri/excelize/v2"
)

type excelExport struct {
	rows       *sql.Rows
	bar        *progressbar.ProgressBar
	exportPath string
	columns    []string
}

// NewExcelExport creates a new Excel exporter
func NewExcelExport(rows *sql.Rows, exportPath string, bar *progressbar.ProgressBar) exportdata.Export {
	return &excelExport{rows: rows, exportPath: exportPath, bar: bar}
}

// Export exports rows to an Excel file
func (e *excelExport) Export() error {
	if err := e.loadColumns(); err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	f := excelize.NewFile()
	defer f.Close()

	sheetName := "Sheet1"
	index, err := f.NewSheet(sheetName)
	if err != nil {
		return fmt.Errorf("failed to create sheet: %w", err)
	}
	f.SetActiveSheet(index)

	// Write header row
	for i, col := range e.columns {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		if err := f.SetCellValue(sheetName, cell, col); err != nil {
			return fmt.Errorf("failed to write header cell: %w", err)
		}
	}

	// Style header row
	headerStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"CCCCCC"}, Pattern: 1},
	})
	if err == nil {
		startCell, _ := excelize.CoordinatesToCellName(1, 1)
		endCell, _ := excelize.CoordinatesToCellName(len(e.columns), 1)
		_ = f.SetCellStyle(sheetName, startCell, endCell, headerStyle)
	}

	// Write data rows
	rowNum := 2
	for e.rows.Next() {
		_ = e.bar.Add(1)

		values := make([]interface{}, len(e.columns))
		pointers := make([]interface{}, len(e.columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := e.rows.Scan(pointers...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		for i, val := range values {
			cell, _ := excelize.CoordinatesToCellName(i+1, rowNum)
			if err := f.SetCellValue(sheetName, cell, val); err != nil {
				return fmt.Errorf("failed to write cell: %w", err)
			}
		}
		rowNum++
	}

	// Auto-fit columns (approximate)
	for i := range e.columns {
		col, _ := excelize.ColumnNumberToName(i + 1)
		_ = f.SetColWidth(sheetName, col, col, 15)
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(e.exportPath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %w", err)
	}

	// Remove existing file if exists
	if _, err := os.Stat(e.exportPath); !os.IsNotExist(err) {
		if err := os.Remove(e.exportPath); err != nil {
			return fmt.Errorf("failed to remove existing file: %w", err)
		}
	}

	// Save the file
	if err := f.SaveAs(e.exportPath); err != nil {
		return fmt.Errorf("failed to save Excel file: %w", err)
	}

	return nil
}

// Close execute in defer
func (e *excelExport) Close() error {
	return nil
}

// loadColumns load columns
func (e *excelExport) loadColumns() error {
	columns, err := e.rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}

	e.columns = columns

	return nil
}
