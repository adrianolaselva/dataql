package excel

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/schollz/progressbar/v3"
	"github.com/xuri/excelize/v2"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_ ]+`)

type excelHandler struct {
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	fileInputs  []string
	totalLines  int
	limitLines  int
	currentLine int
	collection  string
	aliases     map[string]string // Map of file path -> table alias
}

// NewExcelHandler creates a new Excel file handler
func NewExcelHandler(fileInputs []string, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &excelHandler{
		fileInputs: fileInputs,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
	}
}

// NewExcelHandlerWithAliases creates a new Excel file handler with table aliases
func NewExcelHandlerWithAliases(fileInputs []string, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string, aliases map[string]string) filehandler.FileHandler {
	return &excelHandler{
		fileInputs: fileInputs,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
		aliases:    aliases,
	}
}

// Import imports data from Excel files
func (e *excelHandler) Import() error {
	for _, filePath := range e.fileInputs {
		if err := e.loadFile(filePath); err != nil {
			return fmt.Errorf("failed to load file %s: %w", filePath, err)
		}
	}
	return nil
}

// loadFile loads a single Excel file
func (e *excelHandler) loadFile(filePath string) error {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to open Excel file %s: %w", filePath, err)
	}
	defer f.Close()

	// Get the first sheet name (or use collection if specified)
	sheetList := f.GetSheetList()
	if len(sheetList) == 0 {
		return fmt.Errorf("no sheets found in Excel file %s", filePath)
	}

	// Process the first sheet (or all sheets based on configuration)
	sheetName := sheetList[0]
	tableName := e.formatTableName(filePath)

	// Get all rows from the sheet
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return fmt.Errorf("failed to get rows from sheet %s: %w", sheetName, err)
	}

	if len(rows) == 0 {
		// Empty sheet - create placeholder table
		if err := e.storage.BuildStructure(tableName, []string{"_empty"}); err != nil {
			return fmt.Errorf("failed to build structure for empty Excel: %w", err)
		}
		return nil
	}

	// First row is the header
	columns := make([]string, len(rows[0]))
	for i, col := range rows[0] {
		columns[i] = e.sanitizeColumnName(col)
		if columns[i] == "" {
			columns[i] = fmt.Sprintf("column_%d", i+1)
		}
	}

	// Build table structure
	if err := e.storage.BuildStructure(tableName, columns); err != nil {
		return fmt.Errorf("failed to build structure: %w", err)
	}

	// Calculate total lines (excluding header)
	dataRows := rows[1:]
	e.totalLines = len(dataRows)
	if e.limitLines > 0 && e.totalLines > e.limitLines {
		e.totalLines = e.limitLines
	}

	e.bar.ChangeMax(e.totalLines)

	// Insert data rows
	for i, row := range dataRows {
		if e.limitLines > 0 && i >= e.limitLines {
			break
		}

		// Pad row with empty values if needed
		values := make([]any, len(columns))
		for j := 0; j < len(columns); j++ {
			if j < len(row) {
				values[j] = row[j]
			} else {
				values[j] = ""
			}
		}

		if err := e.storage.InsertRow(tableName, columns, values); err != nil {
			return fmt.Errorf("failed to insert row %d: %w", i+1, err)
		}

		_ = e.bar.Add(1)
		e.currentLine++
	}

	return nil
}

// sanitizeColumnName sanitizes a string to be used as a SQL column name
func (e *excelHandler) sanitizeColumnName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// formatTableName formats table name from file path
func (e *excelHandler) formatTableName(filePath string) string {
	// Check if there's an alias for this file
	if e.aliases != nil {
		if alias, ok := e.aliases[filePath]; ok && alias != "" {
			tableName := strings.ReplaceAll(strings.ToLower(alias), " ", "_")
			return nonAlphanumericRegex.ReplaceAllString(tableName, "")
		}
	}

	// Use collection if provided
	if e.collection != "" {
		tableName := strings.ReplaceAll(strings.ToLower(e.collection), " ", "_")
		return nonAlphanumericRegex.ReplaceAllString(tableName, "")
	}

	// Default: use filename
	tableName := strings.ReplaceAll(strings.ToLower(filepath.Base(filePath)), filepath.Ext(filePath), "")
	tableName = strings.ReplaceAll(tableName, " ", "_")
	return nonAlphanumericRegex.ReplaceAllString(tableName, "")
}

// Lines returns total lines count
func (e *excelHandler) Lines() int {
	return e.totalLines
}

// Close cleans up resources
func (e *excelHandler) Close() error {
	return nil
}
