package parquet

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/schollz/progressbar/v3"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_ ]+`)

type parquetHandler struct {
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	fileInputs  []string
	totalLines  int
	limitLines  int
	currentLine int
	collection  string
}

// NewParquetHandler creates a new Parquet file handler
func NewParquetHandler(fileInputs []string, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &parquetHandler{
		fileInputs: fileInputs,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
	}
}

// Import imports data from Parquet files
func (p *parquetHandler) Import() error {
	for _, filePath := range p.fileInputs {
		if err := p.loadFile(filePath); err != nil {
			return fmt.Errorf("failed to load file %s: %w", filePath, err)
		}
	}
	return nil
}

// loadFile loads a single Parquet file
func (p *parquetHandler) loadFile(filePath string) error {
	// Open the file
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return fmt.Errorf("failed to open Parquet file %s: %w", filePath, err)
	}
	defer fr.Close()

	// Create parquet column reader (no schema needed)
	pr, err := reader.NewParquetColumnReader(fr, 4)
	if err != nil {
		return fmt.Errorf("failed to create Parquet reader: %w", err)
	}
	defer pr.ReadStop()

	tableName := p.formatTableName(filePath)
	numRows := int(pr.GetNumRows())

	if numRows == 0 {
		// Empty file - create placeholder table
		if err := p.storage.BuildStructure(tableName, []string{"_empty"}); err != nil {
			return fmt.Errorf("failed to build structure for empty Parquet: %w", err)
		}
		return nil
	}

	// Get schema columns - using the schema handler to extract column names
	schemaHandler := pr.SchemaHandler
	columns := make([]string, 0)
	columnPaths := make([]string, 0)

	// Extract leaf columns (actual data columns)
	for i := 0; i < len(schemaHandler.SchemaElements); i++ {
		elem := schemaHandler.SchemaElements[i]
		if elem.GetNumChildren() == 0 { // Leaf node (actual column)
			colName := p.sanitizeColumnName(elem.GetName())
			columns = append(columns, colName)
			// Get the path for this column
			path := schemaHandler.IndexMap[int32(i)]
			columnPaths = append(columnPaths, path)
		}
	}

	if len(columns) == 0 {
		columns = []string{"_empty"}
		if err := p.storage.BuildStructure(tableName, columns); err != nil {
			return fmt.Errorf("failed to build structure: %w", err)
		}
		return nil
	}

	// Build table structure
	if err := p.storage.BuildStructure(tableName, columns); err != nil {
		return fmt.Errorf("failed to build structure: %w", err)
	}

	// Calculate rows to read
	p.totalLines = numRows
	if p.limitLines > 0 && p.totalLines > p.limitLines {
		p.totalLines = p.limitLines
	}

	p.bar.ChangeMax(p.totalLines)

	// Read all columns data
	columnData := make([][]interface{}, len(columns))
	for i, path := range columnPaths {
		values, _, _, err := pr.ReadColumnByPath(path, int64(p.totalLines))
		if err != nil {
			return fmt.Errorf("failed to read column %s: %w", columns[i], err)
		}
		columnData[i] = values
	}

	// Insert rows
	for rowIdx := 0; rowIdx < p.totalLines; rowIdx++ {
		values := make([]any, len(columns))
		for colIdx := range columns {
			if rowIdx < len(columnData[colIdx]) {
				values[colIdx] = fmt.Sprintf("%v", columnData[colIdx][rowIdx])
			} else {
				values[colIdx] = ""
			}
		}

		if err := p.storage.InsertRow(tableName, columns, values); err != nil {
			return fmt.Errorf("failed to insert row %d: %w", rowIdx+1, err)
		}

		_ = p.bar.Add(1)
		p.currentLine++
	}

	return nil
}

// sanitizeColumnName sanitizes a string to be used as a SQL column name
func (p *parquetHandler) sanitizeColumnName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// formatTableName formats table name from file path
func (p *parquetHandler) formatTableName(filePath string) string {
	if p.collection != "" {
		tableName := strings.ReplaceAll(strings.ToLower(p.collection), " ", "_")
		return nonAlphanumericRegex.ReplaceAllString(tableName, "")
	}
	tableName := strings.ReplaceAll(strings.ToLower(filepath.Base(filePath)), filepath.Ext(filePath), "")
	tableName = strings.ReplaceAll(tableName, " ", "_")
	return nonAlphanumericRegex.ReplaceAllString(tableName, "")
}

// Lines returns total lines count
func (p *parquetHandler) Lines() int {
	return p.totalLines
}

// Close cleans up resources
func (p *parquetHandler) Close() error {
	return nil
}
