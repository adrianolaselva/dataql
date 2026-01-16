package orc

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/schollz/progressbar/v3"
	"github.com/scritchley/orc"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_ ]+`)

type orcHandler struct {
	mx          sync.Mutex
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	files       []string
	totalLines  int
	limitLines  int
	currentLine int
	collection  string
}

// NewOrcHandler creates a new ORC file handler
func NewOrcHandler(files []string, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &orcHandler{
		files:      files,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
	}
}

// Import imports data from ORC files
func (o *orcHandler) Import() error {
	for _, file := range o.files {
		if err := o.importFile(file); err != nil {
			return err
		}
	}
	return nil
}

// importFile imports a single ORC file
func (o *orcHandler) importFile(filePath string) error {
	// Open ORC file
	reader, err := orc.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open ORC file: %w", err)
	}
	defer reader.Close()

	// Determine collection name
	collectionName := o.collection
	if collectionName == "" {
		baseName := filepath.Base(filePath)
		collectionName = strings.TrimSuffix(baseName, filepath.Ext(baseName))
	}
	collectionName = o.sanitizeName(collectionName)

	// Get schema
	schema := reader.Schema()
	schemaColumns := schema.Columns()
	columns := make([]string, len(schemaColumns))
	for i, col := range schemaColumns {
		columns[i] = o.sanitizeName(col)
	}

	if len(columns) == 0 {
		// Empty schema - create placeholder
		if err := o.storage.BuildStructure(collectionName, []string{"_empty"}); err != nil {
			return fmt.Errorf("failed to build structure for empty ORC: %w", err)
		}
		return nil
	}

	// Build table structure
	if err := o.storage.BuildStructure(collectionName, columns); err != nil {
		return fmt.Errorf("failed to build structure: %w", err)
	}

	// Create cursor for reading
	cursor := reader.Select(columns...)

	// Read rows
	rowCount := 0
	for cursor.Stripes() {
		for cursor.Next() {
			if o.limitLines > 0 && rowCount >= o.limitLines {
				break
			}

			row := cursor.Row()
			values := make([]any, len(columns))
			for i, val := range row {
				if val == nil {
					values[i] = ""
				} else {
					values[i] = fmt.Sprintf("%v", val)
				}
			}

			if err := o.storage.InsertRow(collectionName, columns, values); err != nil {
				return fmt.Errorf("failed to insert row: %w", err)
			}

			rowCount++
			o.totalLines++
			o.currentLine++
			_ = o.bar.Add(1)
		}

		if o.limitLines > 0 && rowCount >= o.limitLines {
			break
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("error reading ORC file: %w", err)
	}

	return nil
}

// sanitizeName sanitizes a string to be used as a SQL identifier
func (o *orcHandler) sanitizeName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// Lines returns total lines count
func (o *orcHandler) Lines() int {
	return o.totalLines
}

// Close cleans up resources
func (o *orcHandler) Close() error {
	return nil
}
