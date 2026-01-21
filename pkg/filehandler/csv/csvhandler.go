package csv

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/schollz/progressbar/v3"
)

const (
	bufferMaxLength = 32 * 1024
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_ ]+`)

type csvHandler struct {
	mx          sync.Mutex
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	files       []*os.File
	fileInputs  []string
	totalLines  int
	limitLines  int
	currentLine int
	delimiter   rune
	collection  string
}

// NewCsvHandler creates a new CSV file handler
func NewCsvHandler(fileInputs []string, delimiter rune, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &csvHandler{fileInputs: fileInputs, delimiter: delimiter, storage: storage, bar: bar, limitLines: limitLines, collection: collection}
}

// Import imports data from CSV files
func (c *csvHandler) Import() error {
	if err := c.openFiles(); err != nil {
		return err
	}

	// Load total rows from all files
	if err := c.countTotalRows(); err != nil {
		return err
	}

	if c.limitLines > 0 && c.totalLines > c.limitLines {
		c.totalLines = c.limitLines
	}

	// Load data from all files
	if err := c.loadAllFiles(); err != nil {
		return err
	}

	return nil
}

// countTotalRows counts total rows across all files in parallel
func (c *csvHandler) countTotalRows() error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(c.fileInputs))
	linesChan := make(chan int, len(c.fileInputs))

	for _, file := range c.fileInputs {
		wg.Add(1)
		go func(filePath string) {
			defer wg.Done()
			lines, err := c.countFileLines(filePath)
			if err != nil {
				errChan <- err
				return
			}
			linesChan <- lines
		}(file)
	}

	wg.Wait()
	close(errChan)
	close(linesChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	// Sum all lines
	c.totalLines = 0
	for lines := range linesChan {
		c.totalLines += lines
	}

	return nil
}

// countFileLines counts the number of lines in a file
func (c *csvHandler) countFileLines(filePath string) (int, error) {
	r, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer func(r *os.File) {
		_ = r.Close()
	}(r)

	buf := make([]byte, bufferMaxLength)
	count := 0
	lineSep := []byte{'\n'}

	for {
		n, err := r.Read(buf)
		count += bytes.Count(buf[:n], lineSep)

		switch {
		case err == io.EOF:
			return count, nil
		case err != nil:
			return 0, fmt.Errorf("failed to count rows: %w", err)
		}
	}
}

// loadAllFiles loads data from all files in parallel
func (c *csvHandler) loadAllFiles() error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(c.files))

	for _, file := range c.files {
		wg.Add(1)
		tableName := c.formatTableName(file)
		go func(f *os.File, tbl string) {
			defer wg.Done()
			if err := c.loadDataFromFile(tbl, f); err != nil {
				errChan <- err
			}
		}(file, tableName)
	}

	wg.Wait()
	close(errChan)

	// Return the first error encountered
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// formatTableName formats table name by removing invalid characters
// If collection is provided, it will be used as the table name
func (c *csvHandler) formatTableName(file *os.File) string {
	if c.collection != "" {
		tableName := strings.ReplaceAll(strings.ToLower(c.collection), " ", "_")
		return nonAlphanumericRegex.ReplaceAllString(tableName, "")
	}
	tableName := strings.ReplaceAll(strings.ToLower(filepath.Base(file.Name())), filepath.Ext(file.Name()), "")
	tableName = strings.ReplaceAll(tableName, " ", "_")
	return nonAlphanumericRegex.ReplaceAllString(tableName, "")
}

// Query executes SQL statements
func (c *csvHandler) Query(cmd string) (*sql.Rows, error) {
	rows, err := c.storage.Query(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

// Lines returns total lines count
func (c *csvHandler) Lines() int {
	return c.totalLines
}

// Close cleans up resources
func (c *csvHandler) Close() error {
	defer func(storage storage.Storage) {
		_ = storage.Close()
	}(c.storage)

	defer func(files []*os.File) {
		for _, file := range files {
			_ = file.Close()
		}
	}(c.files)

	return nil
}

// loadDataFromFile loads data from a single file
func (c *csvHandler) loadDataFromFile(tableName string, file *os.File) error {
	c.mx.Lock()
	defer c.mx.Unlock()

	c.bar.ChangeMax(c.totalLines)

	r := csv.NewReader(file)
	r.Comma = c.delimiter

	// Read header
	columns, err := r.Read()
	if err != nil {
		return fmt.Errorf("failed to load headers: %w", err)
	}

	// Collect sample rows for type inference (up to 100 rows)
	const sampleSize = 100
	var sampleRows [][]any
	var allRecords [][]string // Store all records if we need to replay

	for i := 0; i < sampleSize; i++ {
		record, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read sample row: %w", err)
		}
		sampleRows = append(sampleRows, c.convertToAnyArray(record))
		allRecords = append(allRecords, record)
	}

	// Infer column types from sample data
	columnDefs := storage.InferColumnTypes(columns, sampleRows)

	// Create table structure with inferred types if storage supports it
	if typedStorage, ok := c.storage.(storage.TypedStorage); ok {
		if err := typedStorage.BuildStructureWithTypes(tableName, columnDefs); err != nil {
			return fmt.Errorf("failed to build structure with types: %w", err)
		}
	} else {
		if err := c.storage.BuildStructure(tableName, columns); err != nil {
			return fmt.Errorf("failed to build structure: %w", err)
		}
	}

	// Check if storage supports type coercion
	typedStorage, hasTypedStorage := c.storage.(storage.TypedStorage)

	// Insert the sample rows we already read
	c.currentLine = 0
	for _, record := range allRecords {
		if c.limitLines > 0 && c.currentLine >= c.limitLines {
			return nil
		}
		_ = c.bar.Add(1)
		c.currentLine++

		values := c.convertToAnyArrayWithTypes(record, columnDefs)
		var insertErr error
		if hasTypedStorage {
			insertErr = typedStorage.InsertRowWithCoercion(tableName, columns, values, columnDefs)
		} else {
			insertErr = c.storage.InsertRow(tableName, columns, values)
		}
		if insertErr != nil {
			return fmt.Errorf("failed to process row number %d: %w", c.currentLine, insertErr)
		}
	}

	// Continue reading the rest of the file
	for {
		if c.limitLines > 0 && c.currentLine >= c.limitLines {
			break
		}

		record, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read line: %w", err)
		}

		_ = c.bar.Add(1)
		c.currentLine++

		values := c.convertToAnyArrayWithTypes(record, columnDefs)
		var insertErr error
		if hasTypedStorage {
			insertErr = typedStorage.InsertRowWithCoercion(tableName, columns, values, columnDefs)
		} else {
			insertErr = c.storage.InsertRow(tableName, columns, values)
		}
		if insertErr != nil {
			return fmt.Errorf("failed to process row number %d: %w", c.currentLine, insertErr)
		}
	}

	return nil
}

// convertToAnyArray converts string array to any array
func (c *csvHandler) convertToAnyArray(records []string) []any {
	values := make([]any, 0, len(records))
	for _, r := range records {
		values = append(values, r)
	}

	return values
}

// convertToAnyArrayWithTypes converts string array to any array, using nil for empty numeric values
func (c *csvHandler) convertToAnyArrayWithTypes(records []string, columnDefs []storage.ColumnDef) []any {
	values := make([]any, len(records))
	for i, r := range records {
		if r == "" && i < len(columnDefs) {
			// For numeric columns, use nil instead of empty string
			if columnDefs[i].Type == storage.TypeBigInt || columnDefs[i].Type == storage.TypeDouble {
				values[i] = nil
			} else {
				values[i] = r
			}
		} else {
			values[i] = r
		}
	}
	return values
}

// openFiles opens all input files
func (c *csvHandler) openFiles() error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	errChan := make(chan error, len(c.fileInputs))

	for _, filePath := range c.fileInputs {
		wg.Add(1)
		go func(fp string) {
			defer wg.Done()

			f, err := os.Open(fp)
			if err != nil {
				errChan <- fmt.Errorf("failed to open file %s: %w", fp, err)
				return
			}

			mu.Lock()
			c.files = append(c.files, f)
			mu.Unlock()
		}(filePath)
	}

	wg.Wait()
	close(errChan)

	// Return the first error encountered
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}
