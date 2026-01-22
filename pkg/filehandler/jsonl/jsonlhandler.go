package jsonl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/schollz/progressbar/v3"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_ ]+`)

type jsonlHandler struct {
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	fileInputs  []string
	totalLines  int
	limitLines  int
	currentLine int
	collection  string
	aliases     map[string]string // Map of file path -> table alias
}

// NewJsonlHandler creates a new JSONL file handler
func NewJsonlHandler(fileInputs []string, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &jsonlHandler{
		fileInputs: fileInputs,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
	}
}

// NewJsonlHandlerWithAliases creates a new JSONL file handler with table aliases
func NewJsonlHandlerWithAliases(fileInputs []string, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string, aliases map[string]string) filehandler.FileHandler {
	return &jsonlHandler{
		fileInputs: fileInputs,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
		aliases:    aliases,
	}
}

// Import imports data from JSONL files
func (j *jsonlHandler) Import() error {
	// First pass: count lines and detect schema
	for _, filePath := range j.fileInputs {
		count, err := j.countLines(filePath)
		if err != nil {
			return fmt.Errorf("failed to count lines in %s: %w", filePath, err)
		}
		j.totalLines += count
	}

	if j.limitLines > 0 && j.totalLines > j.limitLines {
		j.totalLines = j.limitLines
	}

	j.bar.ChangeMax(j.totalLines)

	// Second pass: load data
	for _, filePath := range j.fileInputs {
		if err := j.loadFile(filePath); err != nil {
			return fmt.Errorf("failed to load file %s: %w", filePath, err)
		}
	}

	return nil
}

// countLines counts the number of lines in a file
func (j *jsonlHandler) countLines(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			count++
		}
	}
	return count, scanner.Err()
}

// loadFile loads a single JSONL file using streaming
func (j *jsonlHandler) loadFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	tableName := j.formatTableName(filePath)

	// First pass: detect all columns and their types
	columnDefs, columns, err := j.detectColumnsWithTypes(filePath)
	if err != nil {
		return fmt.Errorf("failed to detect columns: %w", err)
	}

	// Build table structure with inferred types if storage supports it
	if typedStorage, ok := j.storage.(storage.TypedStorage); ok {
		if err := typedStorage.BuildStructureWithTypes(tableName, columnDefs); err != nil {
			return fmt.Errorf("failed to build structure with types: %w", err)
		}
	} else {
		if err := j.storage.BuildStructure(tableName, columns); err != nil {
			return fmt.Errorf("failed to build structure: %w", err)
		}
	}

	// Second pass: import data
	scanner := bufio.NewScanner(file)
	// Increase buffer size for large lines
	const maxScanTokenSize = 10 * 1024 * 1024 // 10MB
	buf := make([]byte, maxScanTokenSize)
	scanner.Buffer(buf, maxScanTokenSize)

	// Check if storage supports type coercion
	typedStorage, hasTypedStorage := j.storage.(storage.TypedStorage)

	lineNum := 0
	for scanner.Scan() {
		if j.limitLines > 0 && j.currentLine >= j.limitLines {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		lineNum++

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return fmt.Errorf("failed to parse JSON at line %d: %w", lineNum, err)
		}

		flat := j.flattenMap(record, "")

		values := make([]any, len(columns))
		for idx, col := range columns {
			if val, ok := flat[col]; ok && val != "" {
				values[idx] = val
			} else {
				// For numeric/boolean columns, use nil instead of empty string
				if columnDefs[idx].Type == storage.TypeBigInt ||
					columnDefs[idx].Type == storage.TypeDouble ||
					columnDefs[idx].Type == storage.TypeBoolean {
					values[idx] = nil
				} else {
					values[idx] = ""
				}
			}
		}

		var insertErr error
		if hasTypedStorage {
			insertErr = typedStorage.InsertRowWithCoercion(tableName, columns, values, columnDefs)
		} else {
			insertErr = j.storage.InsertRow(tableName, columns, values)
		}
		if insertErr != nil {
			return fmt.Errorf("failed to insert row %d: %w", lineNum, insertErr)
		}

		_ = j.bar.Add(1)
		j.currentLine++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	return nil
}

// detectColumnsWithTypes scans the file to detect all unique columns and their types
func (j *jsonlHandler) detectColumnsWithTypes(filePath string) ([]storage.ColumnDef, []string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	columnsSet := make(map[string]struct{})
	scanner := bufio.NewScanner(file)

	// Increase buffer size for large lines
	const maxScanTokenSize = 10 * 1024 * 1024 // 10MB
	buf := make([]byte, maxScanTokenSize)
	scanner.Buffer(buf, maxScanTokenSize)

	// Scan first N lines to detect schema (or all if limit is set)
	maxScan := 100
	if j.limitLines > 0 && j.limitLines < maxScan {
		maxScan = j.limitLines
	}

	var sampleRecords []map[string]string
	scanned := 0
	for scanner.Scan() && scanned < maxScan {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue // Skip invalid lines for schema detection
		}

		flat := j.flattenMap(record, "")
		for col := range flat {
			columnsSet[col] = struct{}{}
		}
		sampleRecords = append(sampleRecords, flat)
		scanned++
	}

	columns := make([]string, 0, len(columnsSet))
	for col := range columnsSet {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// If no columns detected (empty file), add a placeholder column
	if len(columns) == 0 {
		return []storage.ColumnDef{{Name: "_empty", Type: storage.TypeVarchar}}, []string{"_empty"}, nil
	}

	// Convert sample records to [][]any for type inference
	sampleRows := make([][]any, len(sampleRecords))
	for i, record := range sampleRecords {
		row := make([]any, len(columns))
		for idx, col := range columns {
			if val, ok := record[col]; ok {
				row[idx] = val
			} else {
				row[idx] = ""
			}
		}
		sampleRows[i] = row
	}

	// Infer column types
	columnDefs := storage.InferColumnTypes(columns, sampleRows)

	return columnDefs, columns, nil
}

// flattenMap flattens a nested map into a single-level map with underscore notation keys
func (j *jsonlHandler) flattenMap(data map[string]interface{}, prefix string) map[string]string {
	result := make(map[string]string)

	for key, value := range data {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "_" + key
		}
		fullKey = j.sanitizeColumnName(fullKey)

		switch v := value.(type) {
		case map[string]interface{}:
			nested := j.flattenMap(v, fullKey)
			for k, val := range nested {
				result[k] = val
			}
		case []interface{}:
			jsonBytes, _ := json.Marshal(v)
			result[fullKey] = string(jsonBytes)
		case nil:
			result[fullKey] = ""
		case float64:
			if v == float64(int64(v)) {
				result[fullKey] = fmt.Sprintf("%d", int64(v))
			} else {
				result[fullKey] = fmt.Sprintf("%v", v)
			}
		case bool:
			if v {
				result[fullKey] = "true"
			} else {
				result[fullKey] = "false"
			}
		default:
			result[fullKey] = fmt.Sprintf("%v", v)
		}
	}

	return result
}

// sanitizeColumnName sanitizes a string to be used as a SQL column name
func (j *jsonlHandler) sanitizeColumnName(name string) string {
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// formatTableName formats table name from file path
// Priority: 1) alias from aliases map, 2) collection, 3) filename
func (j *jsonlHandler) formatTableName(filePath string) string {
	// Check if there's an alias for this file
	if j.aliases != nil {
		if alias, ok := j.aliases[filePath]; ok && alias != "" {
			tableName := strings.ReplaceAll(strings.ToLower(alias), " ", "_")
			return nonAlphanumericRegex.ReplaceAllString(tableName, "")
		}
	}

	// Use collection if provided
	if j.collection != "" {
		tableName := strings.ReplaceAll(strings.ToLower(j.collection), " ", "_")
		return nonAlphanumericRegex.ReplaceAllString(tableName, "")
	}

	// Default: use filename
	tableName := strings.ReplaceAll(strings.ToLower(filepath.Base(filePath)), filepath.Ext(filePath), "")
	tableName = strings.ReplaceAll(tableName, " ", "_")
	return nonAlphanumericRegex.ReplaceAllString(tableName, "")
}

// Lines returns total lines count
func (j *jsonlHandler) Lines() int {
	return j.totalLines
}

// Close cleans up resources
func (j *jsonlHandler) Close() error {
	return nil
}
