package json

import (
	"encoding/json"
	"fmt"
	"io"
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

type jsonHandler struct {
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	fileInputs  []string
	totalLines  int
	limitLines  int
	currentLine int
	collection  string
}

// NewJsonHandler creates a new JSON file handler
func NewJsonHandler(fileInputs []string, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &jsonHandler{
		fileInputs: fileInputs,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
	}
}

// Import imports data from JSON files
func (j *jsonHandler) Import() error {
	for _, filePath := range j.fileInputs {
		if err := j.loadFile(filePath); err != nil {
			return fmt.Errorf("failed to load file %s: %w", filePath, err)
		}
	}
	return nil
}

// loadFile loads a single JSON file
func (j *jsonHandler) loadFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	tableName := j.formatTableName(filePath)

	// Try to parse as array first
	var records []map[string]interface{}
	if err := json.Unmarshal(content, &records); err == nil {
		return j.importRecords(tableName, records)
	}

	// Try to parse as single object
	var record map[string]interface{}
	if err := json.Unmarshal(content, &record); err == nil {
		return j.importRecords(tableName, []map[string]interface{}{record})
	}

	return fmt.Errorf("invalid JSON format in file %s: expected array or object", filePath)
}

// importRecords imports a slice of records into the database
func (j *jsonHandler) importRecords(tableName string, records []map[string]interface{}) error {
	if len(records) == 0 {
		// Create empty table with placeholder column so queries can still run
		if err := j.storage.BuildStructure(tableName, []string{"_empty"}); err != nil {
			return fmt.Errorf("failed to build structure for empty array: %w", err)
		}
		return nil
	}

	// Flatten all records and collect all columns
	flattenedRecords := make([]map[string]string, 0, len(records))
	columnsSet := make(map[string]struct{})

	for _, record := range records {
		flat := j.flattenMap(record, "")
		flattenedRecords = append(flattenedRecords, flat)
		for col := range flat {
			columnsSet[col] = struct{}{}
		}
	}

	// Sort columns for consistent ordering
	columns := make([]string, 0, len(columnsSet))
	for col := range columnsSet {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Collect sample rows for type inference (up to 100 rows)
	sampleSize := 100
	if len(flattenedRecords) < sampleSize {
		sampleSize = len(flattenedRecords)
	}
	sampleRows := make([][]any, sampleSize)
	for i := 0; i < sampleSize; i++ {
		row := make([]any, len(columns))
		for idx, col := range columns {
			if val, ok := flattenedRecords[i][col]; ok {
				row[idx] = val
			} else {
				row[idx] = ""
			}
		}
		sampleRows[i] = row
	}

	// Infer column types from sample data
	columnDefs := storage.InferColumnTypes(columns, sampleRows)

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

	j.totalLines = len(flattenedRecords)
	if j.limitLines > 0 && j.totalLines > j.limitLines {
		j.totalLines = j.limitLines
	}

	j.bar.ChangeMax(j.totalLines)

	// Check if storage supports type coercion
	typedStorage, hasTypedStorage := j.storage.(storage.TypedStorage)

	// Insert records
	for i, record := range flattenedRecords {
		if j.limitLines > 0 && i >= j.limitLines {
			break
		}

		values := make([]any, len(columns))
		for idx, col := range columns {
			if val, ok := record[col]; ok && val != "" {
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
			return fmt.Errorf("failed to insert row %d: %w", i+1, insertErr)
		}

		_ = j.bar.Add(1)
		j.currentLine++
	}

	return nil
}

// flattenMap flattens a nested map into a single-level map with dot notation keys
func (j *jsonHandler) flattenMap(data map[string]interface{}, prefix string) map[string]string {
	result := make(map[string]string)

	for key, value := range data {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "_" + key
		}
		// Sanitize key for SQL column name
		fullKey = j.sanitizeColumnName(fullKey)

		switch v := value.(type) {
		case map[string]interface{}:
			// Recursively flatten nested objects
			nested := j.flattenMap(v, fullKey)
			for k, val := range nested {
				result[k] = val
			}
		case []interface{}:
			// Convert arrays to JSON string
			jsonBytes, _ := json.Marshal(v)
			result[fullKey] = string(jsonBytes)
		case nil:
			result[fullKey] = ""
		case float64:
			// Handle numbers - check if it's an integer
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
func (j *jsonHandler) sanitizeColumnName(name string) string {
	// Replace dots and special characters with underscores
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// formatTableName formats table name from file path
func (j *jsonHandler) formatTableName(filePath string) string {
	if j.collection != "" {
		tableName := strings.ReplaceAll(strings.ToLower(j.collection), " ", "_")
		return nonAlphanumericRegex.ReplaceAllString(tableName, "")
	}
	tableName := strings.ReplaceAll(strings.ToLower(filepath.Base(filePath)), filepath.Ext(filePath), "")
	tableName = strings.ReplaceAll(tableName, " ", "_")
	return nonAlphanumericRegex.ReplaceAllString(tableName, "")
}

// Lines returns total lines count
func (j *jsonHandler) Lines() int {
	return j.totalLines
}

// Close cleans up resources
func (j *jsonHandler) Close() error {
	return nil
}
