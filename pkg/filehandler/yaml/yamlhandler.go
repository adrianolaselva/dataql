package yaml

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/schollz/progressbar/v3"
	"gopkg.in/yaml.v3"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_ ]+`)

type yamlHandler struct {
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	files       []string
	totalLines  int
	limitLines  int
	currentLine int
	collection  string
}

// NewYamlHandler creates a new YAML file handler
func NewYamlHandler(files []string, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &yamlHandler{
		files:      files,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
	}
}

// Import imports data from YAML files
func (y *yamlHandler) Import() error {
	for _, file := range y.files {
		if err := y.importFile(file); err != nil {
			return err
		}
	}
	return nil
}

// importFile imports a single YAML file
func (y *yamlHandler) importFile(filePath string) error {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read YAML file: %w", err)
	}

	// Determine collection name
	collectionName := y.collection
	if collectionName == "" {
		baseName := filepath.Base(filePath)
		collectionName = strings.TrimSuffix(baseName, filepath.Ext(baseName))
	}
	collectionName = y.sanitizeName(collectionName)

	// Parse YAML
	var data interface{}
	if err := yaml.Unmarshal(content, &data); err != nil {
		return fmt.Errorf("failed to parse YAML: %w", err)
	}

	// Handle different YAML structures
	var records []map[string]interface{}
	switch v := data.(type) {
	case []interface{}:
		// Array of objects
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				records = append(records, m)
			}
		}
	case map[string]interface{}:
		// Single object or object with array value
		// Check if it contains an array we can use as records
		foundArray := false
		for _, val := range v {
			if arr, ok := val.([]interface{}); ok {
				for _, item := range arr {
					if m, ok := item.(map[string]interface{}); ok {
						records = append(records, m)
					}
				}
				foundArray = true
				break
			}
		}
		if !foundArray {
			// Treat the single object as one record
			records = append(records, v)
		}
	default:
		return fmt.Errorf("unsupported YAML structure: expected array or object")
	}

	if len(records) == 0 {
		// Empty data - create placeholder
		if err := y.storage.BuildStructure(collectionName, []string{"_empty"}); err != nil {
			return fmt.Errorf("failed to build structure for empty YAML: %w", err)
		}
		return nil
	}

	// Extract and flatten all columns
	columnSet := make(map[string]bool)
	for _, record := range records {
		flatRecord := y.flattenMap(record, "")
		for col := range flatRecord {
			columnSet[col] = true
		}
	}

	columns := make([]string, 0, len(columnSet))
	for col := range columnSet {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Collect sample rows for type inference (up to 100 rows)
	sampleSize := 100
	if len(records) < sampleSize {
		sampleSize = len(records)
	}
	sampleRows := make([][]any, sampleSize)
	for i := 0; i < sampleSize; i++ {
		flatRecord := y.flattenMap(records[i], "")
		row := make([]any, len(columns))
		for idx, col := range columns {
			if val, ok := flatRecord[col]; ok {
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
	if typedStorage, ok := y.storage.(storage.TypedStorage); ok {
		if err := typedStorage.BuildStructureWithTypes(collectionName, columnDefs); err != nil {
			return fmt.Errorf("failed to build structure with types: %w", err)
		}
	} else {
		if err := y.storage.BuildStructure(collectionName, columns); err != nil {
			return fmt.Errorf("failed to build structure: %w", err)
		}
	}

	// Check if storage supports type coercion
	typedStorage, hasTypedStorage := y.storage.(storage.TypedStorage)

	// Insert records
	for i, record := range records {
		if y.limitLines > 0 && i >= y.limitLines {
			break
		}

		flatRecord := y.flattenMap(record, "")
		values := make([]any, len(columns))
		for j, col := range columns {
			if val, ok := flatRecord[col]; ok && val != "" {
				values[j] = val
			} else {
				// For numeric/boolean columns, use nil instead of empty string
				if columnDefs[j].Type == storage.TypeBigInt ||
					columnDefs[j].Type == storage.TypeDouble ||
					columnDefs[j].Type == storage.TypeBoolean {
					values[j] = nil
				} else {
					values[j] = ""
				}
			}
		}

		var insertErr error
		if hasTypedStorage {
			insertErr = typedStorage.InsertRowWithCoercion(collectionName, columns, values, columnDefs)
		} else {
			insertErr = y.storage.InsertRow(collectionName, columns, values)
		}
		if insertErr != nil {
			return fmt.Errorf("failed to insert row: %w", insertErr)
		}

		y.totalLines++
		y.currentLine++
		_ = y.bar.Add(1)
	}

	return nil
}

// flattenMap flattens a nested map into a single-level map with underscore-separated keys
func (y *yamlHandler) flattenMap(data map[string]interface{}, prefix string) map[string]string {
	result := make(map[string]string)

	for key, value := range data {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "_" + key
		}
		fullKey = y.sanitizeName(fullKey)

		switch v := value.(type) {
		case map[string]interface{}:
			// Recursively flatten nested maps
			nested := y.flattenMap(v, fullKey)
			for k, val := range nested {
				result[k] = val
			}
		case []interface{}:
			// Convert arrays to JSON-like string
			result[fullKey] = fmt.Sprintf("%v", v)
		default:
			if v == nil {
				result[fullKey] = ""
			} else {
				result[fullKey] = fmt.Sprintf("%v", v)
			}
		}
	}

	return result
}

// sanitizeName sanitizes a string to be used as a SQL identifier
func (y *yamlHandler) sanitizeName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// Lines returns total lines count
func (y *yamlHandler) Lines() int {
	return y.totalLines
}

// Close cleans up resources
func (y *yamlHandler) Close() error {
	return nil
}
