package avro

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/linkedin/goavro/v2"
	"github.com/schollz/progressbar/v3"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_ ]+`)

type avroHandler struct {
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	files       []string
	totalLines  int
	limitLines  int
	currentLine int
	collection  string
}

// NewAvroHandler creates a new AVRO file handler
func NewAvroHandler(files []string, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &avroHandler{
		files:      files,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
	}
}

// Import imports data from AVRO files
func (a *avroHandler) Import() error {
	for _, file := range a.files {
		if err := a.importFile(file); err != nil {
			return err
		}
	}
	return nil
}

// importFile imports a single AVRO file
func (a *avroHandler) importFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open AVRO file: %w", err)
	}
	defer file.Close()

	// Create OCF reader
	ocfReader, err := goavro.NewOCFReader(file)
	if err != nil {
		return fmt.Errorf("failed to create AVRO reader: %w", err)
	}

	// Determine collection name
	collectionName := a.collection
	if collectionName == "" {
		baseName := filepath.Base(filePath)
		collectionName = strings.TrimSuffix(baseName, filepath.Ext(baseName))
	}
	collectionName = a.sanitizeName(collectionName)

	// Read all records to determine schema
	var records []map[string]interface{}
	for ocfReader.Scan() {
		datum, err := ocfReader.Read()
		if err != nil {
			return fmt.Errorf("failed to read AVRO record: %w", err)
		}

		if record, ok := datum.(map[string]interface{}); ok {
			records = append(records, record)
		}
	}

	if err := ocfReader.Err(); err != nil {
		return fmt.Errorf("error reading AVRO file: %w", err)
	}

	if len(records) == 0 {
		// Empty data - create placeholder
		if err := a.storage.BuildStructure(collectionName, []string{"_empty"}); err != nil {
			return fmt.Errorf("failed to build structure for empty AVRO: %w", err)
		}
		return nil
	}

	// Extract columns from flattened records
	columnSet := make(map[string]bool)
	for _, record := range records {
		flatRecord := a.flattenMap(record, "")
		for col := range flatRecord {
			columnSet[col] = true
		}
	}

	columns := make([]string, 0, len(columnSet))
	for col := range columnSet {
		columns = append(columns, col)
	}

	// Build table structure
	if err := a.storage.BuildStructure(collectionName, columns); err != nil {
		return fmt.Errorf("failed to build structure: %w", err)
	}

	// Insert records
	for i, record := range records {
		if a.limitLines > 0 && i >= a.limitLines {
			break
		}

		flatRecord := a.flattenMap(record, "")
		values := make([]any, len(columns))
		for j, col := range columns {
			if val, ok := flatRecord[col]; ok {
				values[j] = val
			} else {
				values[j] = ""
			}
		}

		if err := a.storage.InsertRow(collectionName, columns, values); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}

		a.totalLines++
		a.currentLine++
		_ = a.bar.Add(1)
	}

	return nil
}

// flattenMap flattens a nested map into a single-level map
func (a *avroHandler) flattenMap(data map[string]interface{}, prefix string) map[string]string {
	result := make(map[string]string)

	for key, value := range data {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "_" + key
		}
		fullKey = a.sanitizeName(fullKey)

		switch v := value.(type) {
		case map[string]interface{}:
			// Recursively flatten nested maps
			nested := a.flattenMap(v, fullKey)
			for k, val := range nested {
				result[k] = val
			}
		case []interface{}:
			// Convert arrays to string
			result[fullKey] = fmt.Sprintf("%v", v)
		case nil:
			result[fullKey] = ""
		default:
			result[fullKey] = fmt.Sprintf("%v", v)
		}
	}

	return result
}

// sanitizeName sanitizes a string to be used as a SQL identifier
func (a *avroHandler) sanitizeName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// Lines returns total lines count
func (a *avroHandler) Lines() int {
	return a.totalLines
}

// Close cleans up resources
func (a *avroHandler) Close() error {
	return nil
}
