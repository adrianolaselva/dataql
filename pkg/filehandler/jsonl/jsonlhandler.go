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

	// First pass: detect all columns
	columns, err := j.detectColumns(filePath)
	if err != nil {
		return fmt.Errorf("failed to detect columns: %w", err)
	}

	// Build table structure
	if err := j.storage.BuildStructure(tableName, columns); err != nil {
		return fmt.Errorf("failed to build structure: %w", err)
	}

	// Second pass: import data
	scanner := bufio.NewScanner(file)
	// Increase buffer size for large lines
	const maxScanTokenSize = 10 * 1024 * 1024 // 10MB
	buf := make([]byte, maxScanTokenSize)
	scanner.Buffer(buf, maxScanTokenSize)

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
			if val, ok := flat[col]; ok {
				values[idx] = val
			} else {
				values[idx] = ""
			}
		}

		if err := j.storage.InsertRow(tableName, columns, values); err != nil {
			return fmt.Errorf("failed to insert row %d: %w", lineNum, err)
		}

		_ = j.bar.Add(1)
		j.currentLine++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	return nil
}

// detectColumns scans the file to detect all unique columns
func (j *jsonlHandler) detectColumns(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	columnsSet := make(map[string]struct{})
	scanner := bufio.NewScanner(file)

	// Increase buffer size for large lines
	const maxScanTokenSize = 10 * 1024 * 1024 // 10MB
	buf := make([]byte, maxScanTokenSize)
	scanner.Buffer(buf, maxScanTokenSize)

	// Scan first N lines to detect schema (or all if limit is set)
	maxScan := 1000
	if j.limitLines > 0 && j.limitLines < maxScan {
		maxScan = j.limitLines
	}

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
		scanned++
	}

	columns := make([]string, 0, len(columnsSet))
	for col := range columnsSet {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// If no columns detected (empty file), add a placeholder column
	if len(columns) == 0 {
		columns = []string{"_empty"}
	}

	return columns, nil
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
func (j *jsonlHandler) formatTableName(filePath string) string {
	if j.collection != "" {
		tableName := strings.ReplaceAll(strings.ToLower(j.collection), " ", "_")
		return nonAlphanumericRegex.ReplaceAllString(tableName, "")
	}
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
