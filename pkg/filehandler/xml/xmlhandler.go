package xml

import (
	"encoding/xml"
	"errors"
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

type xmlHandler struct {
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	fileInputs  []string
	totalLines  int
	limitLines  int
	currentLine int
	collection  string
}

// NewXmlHandler creates a new XML file handler
func NewXmlHandler(fileInputs []string, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &xmlHandler{
		fileInputs: fileInputs,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
	}
}

// Import imports data from XML files
func (x *xmlHandler) Import() error {
	for _, filePath := range x.fileInputs {
		if err := x.loadFile(filePath); err != nil {
			return fmt.Errorf("failed to load file %s: %w", filePath, err)
		}
	}
	return nil
}

// loadFile loads a single XML file
func (x *xmlHandler) loadFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	tableName := x.formatTableName(filePath)

	// Parse XML and extract records
	records, err := x.parseXML(content)
	if err != nil {
		return fmt.Errorf("failed to parse XML: %w", err)
	}

	return x.importRecords(tableName, records)
}

// parseXML parses XML content and returns a slice of flat records
func (x *xmlHandler) parseXML(content []byte) ([]map[string]string, error) {
	decoder := xml.NewDecoder(strings.NewReader(string(content)))
	var records []map[string]string
	var currentRecord map[string]string
	var elementStack []string
	var charData strings.Builder
	var rootElement string
	var itemElement string
	foundRoot := false
	insideItem := false

	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("XML parse error: %w", err)
		}

		switch t := token.(type) {
		case xml.StartElement:
			if !foundRoot {
				// First element is the root
				rootElement = t.Name.Local
				foundRoot = true
				continue
			}

			if !insideItem {
				// This is an item element (direct child of root)
				if itemElement == "" {
					itemElement = t.Name.Local
				}
				if t.Name.Local == itemElement {
					insideItem = true
					currentRecord = make(map[string]string)
					// Process attributes for the item element
					for _, attr := range t.Attr {
						key := x.sanitizeColumnName(attr.Name.Local)
						currentRecord[key] = attr.Value
					}
				}
			} else if currentRecord != nil {
				// Inside an item, track nested elements
				elementStack = append(elementStack, t.Name.Local)
				// Process attributes
				for _, attr := range t.Attr {
					prefix := strings.Join(elementStack, "_")
					key := x.sanitizeColumnName(prefix + "_" + attr.Name.Local)
					currentRecord[key] = attr.Value
				}
			}
			charData.Reset()

		case xml.EndElement:
			if t.Name.Local == rootElement {
				continue
			}

			if t.Name.Local == itemElement && insideItem {
				// End of an item, save the record
				records = append(records, currentRecord)
				currentRecord = nil
				elementStack = nil
				insideItem = false
			} else if currentRecord != nil && len(elementStack) > 0 {
				// End of a nested element
				text := strings.TrimSpace(charData.String())
				if text != "" {
					prefix := strings.Join(elementStack, "_")
					key := x.sanitizeColumnName(prefix)
					currentRecord[key] = text
				}
				elementStack = elementStack[:len(elementStack)-1]
			}
			charData.Reset()

		case xml.CharData:
			charData.Write(t)
		}
	}

	// Handle case where XML has a single object (not a collection)
	if len(records) == 0 && foundRoot {
		// Try to parse as a single object
		singleRecord, err := x.parseAsSingleObject(content)
		if err == nil && len(singleRecord) > 0 {
			records = append(records, singleRecord)
		}
	}

	return records, nil
}

// parseAsSingleObject parses XML that represents a single object (not an array)
func (x *xmlHandler) parseAsSingleObject(content []byte) (map[string]string, error) {
	record := make(map[string]string)
	decoder := xml.NewDecoder(strings.NewReader(string(content)))
	var elementStack []string
	var charData strings.Builder
	depth := 0

	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		switch t := token.(type) {
		case xml.StartElement:
			depth++
			if depth > 1 {
				elementStack = append(elementStack, t.Name.Local)
				// Process attributes
				for _, attr := range t.Attr {
					prefix := strings.Join(elementStack, "_")
					key := x.sanitizeColumnName(prefix + "_" + attr.Name.Local)
					record[key] = attr.Value
				}
			} else {
				// Root element attributes
				for _, attr := range t.Attr {
					key := x.sanitizeColumnName(attr.Name.Local)
					record[key] = attr.Value
				}
			}
			charData.Reset()

		case xml.EndElement:
			if depth > 1 && len(elementStack) > 0 {
				text := strings.TrimSpace(charData.String())
				if text != "" {
					prefix := strings.Join(elementStack, "_")
					key := x.sanitizeColumnName(prefix)
					record[key] = text
				}
				elementStack = elementStack[:len(elementStack)-1]
			}
			depth--
			charData.Reset()

		case xml.CharData:
			charData.Write(t)
		}
	}

	return record, nil
}

// importRecords imports a slice of records into the database
func (x *xmlHandler) importRecords(tableName string, records []map[string]string) error {
	if len(records) == 0 {
		// Create empty table with placeholder column so queries can still run
		if err := x.storage.BuildStructure(tableName, []string{"_empty"}); err != nil {
			return fmt.Errorf("failed to build structure for empty XML: %w", err)
		}
		return nil
	}

	// Collect all columns from all records
	columnsSet := make(map[string]struct{})
	for _, record := range records {
		for col := range record {
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
	if len(records) < sampleSize {
		sampleSize = len(records)
	}
	sampleRows := make([][]any, sampleSize)
	for i := 0; i < sampleSize; i++ {
		row := make([]any, len(columns))
		for idx, col := range columns {
			if val, ok := records[i][col]; ok {
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
	if typedStorage, ok := x.storage.(storage.TypedStorage); ok {
		if err := typedStorage.BuildStructureWithTypes(tableName, columnDefs); err != nil {
			return fmt.Errorf("failed to build structure with types: %w", err)
		}
	} else {
		if err := x.storage.BuildStructure(tableName, columns); err != nil {
			return fmt.Errorf("failed to build structure: %w", err)
		}
	}

	x.totalLines = len(records)
	if x.limitLines > 0 && x.totalLines > x.limitLines {
		x.totalLines = x.limitLines
	}

	x.bar.ChangeMax(x.totalLines)

	// Insert records
	for i, record := range records {
		if x.limitLines > 0 && i >= x.limitLines {
			break
		}

		values := make([]any, len(columns))
		for idx, col := range columns {
			if val, ok := record[col]; ok && val != "" {
				values[idx] = val
			} else {
				// For numeric columns, use nil instead of empty string
				if columnDefs[idx].Type == storage.TypeBigInt || columnDefs[idx].Type == storage.TypeDouble {
					values[idx] = nil
				} else {
					values[idx] = ""
				}
			}
		}

		if err := x.storage.InsertRow(tableName, columns, values); err != nil {
			return fmt.Errorf("failed to insert row %d: %w", i+1, err)
		}

		_ = x.bar.Add(1)
		x.currentLine++
	}

	return nil
}

// sanitizeColumnName sanitizes a string to be used as a SQL column name
func (x *xmlHandler) sanitizeColumnName(name string) string {
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// formatTableName formats table name from file path
func (x *xmlHandler) formatTableName(filePath string) string {
	if x.collection != "" {
		tableName := strings.ReplaceAll(strings.ToLower(x.collection), " ", "_")
		return nonAlphanumericRegex.ReplaceAllString(tableName, "")
	}
	tableName := strings.ReplaceAll(strings.ToLower(filepath.Base(filePath)), filepath.Ext(filePath), "")
	tableName = strings.ReplaceAll(tableName, " ", "_")
	return nonAlphanumericRegex.ReplaceAllString(tableName, "")
}

// Lines returns total lines count
func (x *xmlHandler) Lines() int {
	return x.totalLines
}

// Close cleans up resources
func (x *xmlHandler) Close() error {
	return nil
}
