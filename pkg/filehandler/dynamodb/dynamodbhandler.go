package dynamodb

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/adrianolaselva/dataql/pkg/dbconnector"
	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/schollz/progressbar/v3"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_ ]+`)

// ConnectionInfo holds parsed DynamoDB connection information
type ConnectionInfo struct {
	Region    string
	TableName string
	Endpoint  string // Optional: for LocalStack or local DynamoDB
}

type dynamodbHandler struct {
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	connInfo    ConnectionInfo
	totalLines  int
	limitLines  int
	currentLine int
	collection  string
}

// NewDynamoDBHandler creates a new DynamoDB file handler
func NewDynamoDBHandler(connInfo ConnectionInfo, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &dynamodbHandler{
		connInfo:   connInfo,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
	}
}

// Import imports data from DynamoDB
func (d *dynamodbHandler) Import() error {
	// Create connector
	cfg := dbconnector.DynamoDBConfig{
		Region:    d.connInfo.Region,
		TableName: d.connInfo.TableName,
		Endpoint:  d.connInfo.Endpoint,
	}

	connector, err := dbconnector.NewDynamoDBConnector(cfg)
	if err != nil {
		return fmt.Errorf("failed to create DynamoDB connector: %w", err)
	}
	defer connector.Close()

	if err := connector.Connect(); err != nil {
		return fmt.Errorf("failed to connect to DynamoDB: %w", err)
	}

	// Get collection/table name for SQLite
	collectionName := d.connInfo.TableName
	if d.collection != "" {
		collectionName = d.collection
	}

	// Get schema from first item
	schema, err := connector.GetTableSchema(d.connInfo.TableName)
	if err != nil {
		return fmt.Errorf("failed to get table schema: %w", err)
	}

	if len(schema) == 0 {
		// Empty table - create placeholder
		if err := d.storage.BuildStructure(d.sanitizeName(collectionName), []string{"_empty"}); err != nil {
			return fmt.Errorf("failed to build structure for empty table: %w", err)
		}
		return nil
	}

	// Convert schema to column names
	columns := make([]string, len(schema))
	for i, col := range schema {
		columns[i] = d.sanitizeName(col.Name)
	}

	// Build table structure
	if err := d.storage.BuildStructure(d.sanitizeName(collectionName), columns); err != nil {
		return fmt.Errorf("failed to build structure: %w", err)
	}

	// Read data from the table
	items, err := connector.ReadItems(d.connInfo.TableName, d.limitLines)
	if err != nil {
		return fmt.Errorf("failed to read table: %w", err)
	}

	// Read and insert items
	for _, item := range items {
		values := make([]any, len(columns))
		for i, col := range schema {
			val, ok := item[col.Name]
			if !ok || val == nil {
				values[i] = ""
			} else {
				values[i] = d.formatValue(val)
			}
		}

		if err := d.storage.InsertRow(d.sanitizeName(collectionName), columns, values); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}

		d.totalLines++
		d.currentLine++
		_ = d.bar.Add(1)
	}

	return nil
}

// formatValue formats a DynamoDB value to string
func (d *dynamodbHandler) formatValue(val interface{}) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case map[string]interface{}:
		// Flatten nested maps to JSON-like string
		return fmt.Sprintf("%v", v)
	case []interface{}:
		// Convert arrays to string representation
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// sanitizeName sanitizes a string to be used as a SQL column/table name
func (d *dynamodbHandler) sanitizeName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// Lines returns total lines count
func (d *dynamodbHandler) Lines() int {
	return d.totalLines
}

// Close cleans up resources
func (d *dynamodbHandler) Close() error {
	return nil
}

// ParseDynamoDBURL parses a DynamoDB URL and returns connection info
// Format: dynamodb://region/table-name
//
//	dynamodb://region/table-name?endpoint=http://localhost:8000
func ParseDynamoDBURL(urlStr string) (*ConnectionInfo, error) {
	// Remove the dynamodb:// prefix for parsing
	if !strings.HasPrefix(urlStr, "dynamodb://") {
		return nil, fmt.Errorf("invalid DynamoDB URL: must start with dynamodb://")
	}

	// Parse as URL to handle query parameters
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DynamoDB URL: %w", err)
	}

	info := &ConnectionInfo{}

	// The host part is the region
	info.Region = parsedURL.Host
	if info.Region == "" {
		return nil, fmt.Errorf("invalid DynamoDB URL: missing region (format: dynamodb://region/table-name)")
	}

	// The path is the table name
	tableName := strings.TrimPrefix(parsedURL.Path, "/")
	if tableName == "" {
		return nil, fmt.Errorf("invalid DynamoDB URL: missing table name (format: dynamodb://region/table-name)")
	}
	info.TableName = tableName

	// Parse query parameters for endpoint
	queryParams := parsedURL.Query()
	if endpoint := queryParams.Get("endpoint"); endpoint != "" {
		info.Endpoint = endpoint
	}

	return info, nil
}

// IsDynamoDBURL checks if a string is a DynamoDB URL
func IsDynamoDBURL(str string) bool {
	return strings.HasPrefix(str, "dynamodb://")
}
