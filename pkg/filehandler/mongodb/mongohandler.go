package mongodb

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/adrianolaselva/dataql/pkg/dbconnector"
	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/schollz/progressbar/v3"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_ ]+`)

// ConnectionInfo holds parsed MongoDB connection information
type ConnectionInfo struct {
	Host       string
	Port       int
	User       string
	Password   string
	Database   string
	Collection string
	Options    string
}

type mongoHandler struct {
	mx          sync.Mutex
	bar         *progressbar.ProgressBar
	storage     storage.Storage
	connInfo    ConnectionInfo
	totalLines  int
	limitLines  int
	currentLine int
	collection  string
}

// NewMongoHandler creates a new MongoDB file handler
func NewMongoHandler(connInfo ConnectionInfo, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) filehandler.FileHandler {
	return &mongoHandler{
		connInfo:   connInfo,
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
	}
}

// Import imports data from MongoDB
func (m *mongoHandler) Import() error {
	// Create connector
	config := dbconnector.Config{
		Type:     dbconnector.DBTypeMongoDB,
		Host:     m.connInfo.Host,
		Port:     m.connInfo.Port,
		User:     m.connInfo.User,
		Password: m.connInfo.Password,
		Database: m.connInfo.Database,
	}

	connector, err := dbconnector.NewMongoDBConnector(config)
	if err != nil {
		return fmt.Errorf("failed to create MongoDB connector: %w", err)
	}
	defer connector.Close()

	if err := connector.Connect(); err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Get collection name
	collectionName := m.connInfo.Collection
	if m.collection != "" {
		collectionName = m.collection
	}

	// Get schema from first document
	schema, err := connector.GetTableSchema(m.connInfo.Collection)
	if err != nil {
		return fmt.Errorf("failed to get collection schema: %w", err)
	}

	if len(schema) == 0 {
		// Empty collection - create placeholder
		if err := m.storage.BuildStructure(m.sanitizeName(collectionName), []string{"_empty"}); err != nil {
			return fmt.Errorf("failed to build structure for empty collection: %w", err)
		}
		return nil
	}

	// Convert schema to column names
	columns := make([]string, len(schema))
	for i, col := range schema {
		columns[i] = m.sanitizeName(col.Name)
	}

	// Build table structure
	if err := m.storage.BuildStructure(m.sanitizeName(collectionName), columns); err != nil {
		return fmt.Errorf("failed to build structure: %w", err)
	}

	// Read data from the collection
	docs, err := connector.ReadCollection(m.connInfo.Collection, m.limitLines)
	if err != nil {
		return fmt.Errorf("failed to read collection: %w", err)
	}

	// Read and insert documents
	for _, doc := range docs {
		values := make([]any, len(columns))
		for i, col := range schema {
			val, ok := doc[col.Name]
			if !ok || val == nil {
				values[i] = ""
			} else {
				values[i] = m.formatValue(val)
			}
		}

		if err := m.storage.InsertRow(m.sanitizeName(collectionName), columns, values); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}

		m.totalLines++
		m.currentLine++
		_ = m.bar.Add(1)
	}

	return nil
}

// formatValue formats a MongoDB value to string
func (m *mongoHandler) formatValue(val interface{}) string {
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
	default:
		return fmt.Sprintf("%v", v)
	}
}

// sanitizeName sanitizes a string to be used as a SQL column/table name
func (m *mongoHandler) sanitizeName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// Lines returns total lines count
func (m *mongoHandler) Lines() int {
	return m.totalLines
}

// Close cleans up resources
func (m *mongoHandler) Close() error {
	return nil
}

// ParseMongoDBURL parses a MongoDB URL and returns connection info
// Format: mongodb://user:password@host:port/database/collection
//         mongodb+srv://user:password@host/database/collection
func ParseMongoDBURL(urlStr string) (*ConnectionInfo, error) {
	// Remove the mongodb:// or mongodb+srv:// prefix for parsing
	if !strings.HasPrefix(urlStr, "mongodb://") && !strings.HasPrefix(urlStr, "mongodb+srv://") {
		return nil, fmt.Errorf("invalid MongoDB URL: must start with mongodb:// or mongodb+srv://")
	}

	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse MongoDB URL: %w", err)
	}

	info := &ConnectionInfo{
		Host: parsedURL.Hostname(),
		Port: 27017, // Default MongoDB port
	}

	// Parse port if present
	if parsedURL.Port() != "" {
		port, err := strconv.Atoi(parsedURL.Port())
		if err == nil {
			info.Port = port
		}
	}

	// Parse user/password
	if parsedURL.User != nil {
		info.User = parsedURL.User.Username()
		info.Password, _ = parsedURL.User.Password()
	}

	// Parse path for database and collection
	// Path format: /database/collection
	path := strings.TrimPrefix(parsedURL.Path, "/")
	parts := strings.SplitN(path, "/", 2)

	if len(parts) < 1 || parts[0] == "" {
		return nil, fmt.Errorf("invalid MongoDB URL: missing database name")
	}
	info.Database = parts[0]

	if len(parts) < 2 || parts[1] == "" {
		return nil, fmt.Errorf("invalid MongoDB URL: missing collection name (format: mongodb://host:port/database/collection)")
	}
	info.Collection = parts[1]

	// Store query string options
	info.Options = parsedURL.RawQuery

	return info, nil
}

// IsMongoDBURL checks if a string is a MongoDB URL
func IsMongoDBURL(str string) bool {
	return strings.HasPrefix(str, "mongodb://") || strings.HasPrefix(str, "mongodb+srv://")
}
