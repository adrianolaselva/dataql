package filehandler

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/schollz/progressbar/v3"

	"github.com/adrianolaselva/dataql/pkg/storage"
)

// Format represents a supported file format
type Format string

const (
	FormatCSV      Format = "csv"
	FormatJSON     Format = "json"
	FormatJSONL    Format = "jsonl"
	FormatXML      Format = "xml"
	FormatExcel    Format = "excel"
	FormatParquet  Format = "parquet"
	FormatYAML     Format = "yaml"
	FormatAVRO     Format = "avro"
	FormatORC      Format = "orc"
	FormatPostgres Format = "postgres"
	FormatMySQL    Format = "mysql"
	FormatDuckDB   Format = "duckdb"
	FormatMongoDB  Format = "mongodb"
	FormatSQLite   Format = "sqlite"
	FormatMQ       Format = "mq" // Message Queue (SQS, Kafka, RabbitMQ, etc.)
)

// HandlerFactory creates file handlers based on format
type HandlerFactory struct {
	storage    storage.Storage
	bar        *progressbar.ProgressBar
	limitLines int
	collection string
	delimiter  rune
}

// NewHandlerFactory creates a new handler factory
func NewHandlerFactory(storage storage.Storage, bar *progressbar.ProgressBar, limitLines int, collection string, delimiter rune) *HandlerFactory {
	return &HandlerFactory{
		storage:    storage,
		bar:        bar,
		limitLines: limitLines,
		collection: collection,
		delimiter:  delimiter,
	}
}

// DetectFormat detects the file format from file extension or URL scheme
func DetectFormat(filePath string) (Format, error) {
	// Check for database URLs first
	if strings.HasPrefix(filePath, "postgres://") || strings.HasPrefix(filePath, "postgresql://") {
		return FormatPostgres, nil
	}
	if strings.HasPrefix(filePath, "mysql://") {
		return FormatMySQL, nil
	}
	if strings.HasPrefix(filePath, "duckdb://") {
		return FormatDuckDB, nil
	}
	if strings.HasPrefix(filePath, "mongodb://") || strings.HasPrefix(filePath, "mongodb+srv://") {
		return FormatMongoDB, nil
	}
	// Check for message queue URLs
	if IsMQURL(filePath) {
		return FormatMQ, nil
	}

	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".csv":
		return FormatCSV, nil
	case ".json":
		return FormatJSON, nil
	case ".jsonl", ".ndjson":
		return FormatJSONL, nil
	case ".xml":
		return FormatXML, nil
	case ".xlsx", ".xls", ".xlsm":
		return FormatExcel, nil
	case ".parquet", ".pq":
		return FormatParquet, nil
	case ".yaml", ".yml":
		return FormatYAML, nil
	case ".avro":
		return FormatAVRO, nil
	case ".orc":
		return FormatORC, nil
	case ".db", ".sqlite", ".sqlite3":
		return FormatSQLite, nil
	default:
		return "", fmt.Errorf("unsupported file format: %s", ext)
	}
}

// DetectFormatFromFiles detects the format from a list of files
// Returns error if files have mixed formats
func DetectFormatFromFiles(files []string) (Format, error) {
	if len(files) == 0 {
		return "", fmt.Errorf("no files provided")
	}

	var detectedFormat Format
	for i, file := range files {
		format, err := DetectFormat(file)
		if err != nil {
			return "", err
		}
		if i == 0 {
			detectedFormat = format
		} else if format != detectedFormat {
			return "", fmt.Errorf("mixed file formats not supported: found %s and %s", detectedFormat, format)
		}
	}

	return detectedFormat, nil
}

// SupportedFormats returns a list of supported file formats
func SupportedFormats() []Format {
	return []Format{FormatCSV, FormatJSON, FormatJSONL, FormatXML, FormatExcel, FormatParquet, FormatYAML, FormatAVRO, FormatORC}
}

// IsFormatSupported checks if a format is supported
func IsFormatSupported(format string) bool {
	f := Format(strings.ToLower(format))
	for _, supported := range SupportedFormats() {
		if f == supported {
			return true
		}
	}
	return false
}

// mqPrefixes are the URL prefixes for message queue systems
var mqPrefixes = []string{
	"sqs://",
	"kafka://",
	"rabbitmq://",
	"amqp://",
	"pulsar://",
	"pubsub://",
}

// IsMQURL checks if the given URL is a message queue URL
func IsMQURL(urlStr string) bool {
	lower := strings.ToLower(urlStr)
	for _, prefix := range mqPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}
