package filehandler

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/schollz/progressbar/v3"
	"github.com/ulikunitz/xz"

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
	FormatDynamoDB Format = "dynamodb"
	FormatSQLite   Format = "sqlite"
	FormatMQ       Format = "mq"    // Message Queue (SQS, Kafka, RabbitMQ, etc.)
	FormatMixed    Format = "mixed" // Mixed file formats (for JOINs across different formats)
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
	if strings.HasPrefix(filePath, "dynamodb://") {
		return FormatDynamoDB, nil
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
// If all files have the same format, returns that format
// If files have mixed formats, returns FormatMixed
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
			// Mixed formats detected - return special format
			return FormatMixed, nil
		}
	}

	return detectedFormat, nil
}

// DetectFormatsFromFiles returns a map of file path to format for each file
func DetectFormatsFromFiles(files []string) (map[string]Format, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no files provided")
	}

	result := make(map[string]Format)
	for _, file := range files {
		format, err := DetectFormat(file)
		if err != nil {
			return nil, err
		}
		result[file] = format
	}

	return result, nil
}

// GroupFilesByFormat groups files by their format
func GroupFilesByFormat(files []string) (map[Format][]string, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no files provided")
	}

	result := make(map[Format][]string)
	for _, file := range files {
		format, err := DetectFormat(file)
		if err != nil {
			return nil, err
		}
		result[format] = append(result[format], file)
	}

	return result, nil
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

// Compression represents a supported compression format
type Compression string

const (
	CompressionNone  Compression = ""
	CompressionGzip  Compression = "gzip"
	CompressionBzip2 Compression = "bzip2"
	CompressionXZ    Compression = "xz"
	CompressionZstd  Compression = "zstd"
)

// compressionExtensions maps file extensions to compression types
var compressionExtensions = map[string]Compression{
	".gz":   CompressionGzip,
	".gzip": CompressionGzip,
	".bz2":  CompressionBzip2,
	".xz":   CompressionXZ,
	".zst":  CompressionZstd,
	".zstd": CompressionZstd,
}

// DetectCompression detects if a file is compressed and returns the compression type
func DetectCompression(filePath string) Compression {
	ext := strings.ToLower(filepath.Ext(filePath))
	if compression, ok := compressionExtensions[ext]; ok {
		return compression
	}
	return CompressionNone
}

// IsCompressed checks if a file path has a compression extension
func IsCompressed(filePath string) bool {
	return DetectCompression(filePath) != CompressionNone
}

// GetUncompressedPath returns the path without the compression extension
// e.g., "data.csv.gz" -> "data.csv"
func GetUncompressedPath(filePath string) string {
	compression := DetectCompression(filePath)
	if compression == CompressionNone {
		return filePath
	}
	// Remove the compression extension
	return strings.TrimSuffix(filePath, filepath.Ext(filePath))
}

// GetInnerFormat detects the format of a compressed file
// e.g., "data.csv.gz" -> FormatCSV
func GetInnerFormat(filePath string) (Format, error) {
	uncompressedPath := GetUncompressedPath(filePath)
	return DetectFormat(uncompressedPath)
}

// DecompressFile decompresses a file to a temporary file and returns the temp file path
// The caller is responsible for cleaning up the temp file
func DecompressFile(filePath string) (string, error) {
	compression := DetectCompression(filePath)
	if compression == CompressionNone {
		return filePath, nil
	}

	// Open the compressed file
	inputFile, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open compressed file: %w", err)
	}
	defer inputFile.Close()

	// Create a temp file with the inner extension
	innerExt := filepath.Ext(GetUncompressedPath(filePath))
	tempFile, err := os.CreateTemp("", "dataql_decompressed_*"+innerExt)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	// Create appropriate decompressor
	var reader io.Reader
	switch compression {
	case CompressionGzip:
		gzReader, err := gzip.NewReader(inputFile)
		if err != nil {
			os.Remove(tempFile.Name())
			return "", fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	case CompressionBzip2:
		reader = bzip2.NewReader(inputFile)
	case CompressionXZ:
		xzReader, err := xz.NewReader(inputFile)
		if err != nil {
			os.Remove(tempFile.Name())
			return "", fmt.Errorf("failed to create xz reader: %w", err)
		}
		reader = xzReader
	case CompressionZstd:
		// For zstd, we'll need to add the library or suggest users install it
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("zstd compression not yet supported (coming soon)")
	default:
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("unsupported compression: %s", compression)
	}

	// Copy decompressed data to temp file
	if _, err := io.Copy(tempFile, reader); err != nil {
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("failed to decompress file: %w", err)
	}

	return tempFile.Name(), nil
}

// DecompressedFileInfo holds information about a decompressed file
type DecompressedFileInfo struct {
	OriginalPath   string
	DecompressPath string
	WasCompressed  bool
	Format         Format
}

// DecompressFiles decompresses all compressed files in the list
// Returns a list of DecompressedFileInfo with paths to the decompressed files
func DecompressFiles(files []string) ([]DecompressedFileInfo, error) {
	result := make([]DecompressedFileInfo, len(files))

	for i, file := range files {
		if IsCompressed(file) {
			innerFormat, err := GetInnerFormat(file)
			if err != nil {
				// Clean up any temp files we've created
				for j := 0; j < i; j++ {
					if result[j].WasCompressed {
						os.Remove(result[j].DecompressPath)
					}
				}
				return nil, err
			}

			decompressedPath, err := DecompressFile(file)
			if err != nil {
				// Clean up any temp files we've created
				for j := 0; j < i; j++ {
					if result[j].WasCompressed {
						os.Remove(result[j].DecompressPath)
					}
				}
				return nil, err
			}

			result[i] = DecompressedFileInfo{
				OriginalPath:   file,
				DecompressPath: decompressedPath,
				WasCompressed:  true,
				Format:         innerFormat,
			}
		} else {
			format, err := DetectFormat(file)
			if err != nil {
				// Clean up any temp files we've created
				for j := 0; j < i; j++ {
					if result[j].WasCompressed {
						os.Remove(result[j].DecompressPath)
					}
				}
				return nil, err
			}

			result[i] = DecompressedFileInfo{
				OriginalPath:   file,
				DecompressPath: file,
				WasCompressed:  false,
				Format:         format,
			}
		}
	}

	return result, nil
}

// CleanupDecompressedFiles removes temporary decompressed files
func CleanupDecompressedFiles(files []DecompressedFileInfo) {
	for _, f := range files {
		if f.WasCompressed && f.DecompressPath != "" {
			os.Remove(f.DecompressPath)
		}
	}
}
