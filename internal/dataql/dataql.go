package dataql

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/adrianolaselva/dataql/internal/exportdata"
	"github.com/adrianolaselva/dataql/pkg/azurehandler"
	"github.com/adrianolaselva/dataql/pkg/compressionhandler"
	"github.com/adrianolaselva/dataql/pkg/filehandler"
	avroHandler "github.com/adrianolaselva/dataql/pkg/filehandler/avro"
	compositeHandler "github.com/adrianolaselva/dataql/pkg/filehandler/composite"
	csvHandler "github.com/adrianolaselva/dataql/pkg/filehandler/csv"
	databaseHandler "github.com/adrianolaselva/dataql/pkg/filehandler/database"
	dynamodbHandler "github.com/adrianolaselva/dataql/pkg/filehandler/dynamodb"
	excelHandler "github.com/adrianolaselva/dataql/pkg/filehandler/excel"
	jsonHandler "github.com/adrianolaselva/dataql/pkg/filehandler/json"
	jsonlHandler "github.com/adrianolaselva/dataql/pkg/filehandler/jsonl"
	mongodbHandler "github.com/adrianolaselva/dataql/pkg/filehandler/mongodb"
	mqHandler "github.com/adrianolaselva/dataql/pkg/filehandler/mq"
	orcHandler "github.com/adrianolaselva/dataql/pkg/filehandler/orc"
	parquetHandler "github.com/adrianolaselva/dataql/pkg/filehandler/parquet"
	sqliteHandler "github.com/adrianolaselva/dataql/pkg/filehandler/sqlitedb"
	xmlHandler "github.com/adrianolaselva/dataql/pkg/filehandler/xml"
	yamlHandler "github.com/adrianolaselva/dataql/pkg/filehandler/yaml"
	"github.com/adrianolaselva/dataql/pkg/gcshandler"
	"github.com/adrianolaselva/dataql/pkg/queryerror"
	"github.com/adrianolaselva/dataql/pkg/repl"
	"github.com/adrianolaselva/dataql/pkg/s3handler"
	"github.com/adrianolaselva/dataql/pkg/stdinhandler"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/adrianolaselva/dataql/pkg/storage/duckdb"
	"github.com/adrianolaselva/dataql/pkg/urlhandler"
	"github.com/chzyer/readline"
	"github.com/fatih/color"
	"github.com/rodaine/table"
	"github.com/schollz/progressbar/v3"
)

const (
	cliInterruptPrompt = "^C"
	cliEOFPrompt       = "exit"
	defaultPageSize    = 25
)

// Version is set by the main package during initialization
var Version = "dev"

// DataQL is the main interface for the data query engine
type DataQL interface {
	Run() error
	RunStorageOnly() error
	Close() error
}

type dataQL struct {
	storage            storage.Storage
	bar                *progressbar.ProgressBar
	params             Params
	fileHandler        filehandler.FileHandler
	urlHandler         *urlhandler.URLHandler
	s3Handler          *s3handler.S3Handler
	gcsHandler         *gcshandler.GCSHandler
	azureHandler       *azurehandler.AzureHandler
	stdinHandler       *stdinhandler.StdinHandler
	compressionHandler *compressionhandler.CompressionHandler
	pageSize           int
	paging             bool // Enable paging in REPL mode
	showTiming         bool // Show query execution time
	truncate           int  // Truncate column values longer than N characters
	vertical           bool // Display results in vertical format
}

// verboseLog prints a message if verbose mode is enabled
func verboseLog(verbose bool, format string, args ...interface{}) {
	if verbose {
		fmt.Printf("[VERBOSE] "+format+"\n", args...)
	}
}

// New creates a new DataQL instance
func New(params Params) (DataQL, error) {
	verboseLog(params.Verbose, "Starting DataQL initialization...")
	verboseLog(params.Verbose, "File inputs: %v", params.FileInputs)

	// Parse file inputs to extract paths and aliases (e.g., "file.csv:alias")
	fileInputs := ParseFileInputs(params.FileInputs)
	aliases := GetAliasMap(fileInputs)
	params.FileInputs = GetPaths(fileInputs)
	verboseLog(params.Verbose, "Parsed aliases: %v", aliases)

	// Create stdin handler to resolve any stdin inputs ("-")
	stdinH := stdinhandler.NewStdinHandler()

	// Check if any file inputs are stdin ("-") and read them to temp files
	verboseLog(params.Verbose, "Checking for stdin input...")
	resolvedFiles, err := stdinH.ResolveFiles(params.FileInputs, params.InputFormat)
	if err != nil {
		_ = stdinH.Cleanup()
		return nil, fmt.Errorf("failed to read stdin: %w", err)
	}
	// Update aliases map with resolved stdin paths
	for i, original := range params.FileInputs {
		if original != resolvedFiles[i] && aliases[original] != "" {
			aliases[resolvedFiles[i]] = aliases[original]
			delete(aliases, original)
		}
	}
	params.FileInputs = resolvedFiles

	// Create URL handler to resolve any HTTP/HTTPS URLs in the file inputs
	urlH := urlhandler.NewURLHandler()

	// Check if any file inputs are HTTP/HTTPS URLs and download them
	verboseLog(params.Verbose, "Resolving HTTP/HTTPS URLs...")
	resolvedFiles, err = urlH.ResolveFiles(params.FileInputs)
	if err != nil {
		_ = stdinH.Cleanup()
		_ = urlH.Cleanup() // Clean up any downloaded files on error
		return nil, fmt.Errorf("failed to resolve file inputs: %w", err)
	}
	params.FileInputs = resolvedFiles

	// Create S3 handler to resolve any S3 URLs
	s3H := s3handler.NewS3Handler()

	// Check if any file inputs are S3 URLs and download them
	verboseLog(params.Verbose, "Resolving S3 URLs...")
	resolvedFiles, err = s3H.ResolveFiles(params.FileInputs)
	if err != nil {
		_ = stdinH.Cleanup()
		_ = urlH.Cleanup()
		_ = s3H.Cleanup()
		return nil, fmt.Errorf("failed to resolve S3 inputs: %w", err)
	}
	params.FileInputs = resolvedFiles

	// Create GCS handler to resolve any GCS URLs
	gcsH := gcshandler.NewGCSHandler()

	// Check if any file inputs are GCS URLs and download them
	verboseLog(params.Verbose, "Resolving GCS URLs...")
	resolvedFiles, err = gcsH.ResolveFiles(params.FileInputs)
	if err != nil {
		_ = stdinH.Cleanup()
		_ = urlH.Cleanup()
		_ = s3H.Cleanup()
		_ = gcsH.Cleanup()
		return nil, fmt.Errorf("failed to resolve GCS inputs: %w", err)
	}
	params.FileInputs = resolvedFiles

	// Create Azure handler to resolve any Azure Blob URLs
	azureH := azurehandler.NewAzureHandler()

	// Check if any file inputs are Azure URLs and download them
	verboseLog(params.Verbose, "Resolving Azure Blob URLs...")
	resolvedFiles, err = azureH.ResolveFiles(params.FileInputs)
	if err != nil {
		_ = stdinH.Cleanup()
		_ = urlH.Cleanup()
		_ = s3H.Cleanup()
		_ = gcsH.Cleanup()
		_ = azureH.Cleanup()
		return nil, fmt.Errorf("failed to resolve Azure inputs: %w", err)
	}
	params.FileInputs = resolvedFiles
	verboseLog(params.Verbose, "Resolved file inputs: %v", params.FileInputs)

	// Create compression handler to decompress any compressed files
	compressionH := compressionhandler.NewCompressionHandler()

	// Check if any file inputs are compressed and decompress them
	verboseLog(params.Verbose, "Checking for compressed files...")
	// Save original paths before resolving (for alias mapping)
	originalFilesBeforeDecompress := make([]string, len(params.FileInputs))
	copy(originalFilesBeforeDecompress, params.FileInputs)
	resolvedFiles, err = compressionH.ResolveFiles(params.FileInputs)
	if err != nil {
		_ = stdinH.Cleanup()
		_ = urlH.Cleanup()
		_ = s3H.Cleanup()
		_ = gcsH.Cleanup()
		_ = azureH.Cleanup()
		_ = compressionH.Cleanup()
		return nil, fmt.Errorf("failed to decompress files: %w", err)
	}
	// Update aliases map with resolved compressed paths
	// The table name should be derived from the original file name (without compression extension)
	for i, original := range originalFilesBeforeDecompress {
		if original != resolvedFiles[i] {
			// File was decompressed - use original path (minus compression extension) as alias
			uncompressedOriginal := compressionhandler.GetUncompressedPath(original)
			if aliases[original] != "" {
				// User specified an explicit alias - transfer it to the decompressed path
				aliases[resolvedFiles[i]] = aliases[original]
				delete(aliases, original)
				verboseLog(params.Verbose, "Compressed file %s -> decompressed %s (explicit alias: %s)", original, resolvedFiles[i], aliases[resolvedFiles[i]])
			} else if params.Collection == "" {
				// No explicit alias and no collection specified - derive table name from original filename
				// e.g., "/tmp/data.csv.gz" -> "data" (will be used by formatTableName as the alias)
				// Skip if collection is specified, as collection has priority over auto-derived aliases
				baseNameWithExt := filepath.Base(uncompressedOriginal)                          // "data.csv"
				tableName := strings.TrimSuffix(baseNameWithExt, filepath.Ext(baseNameWithExt)) // "data"
				aliases[resolvedFiles[i]] = tableName
				verboseLog(params.Verbose, "Compressed file %s -> decompressed %s (auto alias: %s)", original, resolvedFiles[i], aliases[resolvedFiles[i]])
			} else {
				verboseLog(params.Verbose, "Compressed file %s -> decompressed %s (using collection: %s)", original, resolvedFiles[i], params.Collection)
			}
		}
	}
	params.FileInputs = resolvedFiles
	verboseLog(params.Verbose, "Decompressed file inputs: %v", params.FileInputs)

	verboseLog(params.Verbose, "Initializing DuckDB storage...")
	duckDBStorage, err := duckdb.NewDuckDBStorage(params.DataSourceName)
	if err != nil {
		_ = stdinH.Cleanup()
		_ = urlH.Cleanup()
		_ = s3H.Cleanup()
		_ = gcsH.Cleanup()
		_ = azureH.Cleanup()
		_ = compressionH.Cleanup()
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Use stderr for progress bar to keep stdout clean for pipelines
	// Use io.Discard if quiet mode is enabled
	var barWriter io.Writer = os.Stderr
	if params.Quiet {
		barWriter = io.Discard
	}

	bar := progressbar.NewOptions(0,
		progressbar.OptionSetWriter(barWriter),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetDescription("[cyan][1/1][reset] loading data..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	verboseLog(params.Verbose, "Creating file handler...")
	handler, err := createFileHandler(params, bar, duckDBStorage, aliases)
	if err != nil {
		_ = stdinH.Cleanup()
		_ = urlH.Cleanup()
		_ = s3H.Cleanup()
		_ = gcsH.Cleanup()
		_ = azureH.Cleanup()
		_ = compressionH.Cleanup()
		return nil, fmt.Errorf("failed to create file handler: %w", err)
	}

	verboseLog(params.Verbose, "DataQL initialization complete")
	return &dataQL{
		params:             params,
		bar:                bar,
		fileHandler:        handler,
		storage:            duckDBStorage,
		urlHandler:         urlH,
		s3Handler:          s3H,
		gcsHandler:         gcsH,
		azureHandler:       azureH,
		stdinHandler:       stdinH,
		compressionHandler: compressionH,
		pageSize:           defaultPageSize,
		truncate:           params.Truncate,
		vertical:           params.Vertical,
	}, nil
}

// NewStorageOnly creates a DataQL instance that only uses an existing DuckDB storage file
// This mode allows querying previously saved data without specifying input files
func NewStorageOnly(params Params) (DataQL, error) {
	verboseLog(params.Verbose, "Starting DataQL initialization in storage-only mode...")

	// Verify the DuckDB file exists
	if _, err := os.Stat(params.DataSourceName); os.IsNotExist(err) {
		return nil, fmt.Errorf("storage file does not exist: %s (use --file to create a new database)", params.DataSourceName)
	}

	verboseLog(params.Verbose, "Opening existing DuckDB storage: %s", params.DataSourceName)
	duckDBStorage, err := duckdb.NewDuckDBStorage(params.DataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Use stderr for progress bar to keep stdout clean for pipelines
	// Use io.Discard if quiet mode is enabled
	var barWriter io.Writer = os.Stderr
	if params.Quiet {
		barWriter = io.Discard
	}

	bar := progressbar.NewOptions(0,
		progressbar.OptionSetWriter(barWriter),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetDescription("[cyan][storage][reset] querying existing data..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	verboseLog(params.Verbose, "DataQL storage-only initialization complete")
	return &dataQL{
		params:   params,
		bar:      bar,
		storage:  duckDBStorage,
		pageSize: defaultPageSize,
		truncate: params.Truncate,
		vertical: params.Vertical,
	}, nil
}

// createFileHandler creates the appropriate file handler based on file format
func createFileHandler(params Params, bar *progressbar.ProgressBar, storage storage.Storage, aliases map[string]string) (filehandler.FileHandler, error) {
	// Detect format from file extensions
	format, err := filehandler.DetectFormatFromFiles(params.FileInputs)
	if err != nil {
		return nil, err
	}

	switch format {
	case filehandler.FormatCSV:
		delimiter := ','
		if params.Delimiter != "" {
			delimiter = rune(params.Delimiter[0])
		}
		return csvHandler.NewCsvHandlerWithAliases(params.FileInputs, delimiter, bar, storage, params.Lines, params.Collection, aliases), nil

	case filehandler.FormatJSON:
		return jsonHandler.NewJsonHandlerWithAliases(params.FileInputs, bar, storage, params.Lines, params.Collection, aliases), nil

	case filehandler.FormatJSONL:
		return jsonlHandler.NewJsonlHandlerWithAliases(params.FileInputs, bar, storage, params.Lines, params.Collection, aliases), nil

	case filehandler.FormatXML:
		return xmlHandler.NewXmlHandlerWithAliases(params.FileInputs, bar, storage, params.Lines, params.Collection, aliases), nil

	case filehandler.FormatExcel:
		return excelHandler.NewExcelHandlerWithAliases(params.FileInputs, bar, storage, params.Lines, params.Collection, aliases), nil

	case filehandler.FormatParquet:
		return parquetHandler.NewParquetHandlerWithAliases(params.FileInputs, bar, storage, params.Lines, params.Collection, aliases), nil

	case filehandler.FormatYAML:
		return yamlHandler.NewYamlHandlerWithAliases(params.FileInputs, bar, storage, params.Lines, params.Collection, aliases), nil

	case filehandler.FormatAVRO:
		return avroHandler.NewAvroHandlerWithAliases(params.FileInputs, bar, storage, params.Lines, params.Collection, aliases), nil

	case filehandler.FormatORC:
		return orcHandler.NewOrcHandlerWithAliases(params.FileInputs, bar, storage, params.Lines, params.Collection, aliases), nil

	case filehandler.FormatPostgres, filehandler.FormatMySQL, filehandler.FormatDuckDB:
		if len(params.FileInputs) != 1 {
			return nil, fmt.Errorf("database URL must be a single connection string")
		}
		connInfo, err := databaseHandler.ParseDatabaseURL(params.FileInputs[0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse database URL: %w", err)
		}
		if connInfo.Table == "" {
			return nil, fmt.Errorf("database URL must include table name: postgres://user:pass@host:port/database/table")
		}
		return databaseHandler.NewDBHandler(*connInfo, bar, storage, params.Lines, params.Collection), nil

	case filehandler.FormatMongoDB:
		if len(params.FileInputs) != 1 {
			return nil, fmt.Errorf("MongoDB URL must be a single connection string")
		}
		connInfo, err := mongodbHandler.ParseMongoDBURL(params.FileInputs[0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse MongoDB URL: %w", err)
		}
		return mongodbHandler.NewMongoHandler(*connInfo, bar, storage, params.Lines, params.Collection), nil

	case filehandler.FormatDynamoDB:
		if len(params.FileInputs) != 1 {
			return nil, fmt.Errorf("DynamoDB URL must be a single connection string")
		}
		connInfo, err := dynamodbHandler.ParseDynamoDBURL(params.FileInputs[0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse DynamoDB URL: %w", err)
		}
		return dynamodbHandler.NewDynamoDBHandler(*connInfo, bar, storage, params.Lines, params.Collection), nil

	case filehandler.FormatSQLite:
		return sqliteHandler.NewSqliteHandler(params.FileInputs, bar, storage, params.Lines, params.Collection), nil

	case filehandler.FormatMQ:
		if len(params.FileInputs) != 1 {
			return nil, fmt.Errorf("message queue URL must be a single connection string")
		}
		return mqHandler.NewMQHandler(params.FileInputs[0], bar, storage, params.Lines, params.Collection)

	case filehandler.FormatMixed:
		// Mixed formats - use composite handler to process each file with its appropriate handler
		delimiter := ','
		if params.Delimiter != "" {
			delimiter = rune(params.Delimiter[0])
		}
		return compositeHandler.NewCompositeHandlerWithAliases(params.FileInputs, delimiter, bar, storage, params.Lines, params.Collection, aliases)

	default:
		return nil, fmt.Errorf("unsupported file format: %s", format)
	}
}

// Run imports file content and runs the command
func (d *dataQL) Run() error {
	defer func(bar *progressbar.ProgressBar) {
		_ = bar.Clear()
	}(d.bar)

	verboseLog(d.params.Verbose, "Starting data import...")
	if err := d.fileHandler.Import(); err != nil {
		return fmt.Errorf("failed to import data %w", err)
	}
	verboseLog(d.params.Verbose, "Data import complete. Lines imported: %d", d.fileHandler.Lines())
	defer func(fileHandler filehandler.FileHandler) {
		_ = fileHandler.Close()
	}(d.fileHandler)

	// Show table schema unless --no-schema is set or a query is specified (non-REPL mode)
	// Schema is useful in REPL mode but adds noise when running one-off queries
	if !d.params.NoSchema && d.params.Query == "" {
		verboseLog(d.params.Verbose, "Listing available tables...")
		rows, err := d.storage.ShowTables()
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}

		if _, err := d.printResult(rows); err != nil {
			return fmt.Errorf("failed to print tables: %w", err)
		}
	}

	return d.execute()
}

// RunStorageOnly executes queries on an existing DuckDB storage file without importing new data
func (d *dataQL) RunStorageOnly() error {
	defer func(bar *progressbar.ProgressBar) {
		_ = bar.Clear()
	}(d.bar)

	verboseLog(d.params.Verbose, "Running in storage-only mode...")

	// Show table schema unless --no-schema is set or a query is specified (non-REPL mode)
	// Schema is useful in REPL mode but adds noise when running one-off queries
	if !d.params.NoSchema && d.params.Query == "" {
		verboseLog(d.params.Verbose, "Listing available tables in storage...")
		rows, err := d.storage.ShowTables()
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}

		if _, err := d.printResult(rows); err != nil {
			return fmt.Errorf("failed to print tables: %w", err)
		}
	}

	return d.execute()
}

// execute runs the execution after data import
func (d *dataQL) execute() error {
	switch {
	case d.params.Query != "" && d.params.Export == "":
		return d.executeQuery(d.params.Query)
	case d.params.Query != "" && d.params.Export != "":
		return d.executeQueryAndExport(d.params.Query)
	default:
		if err := d.initializePrompt(); err != nil {
			return err
		}
	}

	return nil
}

// Close cleans up resources
func (d *dataQL) Close() error {
	// Close file handler if present (not present in storage-only mode)
	if d.fileHandler != nil {
		_ = d.fileHandler.Close()
	}

	// Clean up any temp files from stdin
	if d.stdinHandler != nil {
		_ = d.stdinHandler.Cleanup()
	}

	// Clean up any downloaded temp files from HTTP/HTTPS URLs
	if d.urlHandler != nil {
		_ = d.urlHandler.Cleanup()
	}

	// Clean up any downloaded temp files from S3
	if d.s3Handler != nil {
		_ = d.s3Handler.Cleanup()
	}

	// Clean up any downloaded temp files from GCS
	if d.gcsHandler != nil {
		_ = d.gcsHandler.Cleanup()
	}

	// Clean up any downloaded temp files from Azure
	if d.azureHandler != nil {
		_ = d.azureHandler.Cleanup()
	}

	// Clean up any decompressed temp files
	if d.compressionHandler != nil {
		_ = d.compressionHandler.Cleanup()
	}

	return nil
}

// getHistoryFilePath returns the path to the history file
func getHistoryFilePath() string {
	// Try to get user's home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		// Fallback to temp directory if home is not available
		return filepath.Join(os.TempDir(), ".dataql_history")
	}

	// Create .dataql directory if it doesn't exist
	dataqlDir := filepath.Join(homeDir, ".dataql")
	if err := os.MkdirAll(dataqlDir, 0755); err != nil {
		// Fallback to home directory directly
		return filepath.Join(homeDir, ".dataql_history")
	}

	return filepath.Join(dataqlDir, "history")
}

func (d *dataQL) initializePrompt() error {
	// Create SQL completer with autocomplete support
	completer := repl.NewSQLCompleter(d.storage)
	if err := completer.RefreshSchema(); err != nil {
		// Non-fatal: continue without autocomplete if schema refresh fails
		fmt.Fprintf(os.Stderr, "Warning: autocomplete disabled (%v)\n", err)
	}

	// Create colored prompt
	promptColor := color.New(color.FgCyan, color.Bold)
	cliPrompt := promptColor.Sprint("dataql> ")

	// Get history file path for persistent history
	historyFile := getHistoryFilePath()

	l, err := readline.NewEx(&readline.Config{
		Prompt:            cliPrompt,
		InterruptPrompt:   cliInterruptPrompt,
		EOFPrompt:         cliEOFPrompt,
		AutoComplete:      completer,
		HistoryFile:       historyFile,
		HistorySearchFold: true,
		HistoryLimit:      1000,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize cli: %w", err)
	}

	defer func(l *readline.Instance) {
		_ = l.Close()
	}(l)

	l.CaptureExitSignal()

	for {
		line, err := l.Readline()
		if errors.Is(err, readline.ErrInterrupt) {
			if len(line) == 0 {
				break
			}

			continue
		}

		if errors.Is(err, io.EOF) {
			break
		}

		line = strings.TrimSpace(line)
		if err := d.executeQuery(line); err != nil {
			if errors.Is(err, io.EOF) {
				break // Exit REPL when \q or .quit is used
			}
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		}
	}

	return nil
}

// executeQueryAndExport executes query and exports results
func (d *dataQL) executeQueryAndExport(line string) error {
	d.bar.Reset()
	d.bar.ChangeMax(d.fileHandler.Lines())
	defer func(bar *progressbar.ProgressBar) {
		_ = bar.Finish()
	}(d.bar)

	rows, err := d.storage.Query(line)
	if err != nil {
		// Enhance error with user-friendly hints
		enhancedErr := queryerror.EnhanceError(err)
		return fmt.Errorf("failed to execute query: %w", enhancedErr)
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	export, err := exportdata.NewExport(d.params.Type, rows, d.params.Export, d.bar)
	if err != nil {
		return fmt.Errorf("failed to export: %w", err)
	}

	if err := export.Export(); err != nil {
		return fmt.Errorf("failed to export data: %w", err)
	}

	_ = d.bar.Clear()

	fmt.Printf("[%s] file successfully exported\n", d.params.Export)

	return nil
}

// handleREPLCommand handles special REPL commands (aliases)
// Returns true if the line was a REPL command, false if it should be executed as SQL
func (d *dataQL) handleREPLCommand(line string) (bool, error) {
	// Normalize command (trim and lowercase for comparison)
	cmd := strings.ToLower(strings.TrimSpace(line))
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return true, nil // Empty line, no action needed
	}

	switch parts[0] {
	case "\\q", ".quit", ".exit":
		return true, io.EOF // Signal to exit REPL

	case "\\h", ".help", "\\?":
		d.printHelp()
		return true, nil

	case "\\d", ".tables":
		rows, err := d.storage.ShowTables()
		if err != nil {
			return true, fmt.Errorf("failed to list tables: %w", err)
		}
		if _, err := d.printResult(rows); err != nil {
			return true, err
		}
		return true, nil

	case "\\dt", ".schema":
		if len(parts) < 2 {
			return true, fmt.Errorf("usage: \\dt <table_name> or .schema <table_name>")
		}
		tableName := parts[1]
		return true, d.describeTable(tableName)

	case ".clear":
		fmt.Print("\033[H\033[2J")
		return true, nil

	case ".version":
		fmt.Printf("dataql version %s\n", Version)
		return true, nil

	case ".pagesize":
		if len(parts) < 2 {
			fmt.Printf("Current page size: %d\n", d.pageSize)
			return true, nil
		}
		size, err := strconv.Atoi(parts[1])
		if err != nil || size < 1 {
			return true, fmt.Errorf("invalid page size: %s (must be a positive integer)", parts[1])
		}
		d.pageSize = size
		fmt.Printf("Page size set to %d\n", size)
		return true, nil

	case ".paging":
		if len(parts) < 2 {
			status := "off"
			if d.paging {
				status = "on"
			}
			fmt.Printf("Paging is %s (page size: %d)\n", status, d.pageSize)
			return true, nil
		}
		switch strings.ToLower(parts[1]) {
		case "on", "true", "1":
			d.paging = true
			fmt.Println("Paging enabled")
		case "off", "false", "0":
			d.paging = false
			fmt.Println("Paging disabled")
		default:
			return true, fmt.Errorf("invalid paging value: %s (use on/off)", parts[1])
		}
		return true, nil

	case ".timing":
		if len(parts) < 2 {
			status := "off"
			if d.showTiming {
				status = "on"
			}
			fmt.Printf("Timing is %s\n", status)
			return true, nil
		}
		switch strings.ToLower(parts[1]) {
		case "on", "true", "1":
			d.showTiming = true
			fmt.Println("Timing enabled")
		case "off", "false", "0":
			d.showTiming = false
			fmt.Println("Timing disabled")
		default:
			return true, fmt.Errorf("invalid timing value: %s (use on/off)", parts[1])
		}
		return true, nil

	case "\\c", ".count":
		if len(parts) < 2 {
			return true, fmt.Errorf("usage: \\c <table_name> or .count <table_name>")
		}
		tableName := parts[1]
		return true, d.countTable(tableName)

	case ".truncate":
		if len(parts) < 2 {
			if d.truncate > 0 {
				fmt.Printf("Truncation is enabled at %d characters\n", d.truncate)
			} else {
				fmt.Println("Truncation is disabled (0)")
			}
			return true, nil
		}
		size, err := strconv.Atoi(parts[1])
		if err != nil || size < 0 {
			return true, fmt.Errorf("invalid truncate value: %s (must be a non-negative integer, 0 to disable)", parts[1])
		}
		d.truncate = size
		if size > 0 {
			fmt.Printf("Truncation set to %d characters\n", size)
		} else {
			fmt.Println("Truncation disabled")
		}
		return true, nil

	case ".vertical", "\\g":
		if len(parts) < 2 {
			status := "off"
			if d.vertical {
				status = "on"
			}
			fmt.Printf("Vertical display is %s\n", status)
			return true, nil
		}
		switch strings.ToLower(parts[1]) {
		case "on", "true", "1":
			d.vertical = true
			fmt.Println("Vertical display enabled")
		case "off", "false", "0":
			d.vertical = false
			fmt.Println("Vertical display disabled")
		default:
			return true, fmt.Errorf("invalid vertical value: %s (use on/off)", parts[1])
		}
		return true, nil
	}

	return false, nil // Not a REPL command, should be executed as SQL
}

// printHelp prints the REPL help message
func (d *dataQL) printHelp() {
	helpText := `
DataQL REPL Commands:
  \d, .tables          List all tables
  \dt <table>, .schema <table>  Show table schema
  \c <table>, .count <table>    Count rows in table
  \q, .quit, .exit     Exit the REPL
  \h, .help, \?        Show this help message
  .clear               Clear the screen
  .version             Show version
  .paging [on|off]     Enable/disable result pagination
  .pagesize [n]        Set/show page size (default: 25)
  .timing [on|off]     Enable/disable query timing display
  .truncate [n]        Truncate columns at n chars (0 to disable)
  .vertical [on|off], \G  Toggle vertical display (like MySQL \G)

SQL Examples:
  SELECT * FROM <table>
  SELECT * FROM <table> WHERE <column> = '<value>'
  SELECT * FROM <table> ORDER BY <column> DESC LIMIT 10
`
	fmt.Println(helpText)
}

// describeTable shows the schema of a table
func (d *dataQL) describeTable(tableName string) error {
	// Use DuckDB's information_schema to get column information
	query := fmt.Sprintf(`SELECT
		column_name AS name,
		data_type AS type,
		CASE WHEN is_nullable = 'YES' THEN 0 ELSE 1 END AS notnull,
		column_default AS dflt_value
		FROM information_schema.columns
		WHERE table_schema = 'main' AND table_name = '%s'
		ORDER BY ordinal_position`, tableName)
	rows, err := d.storage.Query(query)
	if err != nil {
		return fmt.Errorf("failed to describe table: %w", err)
	}
	defer rows.Close()

	_, err = d.printResult(rows)
	return err
}

// countTable shows the row count for a table
func (d *dataQL) countTable(tableName string) error {
	query := fmt.Sprintf("SELECT COUNT(*) as count FROM %s", tableName)
	rows, err := d.storage.Query(query)
	if err != nil {
		return fmt.Errorf("failed to count table: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int64
		if err := rows.Scan(&count); err != nil {
			return fmt.Errorf("failed to read count: %w", err)
		}
		fmt.Printf("%s: %d rows\n", tableName, count)
	}
	return nil
}

func (d *dataQL) executeQuery(line string) error {
	// Check for REPL commands first
	if handled, err := d.handleREPLCommand(line); handled {
		return err
	}

	startTime := time.Now()

	rows, err := d.storage.Query(line)
	if err != nil {
		// Enhance error with user-friendly hints
		enhancedErr := queryerror.EnhanceError(err)
		return fmt.Errorf("failed to execute query: %w", enhancedErr)
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	rowCount, err := d.printResult(rows)
	if err != nil {
		return err
	}

	elapsed := time.Since(startTime)
	if d.showTiming {
		fmt.Printf("(%d rows in %v)\n", rowCount, elapsed.Round(time.Millisecond))
	} else {
		fmt.Printf("(%d rows)\n", rowCount)
	}

	return nil
}

func (d *dataQL) printResult(rows *sql.Rows) (int, error) {
	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("failed to load columns: %w", err)
	}

	cols := make([]interface{}, 0)
	for _, c := range columns {
		cols = append(cols, c)
	}

	_ = d.bar.Clear()

	// Vertical display mode (like MySQL \G)
	if d.vertical {
		return d.printVerticalRows(rows, columns)
	}

	// If paging is disabled, print all results at once
	if !d.paging {
		return d.printAllRows(rows, columns, cols)
	}

	// Paging enabled: print page by page
	return d.printPaginatedRows(rows, columns, cols)
}

// truncateValue truncates a value to the specified length if truncation is enabled
func (d *dataQL) truncateValue(value interface{}) interface{} {
	if d.truncate <= 0 {
		return value
	}

	str := fmt.Sprintf("%v", value)
	if len(str) > d.truncate {
		return str[:d.truncate-3] + "..."
	}
	return value
}

// truncateValues applies truncation to all values in a slice
func (d *dataQL) truncateValues(values []interface{}) []interface{} {
	if d.truncate <= 0 {
		return values
	}

	result := make([]interface{}, len(values))
	for i, v := range values {
		result[i] = d.truncateValue(v)
	}
	return result
}

// printVerticalRows prints rows in vertical format (like MySQL \G)
func (d *dataQL) printVerticalRows(rows *sql.Rows, columns []string) (int, error) {
	// Find the longest column name for alignment
	maxColLen := 0
	for _, col := range columns {
		if len(col) > maxColLen {
			maxColLen = len(col)
		}
	}

	rowCount := 0
	colColor := color.New(color.FgCyan)
	valColor := color.New(color.FgWhite)
	headerColor := color.New(color.FgGreen, color.Bold)

	for rows.Next() {
		rowCount++

		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return rowCount, fmt.Errorf("failed to read row: %w", err)
		}

		// Print row separator
		headerColor.Printf("*************************** %d. row ***************************\n", rowCount)

		// Print each column as key-value pair
		for i, col := range columns {
			val := d.truncateValue(values[i])
			colColor.Printf("%*s: ", maxColLen, col)
			valColor.Printf("%v\n", val)
		}
	}

	return rowCount, nil
}

// printAllRows prints all rows without pagination
func (d *dataQL) printAllRows(rows *sql.Rows, columns []string, cols []interface{}) (int, error) {
	tbl := table.New(cols...).
		WithHeaderFormatter(color.New(color.FgGreen, color.Underline).SprintfFunc()).
		WithFirstColumnFormatter(color.New(color.FgYellow).SprintfFunc()).
		WithWriter(os.Stdout)

	rowCount := 0
	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return rowCount, fmt.Errorf("failed to read row: %w", err)
		}

		// Apply truncation if enabled
		tbl.AddRow(d.truncateValues(values)...)
		rowCount++
	}

	tbl.Print()
	return rowCount, nil
}

// printPaginatedRows prints rows with pagination
func (d *dataQL) printPaginatedRows(rows *sql.Rows, columns []string, cols []interface{}) (int, error) {
	reader := bufio.NewReader(os.Stdin)
	rowCount := 0
	pageNum := 1

	// pendingRow holds the next row if we peeked ahead
	var pendingRow []interface{}

	for {
		// Create a new table for this page
		tbl := table.New(cols...).
			WithHeaderFormatter(color.New(color.FgGreen, color.Underline).SprintfFunc()).
			WithFirstColumnFormatter(color.New(color.FgYellow).SprintfFunc()).
			WithWriter(os.Stdout)

		// Collect rows for this page
		pageRows := 0

		// First, add the pending row if we have one
		if pendingRow != nil {
			tbl.AddRow(d.truncateValues(pendingRow)...)
			rowCount++
			pageRows++
		}

		// Read more rows for this page
		for pageRows < d.pageSize && rows.Next() {
			values := make([]interface{}, len(columns))
			pointers := make([]interface{}, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}

			if err := rows.Scan(pointers...); err != nil {
				return rowCount, fmt.Errorf("failed to read row: %w", err)
			}

			// Apply truncation if enabled
			tbl.AddRow(d.truncateValues(values)...)
			rowCount++
			pageRows++
		}

		// Print this page if we have any rows
		if pageRows > 0 {
			tbl.Print()
		}

		// Check if we've read fewer rows than page size (no more rows)
		if pageRows < d.pageSize {
			return rowCount, nil
		}

		// Peek ahead to see if there are more rows
		if rows.Next() {
			// Save this row for the next page
			values := make([]interface{}, len(columns))
			pointers := make([]interface{}, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				return rowCount, fmt.Errorf("failed to read row: %w", err)
			}
			pendingRow = values

			// Prompt user for next page
			fmt.Printf("\n-- Page %d (%d rows shown) -- Press Enter for more, q to quit --\n", pageNum, rowCount)
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(strings.ToLower(input))
			if input == "q" || input == "quit" {
				return rowCount, nil
			}
			pageNum++
		} else {
			// No more rows
			return rowCount, nil
		}
	}
}
