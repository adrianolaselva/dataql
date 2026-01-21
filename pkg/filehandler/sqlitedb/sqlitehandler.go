package sqlitedb

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/schollz/progressbar/v3"

	"github.com/adrianolaselva/dataql/pkg/storage"
)

// SqliteHandler handles SQLite database files
type SqliteHandler struct {
	filePaths  []string
	bar        *progressbar.ProgressBar
	storage    storage.Storage
	limitLines int
	collection string
	lines      int
}

// NewSqliteHandler creates a new SQLite file handler
func NewSqliteHandler(filePaths []string, bar *progressbar.ProgressBar, storage storage.Storage, limitLines int, collection string) *SqliteHandler {
	return &SqliteHandler{
		filePaths:  filePaths,
		bar:        bar,
		storage:    storage,
		limitLines: limitLines,
		collection: collection,
	}
}

// Import imports data from SQLite database file(s) into the storage
func (h *SqliteHandler) Import() error {
	for _, filePath := range h.filePaths {
		if err := h.importFile(filePath); err != nil {
			return err
		}
	}
	return nil
}

// importFile imports a single SQLite database file
func (h *SqliteHandler) importFile(filePath string) error {
	// Open the source SQLite database
	sourceDB, err := sql.Open("sqlite3", filePath)
	if err != nil {
		return fmt.Errorf("failed to open SQLite database %s: %w", filePath, err)
	}
	defer sourceDB.Close()

	// Get list of tables from the source database
	tables, err := h.getTables(sourceDB)
	if err != nil {
		return fmt.Errorf("failed to get tables from %s: %w", filePath, err)
	}

	// Import each table
	for _, tableName := range tables {
		// Skip SQLite internal tables
		if strings.HasPrefix(tableName, "sqlite_") {
			continue
		}

		targetTable := tableName
		if h.collection != "" && len(tables) == 1 {
			targetTable = h.collection
		}

		if err := h.importTable(sourceDB, tableName, targetTable); err != nil {
			return fmt.Errorf("failed to import table %s: %w", tableName, err)
		}
	}

	return nil
}

// getTables returns the list of tables in the SQLite database
func (h *SqliteHandler) getTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// importTable imports a single table from source to target storage
func (h *SqliteHandler) importTable(sourceDB *sql.DB, sourceTable, targetTable string) error {
	// Get column info with types
	columnDefs, columns, err := h.getTableColumnsWithTypes(sourceDB, sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get columns for table %s: %w", sourceTable, err)
	}

	// Build structure in target storage with types if supported
	if typedStorage, ok := h.storage.(storage.TypedStorage); ok {
		if err := typedStorage.BuildStructureWithTypes(targetTable, columnDefs); err != nil {
			return fmt.Errorf("failed to build structure with types for table %s: %w", targetTable, err)
		}
	} else {
		if err := h.storage.BuildStructure(targetTable, columns); err != nil {
			return fmt.Errorf("failed to build structure for table %s: %w", targetTable, err)
		}
	}

	// Query all data from source table
	query := fmt.Sprintf("SELECT * FROM `%s`", sourceTable)
	if h.limitLines > 0 {
		query += fmt.Sprintf(" LIMIT %d", h.limitLines)
	}

	rows, err := sourceDB.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query table %s: %w", sourceTable, err)
	}
	defer rows.Close()

	// Import rows
	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert values to strings for storage
		stringValues := make([]interface{}, len(values))
		for i, v := range values {
			if v == nil {
				stringValues[i] = nil
			} else {
				stringValues[i] = fmt.Sprintf("%v", v)
			}
		}

		if err := h.storage.InsertRow(targetTable, columns, stringValues); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}

		h.lines++
		_ = h.bar.Add(1)

		if h.limitLines > 0 && h.lines >= h.limitLines {
			break
		}
	}

	return rows.Err()
}

// getTableColumnsWithTypes returns the column definitions with types for a table
func (h *SqliteHandler) getTableColumnsWithTypes(db *sql.DB, tableName string) ([]storage.ColumnDef, []string, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(`%s`)", tableName))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var columnDefs []storage.ColumnDef
	var columns []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notNull, pk int
		var dfltValue interface{}
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dfltValue, &pk); err != nil {
			return nil, nil, err
		}
		columns = append(columns, name)
		columnDefs = append(columnDefs, storage.ColumnDef{
			Name: name,
			Type: mapSQLiteType(ctype),
		})
	}
	return columnDefs, columns, rows.Err()
}

// mapSQLiteType maps SQLite type names to our DataType
func mapSQLiteType(sqliteType string) storage.DataType {
	// SQLite types are case-insensitive, so normalize to uppercase
	upperType := strings.ToUpper(sqliteType)

	// SQLite type affinity rules
	switch {
	case strings.Contains(upperType, "INT"):
		return storage.TypeBigInt
	case strings.Contains(upperType, "REAL"), strings.Contains(upperType, "FLOA"), strings.Contains(upperType, "DOUBLE"):
		return storage.TypeDouble
	case strings.Contains(upperType, "BOOL"):
		return storage.TypeBoolean
	default:
		return storage.TypeVarchar
	}
}

// Lines returns the number of lines imported
func (h *SqliteHandler) Lines() int {
	return h.lines
}

// Close closes the handler
func (h *SqliteHandler) Close() error {
	return nil
}

// GetTableName returns the table name from the file path
func GetTableName(filePath string) string {
	base := filepath.Base(filePath)
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext)
}
