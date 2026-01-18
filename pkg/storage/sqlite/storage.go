package sqlite

import (
	"database/sql"
	"fmt"
	"github.com/adrianolaselva/dataql/pkg/storage"
	_ "github.com/mattn/go-sqlite3"
	"strings"
)

const (
	sqlCreateTableTemplate        = "CREATE TABLE IF NOT EXISTS %s (%s\n);"
	sqlInsertTemplate             = "INSERT INTO %s (%s) VALUES (%s);"
	sqlInsertDefaultTableTemplate = "INSERT INTO `schemas` (`id`, `name`, `columns`, `total_columns`) VALUES ((select count(1)+1 FROM `schemas`),?,?,?);"
	sqlShowTablesTemplate         = "select * from `schemas`;"
	sqlDefaultTableTemplate       = "CREATE TABLE IF NOT EXISTS `schemas` (`id` INTEGER, `name` text, `columns` text, `total_columns` INTEGER);"
	dataSourceNameDefault         = ":memory:"
)

type sqLiteStorage struct {
	db *sql.DB
}

func NewSqLiteStorage(datasource string) (storage.Storage, error) {
	if datasource == "" {
		datasource = dataSourceNameDefault
	}

	db, err := sql.Open("sqlite3", datasource)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection with sqlite3: %w", err)
	}

	return &sqLiteStorage{db: db}, nil
}

// BuildStructure build table creation statement
func (s *sqLiteStorage) BuildStructure(tableName string, columns []string) error {
	var tableAttrsRaw strings.Builder

	// Create quoted column names for SQL but don't modify the original slice
	quotedColumns := make([]string, len(columns))
	for i, v := range columns {
		quotedColumns[i] = fmt.Sprintf("`%v`", v)
	}

	for i, v := range quotedColumns {
		tableAttrsRaw.WriteString(fmt.Sprintf("\n\t%s text", v))
		if len(quotedColumns)-1 > i {
			tableAttrsRaw.WriteString(",")
		}
	}

	query := fmt.Sprintf(sqlCreateTableTemplate, tableName, tableAttrsRaw.String())
	if _, err := s.db.Exec(query); err != nil {
		return fmt.Errorf("failed to create structure: %w (sql: %s)", err, query)
	}

	if _, err := s.db.Exec(sqlDefaultTableTemplate); err != nil {
		return fmt.Errorf("failed to create tables schemas structure: %w", err)
	}

	columnsRaw := fmt.Sprintf("[%v]", strings.Join(quotedColumns, ","))
	if _, err := s.db.Exec(sqlInsertDefaultTableTemplate, []any{tableName, columnsRaw, len(columns)}...); err != nil {
		return fmt.Errorf("failed to execute insert: %w", err)
	}

	return nil
}

// InsertRow build insert create statement
func (s *sqLiteStorage) InsertRow(tableName string, columns []string, values []any) error {
	// Quote column names for SQL
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = fmt.Sprintf("`%s`", col)
	}
	columnsRaw := strings.Join(quotedColumns, ", ")
	paramsRaw := strings.Repeat("?, ", len(columns))
	query := fmt.Sprintf(sqlInsertTemplate, tableName, columnsRaw, paramsRaw[:len(paramsRaw)-2])

	if _, err := s.db.Exec(query, values...); err != nil {
		return fmt.Errorf("failed to execute insert: %w (sql: %s)", err, query)
	}

	return nil
}

// Query execute statements
func (s *sqLiteStorage) Query(cmd string) (*sql.Rows, error) {
	rows, err := s.db.Query(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

func (s *sqLiteStorage) ShowTables() (*sql.Rows, error) {
	rows, err := s.db.Query(sqlShowTablesTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

// Close execute in defer
func (s *sqLiteStorage) Close() error {
	err := s.db.Close()
	if err != nil {
		return fmt.Errorf("failed to close sqlite3 connection: %w", err)
	}

	return nil
}
