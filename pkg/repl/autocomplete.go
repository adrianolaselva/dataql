package repl

import (
	"strings"

	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/chzyer/readline"
)

// SQL keywords for autocomplete
var sqlKeywords = []string{
	"SELECT", "FROM", "WHERE", "AND", "OR", "NOT",
	"ORDER", "BY", "ASC", "DESC", "LIMIT", "OFFSET",
	"GROUP", "HAVING", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
	"ON", "AS", "IN", "LIKE", "BETWEEN", "IS", "NULL",
	"COUNT", "SUM", "AVG", "MIN", "MAX", "DISTINCT",
	"INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
	"CREATE", "TABLE", "DROP", "ALTER", "INDEX",
	"UNION", "ALL", "EXCEPT", "INTERSECT",
	"CASE", "WHEN", "THEN", "ELSE", "END",
	"TRUE", "FALSE",
}

// REPL commands for autocomplete
var replCommands = []string{
	"\\d", "\\dt", "\\c", "\\q", "\\h", "\\?",
	".tables", ".schema", ".count", ".quit", ".exit", ".help", ".clear", ".version",
	".paging", ".pagesize", ".timing",
}

// SQLCompleter provides SQL autocomplete functionality
type SQLCompleter struct {
	storage storage.Storage
	tables  []string
	columns map[string][]string
}

// NewSQLCompleter creates a new SQL completer
func NewSQLCompleter(storage storage.Storage) *SQLCompleter {
	return &SQLCompleter{
		storage: storage,
		columns: make(map[string][]string),
	}
}

// RefreshSchema updates the table and column information from storage
func (c *SQLCompleter) RefreshSchema() error {
	// Get tables from storage
	rows, err := c.storage.ShowTables()
	if err != nil {
		return err
	}
	defer rows.Close()

	c.tables = nil
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		c.tables = append(c.tables, tableName)

		// Get columns for this table
		cols, err := c.getTableColumns(tableName)
		if err != nil {
			continue
		}
		c.columns[tableName] = cols
	}

	return nil
}

// getTableColumns retrieves column names for a table
func (c *SQLCompleter) getTableColumns(tableName string) ([]string, error) {
	// Use PRAGMA table_info for SQLite
	rows, err := c.storage.Query("PRAGMA table_info(" + tableName + ")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notNull, pk int
		var dfltValue interface{}
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dfltValue, &pk); err != nil {
			continue
		}
		columns = append(columns, name)
	}

	return columns, nil
}

// Complete implements the readline.AutoCompleter interface
func (c *SQLCompleter) Do(line []rune, pos int) (newLine [][]rune, length int) {
	lineStr := string(line[:pos])
	words := strings.Fields(lineStr)

	var prefix string
	if len(lineStr) > 0 && lineStr[len(lineStr)-1] != ' ' && len(words) > 0 {
		prefix = words[len(words)-1]
	}

	// Determine context for completion
	context := c.detectContext(words)

	var candidates []string
	switch context {
	case contextKeyword:
		candidates = c.filterByPrefix(sqlKeywords, prefix)
	case contextTable:
		candidates = c.filterByPrefix(c.tables, prefix)
	case contextColumn:
		candidates = c.getAllColumns(prefix)
	case contextTableColumn:
		tableName := c.extractTableName(words)
		if cols, ok := c.columns[tableName]; ok {
			candidates = c.filterByPrefix(cols, prefix)
		}
	default:
		// Default: suggest keywords, tables, and REPL commands
		candidates = append(candidates, c.filterByPrefix(sqlKeywords, prefix)...)
		candidates = append(candidates, c.filterByPrefix(c.tables, prefix)...)
		candidates = append(candidates, c.filterByPrefix(replCommands, prefix)...)
	}

	// Remove duplicates
	candidates = c.unique(candidates)

	// Convert to readline format
	newLine = make([][]rune, len(candidates))
	for i, cand := range candidates {
		suffix := cand
		if len(prefix) > 0 {
			suffix = cand[len(prefix):]
		}
		newLine[i] = []rune(suffix)
	}

	return newLine, len(prefix)
}

type completionContext int

const (
	contextDefault completionContext = iota
	contextKeyword
	contextTable
	contextColumn
	contextTableColumn
)

// detectContext determines what type of completion is needed
func (c *SQLCompleter) detectContext(words []string) completionContext {
	if len(words) == 0 {
		return contextKeyword
	}

	lastWord := strings.ToUpper(words[len(words)-1])

	// After FROM or JOIN, suggest tables
	for i := len(words) - 1; i >= 0; i-- {
		w := strings.ToUpper(words[i])
		if w == "FROM" || w == "JOIN" || w == "INTO" || w == "UPDATE" || w == "TABLE" {
			if i == len(words)-1 {
				return contextTable
			}
			break
		}
	}

	// After SELECT or WHERE, suggest columns
	if lastWord == "SELECT" || lastWord == "WHERE" || lastWord == "AND" || lastWord == "OR" ||
		lastWord == "BY" || lastWord == "ON" || lastWord == "SET" || lastWord == "," {
		return contextColumn
	}

	// After dot, suggest columns for specific table
	if strings.Contains(lastWord, ".") {
		return contextTableColumn
	}

	return contextDefault
}

// extractTableName extracts the table name from the last "table." prefix
func (c *SQLCompleter) extractTableName(words []string) string {
	if len(words) == 0 {
		return ""
	}
	lastWord := words[len(words)-1]
	if idx := strings.LastIndex(lastWord, "."); idx != -1 {
		return strings.ToLower(lastWord[:idx])
	}
	return ""
}

// filterByPrefix filters items by prefix (case-insensitive)
func (c *SQLCompleter) filterByPrefix(items []string, prefix string) []string {
	if prefix == "" {
		return items
	}

	var result []string
	prefixUpper := strings.ToUpper(prefix)
	for _, item := range items {
		if strings.HasPrefix(strings.ToUpper(item), prefixUpper) {
			result = append(result, item)
		}
	}
	return result
}

// getAllColumns returns all columns from all tables filtered by prefix
func (c *SQLCompleter) getAllColumns(prefix string) []string {
	var result []string
	seen := make(map[string]bool)

	for _, cols := range c.columns {
		for _, col := range cols {
			if !seen[col] && (prefix == "" || strings.HasPrefix(strings.ToUpper(col), strings.ToUpper(prefix))) {
				result = append(result, col)
				seen[col] = true
			}
		}
	}

	return result
}

// unique removes duplicates from a slice
func (c *SQLCompleter) unique(items []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, item := range items {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}

// GetCompleter returns a readline completer function
func (c *SQLCompleter) GetCompleter() readline.AutoCompleter {
	return readline.NewPrefixCompleter(c.buildPrefixItems()...)
}

// buildPrefixItems builds the prefix completer items
func (c *SQLCompleter) buildPrefixItems() []readline.PrefixCompleterInterface {
	var items []readline.PrefixCompleterInterface

	// SELECT with column completion
	selectItems := make([]readline.PrefixCompleterInterface, 0)
	for _, col := range c.getAllColumns("") {
		selectItems = append(selectItems, readline.PcItem(col))
	}
	selectItems = append(selectItems, readline.PcItem("*"))
	items = append(items, readline.PcItem("SELECT", selectItems...))

	// FROM with table completion
	fromItems := make([]readline.PrefixCompleterInterface, 0)
	for _, table := range c.tables {
		fromItems = append(fromItems, readline.PcItem(table))
	}
	items = append(items, readline.PcItem("FROM", fromItems...))

	// WHERE
	items = append(items, readline.PcItem("WHERE"))

	// ORDER BY
	items = append(items, readline.PcItem("ORDER",
		readline.PcItem("BY",
			readline.PcItem("ASC"),
			readline.PcItem("DESC"),
		),
	))

	// GROUP BY
	items = append(items, readline.PcItem("GROUP",
		readline.PcItem("BY"),
	))

	// LIMIT
	items = append(items, readline.PcItem("LIMIT"))

	// HAVING
	items = append(items, readline.PcItem("HAVING"))

	// JOIN variants
	for _, joinType := range []string{"JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN"} {
		joinItems := make([]readline.PrefixCompleterInterface, 0)
		for _, table := range c.tables {
			joinItems = append(joinItems, readline.PcItem(table))
		}
		items = append(items, readline.PcItem(joinType, joinItems...))
	}

	return items
}
