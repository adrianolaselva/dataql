package repl

import (
	"regexp"
	"strings"

	"github.com/fatih/color"
)

// SQLHighlighter provides SQL syntax highlighting
type SQLHighlighter struct {
	keywordColor  *color.Color
	functionColor *color.Color
	stringColor   *color.Color
	numberColor   *color.Color
	operatorColor *color.Color
	defaultColor  *color.Color
}

// NewSQLHighlighter creates a new SQL highlighter
func NewSQLHighlighter() *SQLHighlighter {
	return &SQLHighlighter{
		keywordColor:  color.New(color.FgBlue, color.Bold),
		functionColor: color.New(color.FgCyan),
		stringColor:   color.New(color.FgGreen),
		numberColor:   color.New(color.FgYellow),
		operatorColor: color.New(color.FgMagenta),
		defaultColor:  color.New(color.Reset),
	}
}

// SQL keywords for highlighting
var highlightKeywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "AND": true, "OR": true, "NOT": true,
	"ORDER": true, "BY": true, "ASC": true, "DESC": true, "LIMIT": true, "OFFSET": true,
	"GROUP": true, "HAVING": true, "JOIN": true, "LEFT": true, "RIGHT": true, "INNER": true, "OUTER": true,
	"ON": true, "AS": true, "IN": true, "LIKE": true, "BETWEEN": true, "IS": true, "NULL": true,
	"DISTINCT": true, "ALL": true, "UNION": true, "EXCEPT": true, "INTERSECT": true,
	"INSERT": true, "INTO": true, "VALUES": true, "UPDATE": true, "SET": true, "DELETE": true,
	"CREATE": true, "TABLE": true, "DROP": true, "ALTER": true, "INDEX": true,
	"CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
	"TRUE": true, "FALSE": true, "CROSS": true, "FULL": true,
}

// SQL functions for highlighting
var highlightFunctions = map[string]bool{
	"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
	"UPPER": true, "LOWER": true, "LENGTH": true, "SUBSTR": true, "SUBSTRING": true,
	"TRIM": true, "LTRIM": true, "RTRIM": true, "REPLACE": true, "CONCAT": true,
	"COALESCE": true, "NULLIF": true, "IFNULL": true, "IIF": true,
	"ABS": true, "ROUND": true, "CEIL": true, "FLOOR": true,
	"DATE": true, "TIME": true, "DATETIME": true, "STRFTIME": true,
	"CAST": true, "TYPEOF": true, "TOTAL": true, "GROUP_CONCAT": true,
}

// SQL operators
var operators = []string{">=", "<=", "!=", "<>", "=", ">", "<", "+", "-", "*", "/", "%"}

// Regular expressions for pattern matching
var (
	stringPattern = regexp.MustCompile(`'[^']*'|"[^"]*"`)
	numberPattern = regexp.MustCompile(`\b\d+(\.\d+)?\b`)
	wordPattern   = regexp.MustCompile(`\b[a-zA-Z_][a-zA-Z0-9_]*\b`)
)

// Highlight applies syntax highlighting to a SQL string
func (h *SQLHighlighter) Highlight(sql string) string {
	// Disable color if not supported
	if color.NoColor {
		return sql
	}

	// Track positions that have been highlighted (for future use)
	_ = make([]byte, len(sql))

	// Build the highlighted string
	var output strings.Builder
	i := 0
	for i < len(sql) {
		// Check if this position is part of a string
		if loc := stringPattern.FindStringIndex(sql[i:]); loc != nil && loc[0] == 0 {
			str := sql[i : i+loc[1]]
			output.WriteString(h.stringColor.Sprint(str))
			i += loc[1]
			continue
		}

		// Check for numbers
		if loc := numberPattern.FindStringIndex(sql[i:]); loc != nil && loc[0] == 0 {
			num := sql[i : i+loc[1]]
			output.WriteString(h.numberColor.Sprint(num))
			i += loc[1]
			continue
		}

		// Check for operators
		foundOp := false
		for _, op := range operators {
			if strings.HasPrefix(sql[i:], op) {
				output.WriteString(h.operatorColor.Sprint(op))
				i += len(op)
				foundOp = true
				break
			}
		}
		if foundOp {
			continue
		}

		// Check for words (keywords, functions, identifiers)
		if loc := wordPattern.FindStringIndex(sql[i:]); loc != nil && loc[0] == 0 {
			word := sql[i : i+loc[1]]
			wordUpper := strings.ToUpper(word)

			if highlightKeywords[wordUpper] {
				output.WriteString(h.keywordColor.Sprint(word))
			} else if highlightFunctions[wordUpper] {
				output.WriteString(h.functionColor.Sprint(word))
			} else {
				output.WriteString(word)
			}
			i += loc[1]
			continue
		}

		// Default: copy character as-is
		output.WriteByte(sql[i])
		i++
	}

	return output.String()
}

// HighlightPrompt returns a highlighted version of the prompt
func (h *SQLHighlighter) HighlightPrompt(prompt string) string {
	promptColor := color.New(color.FgCyan, color.Bold)
	return promptColor.Sprint(prompt)
}
