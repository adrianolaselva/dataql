package dataql

type Params struct {
	FileInputs     []string
	DataSourceName string
	Delimiter      string
	Query          string
	Export         string
	Type           string
	Lines          int
	Collection     string
	Verbose        bool
	Quiet          bool     // Suppress progress bar output
	NoSchema       bool     // Suppress table schema display before query results
	InputFormat    string   // Input format for stdin (csv, json, jsonl, xml, yaml)
	Truncate       int      // Truncate column values longer than N characters (0 = no truncation)
	Vertical       bool     // Display results in vertical format (like MySQL \G)
	QueryParams    []string // Query parameters in format "name=value"
}

// FileInput represents a file path with an optional table alias
type FileInput struct {
	Path  string // File path
	Alias string // Optional table alias (empty if not specified)
}

// ParseFileInput parses a file input string that may contain an alias
// Format: "path" or "path:alias"
// Examples:
//   - "data.csv" -> FileInput{Path: "data.csv", Alias: ""}
//   - "data.csv:users" -> FileInput{Path: "data.csv", Alias: "users"}
//   - "/path/to/file.csv:my_table" -> FileInput{Path: "/path/to/file.csv", Alias: "my_table"}
func ParseFileInput(input string) FileInput {
	// Handle Windows paths (e.g., C:\path\file.csv)
	// Look for the last colon that is followed by a valid alias (no slashes/backslashes)
	lastColonIdx := -1
	for i := len(input) - 1; i >= 0; i-- {
		if input[i] == ':' {
			// Check if this looks like a path separator (e.g., C: in Windows)
			remaining := input[i+1:]
			if len(remaining) > 0 && remaining[0] != '/' && remaining[0] != '\\' {
				// This might be an alias, check if there's no path separator after
				hasPathSep := false
				for _, c := range remaining {
					if c == '/' || c == '\\' {
						hasPathSep = true
						break
					}
				}
				if !hasPathSep {
					lastColonIdx = i
					break
				}
			}
		}
	}

	if lastColonIdx > 0 && lastColonIdx < len(input)-1 {
		return FileInput{
			Path:  input[:lastColonIdx],
			Alias: input[lastColonIdx+1:],
		}
	}

	return FileInput{Path: input, Alias: ""}
}

// ParseFileInputs parses multiple file input strings
func ParseFileInputs(inputs []string) []FileInput {
	result := make([]FileInput, len(inputs))
	for i, input := range inputs {
		result[i] = ParseFileInput(input)
	}
	return result
}

// GetPaths extracts just the paths from file inputs
func GetPaths(inputs []FileInput) []string {
	paths := make([]string, len(inputs))
	for i, input := range inputs {
		paths[i] = input.Path
	}
	return paths
}

// GetAliasMap creates a map of path -> alias for non-empty aliases
func GetAliasMap(inputs []FileInput) map[string]string {
	aliases := make(map[string]string)
	for _, input := range inputs {
		if input.Alias != "" {
			aliases[input.Path] = input.Alias
		}
	}
	return aliases
}

// ParseQueryParams parses query parameters from "name=value" format
// Returns a map of parameter names to values
func ParseQueryParams(params []string) (map[string]string, error) {
	result := make(map[string]string)
	for _, param := range params {
		idx := indexByte(param, '=')
		if idx == -1 {
			return nil, &ParamError{Param: param, Message: "invalid format, expected name=value"}
		}
		name := param[:idx]
		value := param[idx+1:]
		if name == "" {
			return nil, &ParamError{Param: param, Message: "parameter name cannot be empty"}
		}
		result[name] = value
	}
	return result, nil
}

// indexByte returns the index of the first occurrence of c in s, or -1 if not present
func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// ApplyQueryParams replaces :param placeholders in query with actual values
// Supports both :param and $param syntax
func ApplyQueryParams(query string, params map[string]string) string {
	if len(params) == 0 {
		return query
	}

	result := query
	for name, value := range params {
		// Replace :param syntax
		result = replaceParam(result, ":"+name, value)
		// Replace $param syntax
		result = replaceParam(result, "$"+name, value)
	}
	return result
}

// replaceParam replaces all occurrences of param placeholder with the quoted value
// It handles word boundaries to avoid replacing partial matches
func replaceParam(query, placeholder, value string) string {
	result := ""
	i := 0
	for i < len(query) {
		idx := indexString(query[i:], placeholder)
		if idx == -1 {
			result += query[i:]
			break
		}
		pos := i + idx
		endPos := pos + len(placeholder)

		// Check if this is a word boundary (not part of a larger identifier)
		isWordBoundary := true
		if endPos < len(query) {
			c := query[endPos]
			if isAlphaNumeric(c) || c == '_' {
				isWordBoundary = false
			}
		}

		if isWordBoundary {
			result += query[i:pos]
			// Quote the value appropriately
			result += quoteValue(value)
			i = endPos
		} else {
			result += query[i : pos+1]
			i = pos + 1
		}
	}
	return result
}

// indexString returns the index of the first occurrence of substr in s, or -1 if not present
func indexString(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// isAlphaNumeric checks if a byte is alphanumeric
func isAlphaNumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// quoteValue quotes a parameter value for SQL
// Numbers are left unquoted, strings are quoted with single quotes
func quoteValue(value string) string {
	// Check if it's a number
	if isNumber(value) {
		return value
	}
	// Check for common literals
	lowerVal := toLower(value)
	if lowerVal == "null" || lowerVal == "true" || lowerVal == "false" {
		return value
	}
	// Quote as string, escaping single quotes
	escaped := ""
	for i := 0; i < len(value); i++ {
		if value[i] == '\'' {
			escaped += "''"
		} else {
			escaped += string(value[i])
		}
	}
	return "'" + escaped + "'"
}

// isNumber checks if a string represents a number
func isNumber(s string) bool {
	if len(s) == 0 {
		return false
	}
	start := 0
	if s[0] == '-' || s[0] == '+' {
		start = 1
	}
	if start >= len(s) {
		return false
	}
	hasDigit := false
	hasDot := false
	for i := start; i < len(s); i++ {
		c := s[i]
		if c >= '0' && c <= '9' {
			hasDigit = true
		} else if c == '.' && !hasDot {
			hasDot = true
		} else if (c == 'e' || c == 'E') && hasDigit && i+1 < len(s) {
			// Scientific notation
			i++
			if i < len(s) && (s[i] == '+' || s[i] == '-') {
				i++
			}
			if i >= len(s) || s[i] < '0' || s[i] > '9' {
				return false
			}
			for i++; i < len(s); i++ {
				if s[i] < '0' || s[i] > '9' {
					return false
				}
			}
			return true
		} else {
			return false
		}
	}
	return hasDigit
}

// toLower converts a string to lowercase
func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			result[i] = c + 32
		} else {
			result[i] = c
		}
	}
	return string(result)
}

// ParamError represents an error parsing a query parameter
type ParamError struct {
	Param   string
	Message string
}

func (e *ParamError) Error() string {
	return "invalid parameter '" + e.Param + "': " + e.Message
}
