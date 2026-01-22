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
	Quiet          bool   // Suppress progress bar output
	NoSchema       bool   // Suppress table schema display before query results
	InputFormat    string // Input format for stdin (csv, json, jsonl, xml, yaml)
	Truncate       int    // Truncate column values longer than N characters (0 = no truncation)
	Vertical       bool   // Display results in vertical format (like MySQL \G)
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
