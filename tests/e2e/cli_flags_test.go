package e2e_test

import (
	"testing"
)

// TestCLI_VerboseFlag tests the -v verbose flag
func TestCLI_VerboseFlag_CSV(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	// Verbose mode should show initialization messages
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

func TestCLI_VerboseFlag_JSON(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-v",
		"-q", "SELECT * FROM people")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

func TestCLI_VerboseFlag_ShowsFileInputs(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "File inputs")
}

// TestCLI_CollectionFlag tests the -c collection flag
func TestCLI_CollectionFlag_CSV(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-c", "my_custom_table",
		"-q", "SELECT * FROM my_custom_table")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "my_custom_table")
}

func TestCLI_CollectionFlag_JSON(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-c", "json_data",
		"-q", "SELECT * FROM json_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "json_data")
}

func TestCLI_CollectionFlag_JSONL(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-c", "logs",
		"-q", "SELECT * FROM logs")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "logs")
}

func TestCLI_CollectionFlag_XML(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-c", "xml_records",
		"-q", "SELECT * FROM xml_records")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "xml_records")
}

func TestCLI_CollectionFlag_YAML(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-c", "yaml_data",
		"-q", "SELECT * FROM yaml_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "yaml_data")
}

// TestCLI_LinesFlag tests the -l lines limit flag
func TestCLI_LinesFlag_CSV(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/large.csv"),
		"-l", "2",
		"-q", "SELECT COUNT(*) as count FROM large")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

func TestCLI_LinesFlag_JSON(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-l", "1",
		"-q", "SELECT COUNT(*) as count FROM people")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
}

func TestCLI_LinesFlag_Zero(t *testing.T) {
	// Lines=0 should import all rows
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-l", "0",
		"-q", "SELECT COUNT(*) as count FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3") // simple.csv has 3 data rows
}

// TestCLI_DelimiterFlag tests the -d delimiter flag
func TestCLI_DelimiterFlag_Semicolon(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/semicolon.csv"),
		"-d", ";",
		"-q", "SELECT * FROM semicolon")

	assertNoError(t, err, stderr)
	// Should parse correctly with semicolon delimiter
	assertContains(t, stdout, "rows)")
}

func TestCLI_DelimiterFlag_Tab(t *testing.T) {
	// Create a tab-delimited test inline
	tsvData := "name\tage\tCity\nAlice\t30\tNY\nBob\t25\tLA"
	stdout, stderr, err := runDataQLWithStdin(t, tsvData, "run",
		"-f", "-",
		"-i", "csv",
		"-d", "\t",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
}

func TestCLI_DelimiterFlag_Pipe(t *testing.T) {
	pipeData := "name|age|city\nAlice|30|NY\nBob|25|LA"
	stdout, stderr, err := runDataQLWithStdin(t, pipeData, "run",
		"-f", "-",
		"-i", "csv",
		"-d", "|",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
}

// TestCLI_InputFormatFlag tests the -i input format flag for stdin
func TestCLI_InputFormatFlag_CSV(t *testing.T) {
	csvData := "a,b,c\n1,2,3"
	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
}

func TestCLI_InputFormatFlag_JSON(t *testing.T) {
	jsonData := `[{"a": 1}]`
	stdout, stderr, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
}

func TestCLI_InputFormatFlag_JSONL(t *testing.T) {
	jsonlData := `{"a": 1}
{"a": 2}`
	stdout, stderr, err := runDataQLWithStdin(t, jsonlData, "run",
		"-f", "-",
		"-i", "jsonl",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
	assertContains(t, stdout, "2")
}

func TestCLI_InputFormatFlag_YAML(t *testing.T) {
	yamlData := `- a: 1
- a: 2`
	stdout, stderr, err := runDataQLWithStdin(t, yamlData, "run",
		"-f", "-",
		"-i", "yaml",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
	assertContains(t, stdout, "2")
}

// TestCLI_ExportFlag tests the -e export flag with -t type
func TestCLI_ExportFlag_CSV(t *testing.T) {
	outputFile := tempFile(t, "output.csv")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	content := readFile(t, outputFile)
	assertContains(t, content, "John")
}

func TestCLI_ExportFlag_JSONL(t *testing.T) {
	outputFile := tempFile(t, "output.jsonl")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	content := readFile(t, outputFile)
	assertContains(t, content, "John")
}

func TestCLI_ExportFlag_JSON(t *testing.T) {
	outputFile := tempFile(t, "output.json")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "json")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	content := readFile(t, outputFile)
	assertContains(t, content, "John")
}

func TestCLI_ExportFlag_XML(t *testing.T) {
	outputFile := tempFile(t, "output.xml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "xml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	content := readFile(t, outputFile)
	assertContains(t, content, "John")
}

func TestCLI_ExportFlag_YAML(t *testing.T) {
	outputFile := tempFile(t, "output.yaml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "yaml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	content := readFile(t, outputFile)
	assertContains(t, content, "John")
}

// TestCLI_StorageFlag tests the -s storage flag for SQLite persistence
func TestCLI_StorageFlag_CreateDB(t *testing.T) {
	dbFile := tempFile(t, "test.db")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-s", dbFile,
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")

	// Verify DB file was created
	if !fileExists(dbFile) {
		t.Error("Expected database file to be created")
	}
}

// TestCLI_CombinedFlags tests multiple flags used together
func TestCLI_CombinedFlags_VerboseAndCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-c", "people",
		"-q", "SELECT * FROM people")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
	assertContains(t, stdout, "people")
}

func TestCLI_CombinedFlags_LinesAndExport(t *testing.T) {
	outputFile := tempFile(t, "output.csv")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/large.csv"),
		"-l", "3",
		"-q", "SELECT * FROM large",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	content := readFile(t, outputFile)
	lines := countLines(content)
	// Header + 3 data rows = 4 lines
	if lines > 4 {
		t.Errorf("Expected at most 4 lines (header + 3 data), got %d", lines)
	}
}

func TestCLI_CombinedFlags_AllFlags(t *testing.T) {
	outputFile := tempFile(t, "output.jsonl")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-c", "mydata",
		"-l", "2",
		"-q", "SELECT * FROM mydata",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
	assertContains(t, stdout, "successfully exported")
}

// TestCLI_MissingRequiredFlags tests error handling for missing required flags
func TestCLI_MissingRequiredFlags_NoFile(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-q", "SELECT * FROM table")

	assertError(t, err)
}

func TestCLI_MissingRequiredFlags_ExportWithoutType(t *testing.T) {
	outputFile := tempFile(t, "output.txt")
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile)

	assertError(t, err)
}

// TestCLI_InvalidFlags tests error handling for invalid flag values
func TestCLI_InvalidExportType(t *testing.T) {
	outputFile := tempFile(t, "output.txt")
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "invalid_format")

	assertError(t, err)
}

func TestCLI_InvalidLinesValue(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-l", "abc",
		"-q", "SELECT * FROM simple")

	assertError(t, err)
}

// TestCLI_QueryFlag tests the -q query flag
func TestCLI_QueryFlag_SimpleSelect(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "rows)")
}

func TestCLI_QueryFlag_SelectWithWhere(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id = 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
}

func TestCLI_QueryFlag_SelectWithLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple LIMIT 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "(1 rows)")
}

func TestCLI_QueryFlag_Count(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT COUNT(*) as total FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

// TestCLI_MultipleFileInputs tests multiple -f flags
func TestCLI_MultipleFileInputs_TwoCSV(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("csv/users.csv"),
		"-q", "SELECT COUNT(*) as count FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestCLI_MultipleFileInputs_ShowTables(t *testing.T) {
	commands := `.tables
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("csv/users.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "simple")
	assertContains(t, stdout, "users")
}
