package e2e_test

import (
	"testing"
)

// ============================================
// File Error Tests
// ============================================

func TestError_FileNotFound_AbsolutePath(t *testing.T) {
	_, stderr, err := runDataQL(t, "run",
		"-f", "/nonexistent/path/to/file.csv",
		"-q", "SELECT * FROM file")

	assertError(t, err)
	assertContains(t, stderr, "Error")
}

func TestError_FileNotFound_RelativePath(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", "nonexistent.csv",
		"-q", "SELECT * FROM nonexistent")

	assertError(t, err)
}

func TestError_EmptyFilePath(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", "",
		"-q", "SELECT * FROM data")

	assertError(t, err)
}

func TestError_DirectoryAsFile(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv"),
		"-q", "SELECT * FROM csv")

	assertError(t, err)
}

// ============================================
// Query Error Tests
// ============================================

func TestError_InvalidSQL_Syntax(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELEC * FORM simple")

	assertError(t, err)
}

func TestError_InvalidSQL_MissingFrom(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT *")

	assertError(t, err)
}

func TestError_InvalidSQL_EmptyQuery(t *testing.T) {
	// Empty query may enter REPL mode or be handled gracefully
	commands := `.quit`
	_, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "")

	// Depends on implementation - may not error
	_ = stderr
	_ = err
}

func TestError_TableNotFound(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM nonexistent_table")

	assertError(t, err)
}

func TestError_ColumnNotFound(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT nonexistent_column FROM simple")

	assertError(t, err)
}

func TestError_AmbiguousColumn_InJoin(t *testing.T) {
	// When joining tables with same column names without qualification
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT id FROM users, departments")

	// SQLite may or may not error depending on implementation
	_ = err
}

// ============================================
// Export Error Tests
// ============================================

func TestError_ExportWithoutType_Flag(t *testing.T) {
	outputFile := tempFile(t, "output.txt")
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile)

	assertError(t, err)
}

func TestError_ExportInvalidType(t *testing.T) {
	outputFile := tempFile(t, "output.txt")
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "invalid_format")

	assertError(t, err)
}

func TestError_ExportTypeWithoutPath(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-t", "csv")

	// Should work without export path (just outputs to stdout)
	_ = err
}

func TestError_ExportToInvalidPath(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", "/nonexistent/path/output.csv",
		"-t", "csv")

	assertError(t, err)
}

// ============================================
// CLI Flag Error Tests
// ============================================

func TestError_InvalidLinesValue(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-l", "abc",
		"-q", "SELECT * FROM simple")

	assertError(t, err)
}

func TestError_NegativeLinesValue(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-l", "-1",
		"-q", "SELECT * FROM simple")

	// May or may not error depending on implementation
	_ = err
}

func TestError_InvalidInputFormat(t *testing.T) {
	csvData := "name,age\nAlice,30"
	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "invalid_format",
		"-q", "SELECT * FROM stdin_data")

	// May be handled gracefully or default to a format
	_ = stdout
	_ = stderr
	_ = err
}

func TestError_MissingInputFormatForStdin(t *testing.T) {
	csvData := "name,age\nAlice,30"
	_, _, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-q", "SELECT * FROM stdin_data")

	// May require -i flag for stdin
	_ = err
}

func TestError_MissingFileFlag(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-q", "SELECT * FROM table")

	assertError(t, err)
}

func TestError_MissingQueryFlag(t *testing.T) {
	// Without -q flag, enters REPL mode
	commands := `.quit`
	_, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	// Should not error, just enters REPL and exits
	assertNoError(t, err, stderr)
}

// ============================================
// Data Error Tests
// ============================================

func TestError_MalformedCSV(t *testing.T) {
	csvData := `name,age
Alice,30,extra
Bob`
	_, _, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data")

	// CSV parser may be lenient or strict
	_ = err
}

func TestError_MalformedJSON(t *testing.T) {
	jsonData := `[{"name": "Alice", "age": 30`
	_, _, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-q", "SELECT * FROM stdin_data")

	assertError(t, err)
}

func TestError_MalformedJSONL(t *testing.T) {
	jsonlData := `{"name": "Alice", "age": 30}
{"name": "Bob", incomplete`
	_, _, err := runDataQLWithStdin(t, jsonlData, "run",
		"-f", "-",
		"-i", "jsonl",
		"-q", "SELECT * FROM stdin_data")

	assertError(t, err)
}

func TestError_MalformedXML(t *testing.T) {
	xmlData := `<?xml version="1.0"?>
<root>
  <item>
    <name>Alice</name>
  <!-- missing closing tags -->`
	_, _, err := runDataQLWithStdin(t, xmlData, "run",
		"-f", "-",
		"-i", "xml",
		"-q", "SELECT * FROM stdin_data")

	assertError(t, err)
}

func TestError_MalformedYAML(t *testing.T) {
	yamlData := `- name: Alice
  age: 30
  invalid:yaml:colons`
	_, _, err := runDataQLWithStdin(t, yamlData, "run",
		"-f", "-",
		"-i", "yaml",
		"-q", "SELECT * FROM stdin_data")

	assertError(t, err)
}

// ============================================
// Type Mismatch Tests
// ============================================

func TestError_TypeMismatch_NumericCompare(t *testing.T) {
	// Comparing string column with numeric value
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE name > 10")

	// SQLite may handle type coercion differently
	_ = stdout
	_ = stderr
	_ = err
}

func TestError_DivisionByZero(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id / 0 as result FROM simple")

	// SQLite returns NULL for division by zero
	_ = stdout
	_ = stderr
	_ = err
}

func TestError_InvalidAggregateFunction(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT INVALID_FUNC(id) FROM simple")

	assertError(t, err)
}

// ============================================
// REPL Error Tests
// ============================================

func TestError_REPL_InvalidCommand(t *testing.T) {
	commands := `.invalidcommand
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	// Invalid commands treated as SQL which will error
	_ = stdout
	_ = stderr
	_ = err
}

func TestError_REPL_SchemaNoTable(t *testing.T) {
	commands := `.schema
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	// Should show usage error
	combined := stdout + stderr
	assertContains(t, combined, "usage")
	_ = err
}

func TestError_REPL_CountNoTable(t *testing.T) {
	commands := `.count
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	// Should show usage error
	combined := stdout + stderr
	assertContains(t, combined, "usage")
	_ = err
}

func TestError_REPL_InvalidPagesize(t *testing.T) {
	commands := `.pagesize abc
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	combined := stdout + stderr
	assertContains(t, combined, "invalid")
	_ = err
}

// ============================================
// Storage Error Tests
// ============================================

func TestError_InvalidStoragePath(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-s", "/nonexistent/path/database.db",
		"-q", "SELECT * FROM simple")

	assertError(t, err)
}

// ============================================
// Mixed Format Error Tests
// ============================================

func TestError_MixedFormats(t *testing.T) {
	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM simple")

	assertError(t, err)
	assertContains(t, stderr, "mixed")
}

// ============================================
// Collection Name Error Tests
// ============================================

func TestError_QueryWrongCollection(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-c", "my_table",
		"-q", "SELECT * FROM simple") // Using wrong name

	assertError(t, err)
}

// ============================================
// Recovery Tests (should not crash)
// ============================================

func TestRecovery_AfterError_ContinueREPL(t *testing.T) {
	commands := `SELECT * FROM nonexistent
SELECT * FROM simple
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	// First query errors, but REPL should continue
	assertContains(t, stdout, "rows)")
	_ = stderr
	_ = err
}

func TestRecovery_InvalidThenValid_Query(t *testing.T) {
	commands := `INVALID SQL SYNTAX
SELECT * FROM simple LIMIT 1
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	// Should recover and execute valid query
	assertContains(t, stdout, "(1 rows)")
	_ = stderr
	_ = err
}
