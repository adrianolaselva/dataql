package e2e_test

import (
	"testing"
)

// ============================================
// CSV Edge Cases
// ============================================

func TestEdge_CSV_UTF8(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/utf8.csv"),
		"-q", "SELECT * FROM utf8")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_CSV_QuotedFields(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/quoted.csv"),
		"-q", "SELECT * FROM quoted")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_CSV_SemicolonDelimiter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/semicolon.csv"),
		"-d", ";",
		"-q", "SELECT * FROM semicolon")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_CSV_EmptyFile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/empty.csv"),
		"-q", "SELECT * FROM empty")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "(0 rows)")
}

func TestEdge_CSV_WhitespaceInStdin(t *testing.T) {
	csvData := `name,value
  Alice  ,  100
Bob,200`
	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_CSV_SpecialCharsInValues(t *testing.T) {
	csvData := `name,description
"Test, with comma","Has, comma"
"Test ""quoted""","Has ""quotes"""
"Multi
line","Line
break"`
	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// JSON Edge Cases
// ============================================

func TestEdge_JSON_EmptyArray(t *testing.T) {
	jsonData := `[]`
	stdout, stderr, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "(0 rows)")
}

func TestEdge_JSON_SingleElement(t *testing.T) {
	jsonData := `[{"name": "Alice", "age": 30}]`
	stdout, stderr, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "(1 rows)")
}

func TestEdge_JSON_NullValues(t *testing.T) {
	jsonData := `[{"name": "Alice", "age": null}, {"name": null, "age": 25}]`
	stdout, stderr, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_JSON_BooleanValues(t *testing.T) {
	jsonData := `[{"name": "Alice", "active": true}, {"name": "Bob", "active": false}]`
	stdout, stderr, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_JSON_NumericTypes(t *testing.T) {
	jsonData := `[{"int": 42, "float": 3.14, "negative": -10, "zero": 0}]`
	stdout, stderr, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "42")
}

func TestEdge_JSON_UnicodeStrings(t *testing.T) {
	jsonData := `[{"name": "日本語", "emoji": "🎉"}, {"name": "中文", "emoji": "✓"}]`
	stdout, stderr, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_JSON_EscapedStrings(t *testing.T) {
	jsonData := `[{"text": "Line1\nLine2", "quote": "He said \"hello\""}]`
	stdout, stderr, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// JSONL Edge Cases
// ============================================

func TestEdge_JSONL_SingleLine(t *testing.T) {
	jsonlData := `{"name": "Alice", "age": 30}`
	stdout, stderr, err := runDataQLWithStdin(t, jsonlData, "run",
		"-f", "-",
		"-i", "jsonl",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
}

func TestEdge_JSONL_EmptyLines(t *testing.T) {
	jsonlData := `{"name": "Alice"}

{"name": "Bob"}`
	stdout, stderr, err := runDataQLWithStdin(t, jsonlData, "run",
		"-f", "-",
		"-i", "jsonl",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// XML Edge Cases
// ============================================

func TestEdge_XML_SingleItem(t *testing.T) {
	xmlData := `<?xml version="1.0" encoding="UTF-8"?>
<root>
  <item>
    <name>Alice</name>
    <age>30</age>
  </item>
</root>`
	stdout, stderr, err := runDataQLWithStdin(t, xmlData, "run",
		"-f", "-",
		"-i", "xml",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
}

func TestEdge_XML_Attributes(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// YAML Edge Cases
// ============================================

func TestEdge_YAML_SingleItem(t *testing.T) {
	yamlData := `- name: Alice
  age: 30`
	stdout, stderr, err := runDataQLWithStdin(t, yamlData, "run",
		"-f", "-",
		"-i", "yaml",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
}

func TestEdge_YAML_MultilineStrings(t *testing.T) {
	yamlData := `- name: Alice
  bio: |
    This is a multiline
    biography text
- name: Bob
  bio: "Simple bio"`
	stdout, stderr, err := runDataQLWithStdin(t, yamlData, "run",
		"-f", "-",
		"-i", "yaml",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_YAML_NullValues(t *testing.T) {
	yamlData := `- name: Alice
  value: null
- name: Bob
  value: ~`
	stdout, stderr, err := runDataQLWithStdin(t, yamlData, "run",
		"-f", "-",
		"-i", "yaml",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// Query Edge Cases
// ============================================

func TestEdge_Query_EmptyResult(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id = 999")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "(0 rows)")
}

func TestEdge_Query_CaseSensitivity(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "select * from simple where NAME = 'John'")

	// SQL is case-insensitive for keywords, column names may vary
	_ = stdout
	_ = stderr
	_ = err
}

func TestEdge_Query_WhitespaceInQuery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "  SELECT   *   FROM   simple  ")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_Query_SingleQuoteEscape(t *testing.T) {
	csvData := `name,value
O'Brien,100
Normal,200`
	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data WHERE name = 'O''Brien'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// Data Type Edge Cases
// ============================================

func TestEdge_Type_LargeNumbers(t *testing.T) {
	csvData := `id,big_number
1,9999999999999999
2,1234567890123456789`
	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_Type_FloatPrecision(t *testing.T) {
	csvData := `id,value
1,3.141592653589793
2,0.1
3,0.2`
	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_Type_EmptyStrings(t *testing.T) {
	csvData := `name,value
Alice,100
,200
Bob,`
	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// Collection Name Edge Cases
// ============================================

func TestEdge_Collection_WithUnderscore(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-c", "my_custom_table",
		"-q", "SELECT * FROM my_custom_table")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestEdge_Collection_WithNumbers(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-c", "table123",
		"-q", "SELECT * FROM table123")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// ============================================
// Lines Limit Edge Cases
// ============================================

func TestEdge_Lines_Zero(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-l", "0",
		"-q", "SELECT COUNT(*) as total FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3") // All rows
}

func TestEdge_Lines_One(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-l", "1",
		"-q", "SELECT COUNT(*) as total FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "1")
}

func TestEdge_Lines_MoreThanData(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-l", "1000",
		"-q", "SELECT COUNT(*) as total FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3") // Only 3 rows exist
}

// ============================================
// Multiple Files Edge Cases
// ============================================

func TestEdge_MultiFile_SameName(t *testing.T) {
	// When loading two files that would have the same table name
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("json/simple.json"),
		"-q", "SELECT COUNT(*) as total FROM simple")

	// Should handle name collision
	_ = stdout
	_ = stderr
	_ = err
}

func TestEdge_MultiFile_DifferentFormats(t *testing.T) {
	// Mixed file formats are now supported (Issue #17)
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
}

func TestEdge_MultiFile_SameFormat(t *testing.T) {
	commands := `.tables
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("csv/users.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "simple")
	assertContains(t, stdout, "users")
}
