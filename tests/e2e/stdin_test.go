package e2e_test

import (
	"testing"
)

func TestStdin_CSV_BasicQuery(t *testing.T) {
	csvData := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago`

	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestStdin_CSV_WithDelimiter(t *testing.T) {
	csvData := `name;age;city
Alice;30;New York
Bob;25;Los Angeles`

	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-d", ";",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "New York")
}

func TestStdin_CSV_SelectSpecificColumns(t *testing.T) {
	csvData := `user_id,name,email,age
101,Alice,alice@example.com,30
102,Bob,bob@example.com,25
103,Charlie,charlie@example.com,35`

	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT name, email FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "alice@example.com")
	assertNotContains(t, stdout, "101") // user_id should not be in output
}

func TestStdin_CSV_WithWhere(t *testing.T) {
	csvData := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago`

	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data WHERE age > 28")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Charlie")
	assertNotContains(t, stdout, "Bob")
}

func TestStdin_CSV_WithOrderBy(t *testing.T) {
	csvData := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago`

	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT name, age FROM stdin_data ORDER BY age")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Charlie")
}

func TestStdin_CSV_WithLimit(t *testing.T) {
	csvData := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago
Diana,28,Seattle
Eve,32,Boston`

	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data LIMIT 2")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "(2 rows)")
}

func TestStdin_CSV_CountRows(t *testing.T) {
	csvData := `name,age
Alice,30
Bob,25
Charlie,35`

	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT COUNT(*) as total FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestStdin_JSON_BasicQuery(t *testing.T) {
	jsonData := `[
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
		{"name": "Charlie", "age": 35}
	]`

	stdout, stderr, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestStdin_JSON_WithWhere(t *testing.T) {
	jsonData := `[
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
		{"name": "Charlie", "age": 35}
	]`

	stdout, stderr, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-q", "SELECT name FROM stdin_data WHERE age >= 30")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Charlie")
	assertNotContains(t, stdout, "Bob")
}

func TestStdin_JSONL_BasicQuery(t *testing.T) {
	jsonlData := `{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie", "age": 35}`

	stdout, stderr, err := runDataQLWithStdin(t, jsonlData, "run",
		"-f", "-",
		"-i", "jsonl",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestStdin_JSONL_WithFilter(t *testing.T) {
	jsonlData := `{"name": "Alice", "age": 30, "city": "NYC"}
{"name": "Bob", "age": 25, "city": "LA"}
{"name": "Charlie", "age": 35, "city": "Chicago"}`

	stdout, stderr, err := runDataQLWithStdin(t, jsonlData, "run",
		"-f", "-",
		"-i", "jsonl",
		"-q", "SELECT name, city FROM stdin_data WHERE city = 'LA'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "LA")
	assertNotContains(t, stdout, "Alice")
}

func TestStdin_CSV_Export(t *testing.T) {
	csvData := `name,age
Alice,30
Bob,25`

	outputFile := tempFile(t, "output.csv")

	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	// Verify output file
	content := readFile(t, outputFile)
	assertContains(t, content, "Alice")
	assertContains(t, content, "Bob")
}

func TestStdin_CSV_ExportToJSONL(t *testing.T) {
	csvData := `name,age
Alice,30
Bob,25`

	outputFile := tempFile(t, "output.jsonl")

	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-q", "SELECT * FROM stdin_data",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	// Verify output file contains JSONL format
	content := readFile(t, outputFile)
	assertContains(t, content, "Alice")
	assertContains(t, content, "30")
}

func TestStdin_YAML_BasicQuery(t *testing.T) {
	yamlData := `- name: Alice
  age: 30
- name: Bob
  age: 25
- name: Charlie
  age: 35`

	stdout, stderr, err := runDataQLWithStdin(t, yamlData, "run",
		"-f", "-",
		"-i", "yaml",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestStdin_CustomCollection(t *testing.T) {
	csvData := `name,age
Alice,30
Bob,25`

	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-c", "people",
		"-q", "SELECT * FROM people")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
}

func TestStdin_Lines_Limit(t *testing.T) {
	csvData := `name,age
Alice,30
Bob,25
Charlie,35
Diana,28
Eve,32`

	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-l", "2",
		"-q", "SELECT COUNT(*) as total FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "2")
}

// XML stdin tests
func TestStdin_XML_BasicQuery(t *testing.T) {
	xmlData := `<?xml version="1.0" encoding="UTF-8"?>
<root>
  <item>
    <name>Alice</name>
    <age>30</age>
  </item>
  <item>
    <name>Bob</name>
    <age>25</age>
  </item>
  <item>
    <name>Charlie</name>
    <age>35</age>
  </item>
</root>`

	stdout, stderr, err := runDataQLWithStdin(t, xmlData, "run",
		"-f", "-",
		"-i", "xml",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
}

func TestStdin_XML_WithFilter(t *testing.T) {
	xmlData := `<?xml version="1.0" encoding="UTF-8"?>
<root>
  <item>
    <name>Alice</name>
    <age>30</age>
  </item>
  <item>
    <name>Bob</name>
    <age>25</age>
  </item>
  <item>
    <name>Charlie</name>
    <age>35</age>
  </item>
</root>`

	stdout, stderr, err := runDataQLWithStdin(t, xmlData, "run",
		"-f", "-",
		"-i", "xml",
		"-q", "SELECT name FROM stdin_data WHERE age >= 30")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Charlie")
	assertNotContains(t, stdout, "Bob")
}

func TestStdin_XML_Export(t *testing.T) {
	xmlData := `<?xml version="1.0" encoding="UTF-8"?>
<root>
  <item>
    <name>Alice</name>
    <age>30</age>
  </item>
  <item>
    <name>Bob</name>
    <age>25</age>
  </item>
</root>`

	outputFile := tempFile(t, "output.csv")

	stdout, stderr, err := runDataQLWithStdin(t, xmlData, "run",
		"-f", "-",
		"-i", "xml",
		"-q", "SELECT * FROM stdin_data",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	content := readFile(t, outputFile)
	assertContains(t, content, "Alice")
	assertContains(t, content, "Bob")
}

func TestStdin_XML_CustomCollection(t *testing.T) {
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
		"-c", "people",
		"-q", "SELECT * FROM people")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
}
