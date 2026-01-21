package e2e_test

import (
	"testing"
)

// Format Conversion Matrix Tests
// Input formats: CSV, JSON, JSONL, XML, YAML, Excel, Parquet
// Export formats: CSV, JSONL, JSON, XML, YAML, Excel, Parquet

// ============================================
// CSV Input Conversions
// ============================================

func TestConversion_CSV_To_CSV(t *testing.T) {
	outputFile := tempFile(t, "output.csv")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_CSV_To_JSONL(t *testing.T) {
	outputFile := tempFile(t, "output.jsonl")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_CSV_To_JSON(t *testing.T) {
	outputFile := tempFile(t, "output.json")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "json")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_CSV_To_XML(t *testing.T) {
	outputFile := tempFile(t, "output.xml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "xml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_CSV_To_YAML(t *testing.T) {
	outputFile := tempFile(t, "output.yaml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "yaml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_CSV_To_Excel(t *testing.T) {
	outputFile := tempFile(t, "output.xlsx")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "excel")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_CSV_To_Parquet(t *testing.T) {
	outputFile := tempFile(t, "output.parquet")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "parquet")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

// ============================================
// JSON Input Conversions
// ============================================

func TestConversion_JSON_To_CSV(t *testing.T) {
	outputFile := tempFile(t, "output.csv")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSON_To_JSONL(t *testing.T) {
	outputFile := tempFile(t, "output.jsonl")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSON_To_JSON(t *testing.T) {
	outputFile := tempFile(t, "output.json")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people",
		"-e", outputFile,
		"-t", "json")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSON_To_XML(t *testing.T) {
	outputFile := tempFile(t, "output.xml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people",
		"-e", outputFile,
		"-t", "xml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSON_To_YAML(t *testing.T) {
	outputFile := tempFile(t, "output.yaml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people",
		"-e", outputFile,
		"-t", "yaml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSON_To_Excel(t *testing.T) {
	outputFile := tempFile(t, "output.xlsx")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people",
		"-e", outputFile,
		"-t", "excel")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSON_To_Parquet(t *testing.T) {
	outputFile := tempFile(t, "output.parquet")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people",
		"-e", outputFile,
		"-t", "parquet")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

// ============================================
// JSONL Input Conversions
// ============================================

func TestConversion_JSONL_To_CSV(t *testing.T) {
	outputFile := tempFile(t, "output.csv")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSONL_To_JSONL(t *testing.T) {
	outputFile := tempFile(t, "output.jsonl")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSONL_To_JSON(t *testing.T) {
	outputFile := tempFile(t, "output.json")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "json")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSONL_To_XML(t *testing.T) {
	outputFile := tempFile(t, "output.xml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "xml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSONL_To_YAML(t *testing.T) {
	outputFile := tempFile(t, "output.yaml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "yaml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSONL_To_Excel(t *testing.T) {
	outputFile := tempFile(t, "output.xlsx")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "excel")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_JSONL_To_Parquet(t *testing.T) {
	outputFile := tempFile(t, "output.parquet")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "parquet")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

// ============================================
// XML Input Conversions
// ============================================

func TestConversion_XML_To_CSV(t *testing.T) {
	outputFile := tempFile(t, "output.csv")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_XML_To_JSONL(t *testing.T) {
	outputFile := tempFile(t, "output.jsonl")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_XML_To_JSON(t *testing.T) {
	outputFile := tempFile(t, "output.json")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "json")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_XML_To_XML(t *testing.T) {
	outputFile := tempFile(t, "output.xml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "xml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_XML_To_YAML(t *testing.T) {
	outputFile := tempFile(t, "output.yaml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "yaml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_XML_To_Excel(t *testing.T) {
	outputFile := tempFile(t, "output.xlsx")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "excel")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_XML_To_Parquet(t *testing.T) {
	outputFile := tempFile(t, "output.parquet")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "parquet")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

// ============================================
// YAML Input Conversions
// ============================================

func TestConversion_YAML_To_CSV(t *testing.T) {
	outputFile := tempFile(t, "output.csv")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_YAML_To_JSONL(t *testing.T) {
	outputFile := tempFile(t, "output.jsonl")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_YAML_To_JSON(t *testing.T) {
	outputFile := tempFile(t, "output.json")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "json")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_YAML_To_XML(t *testing.T) {
	outputFile := tempFile(t, "output.xml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "xml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_YAML_To_YAML(t *testing.T) {
	outputFile := tempFile(t, "output.yaml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "yaml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_YAML_To_Excel(t *testing.T) {
	outputFile := tempFile(t, "output.xlsx")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "excel")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_YAML_To_Parquet(t *testing.T) {
	outputFile := tempFile(t, "output.parquet")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "parquet")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

// ============================================
// Excel Input Conversions
// ============================================

func TestConversion_Excel_To_CSV(t *testing.T) {
	outputFile := tempFile(t, "output.csv")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Excel_To_JSONL(t *testing.T) {
	outputFile := tempFile(t, "output.jsonl")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Excel_To_JSON(t *testing.T) {
	outputFile := tempFile(t, "output.json")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "json")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Excel_To_XML(t *testing.T) {
	outputFile := tempFile(t, "output.xml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "xml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Excel_To_YAML(t *testing.T) {
	outputFile := tempFile(t, "output.yaml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "yaml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Excel_To_Excel(t *testing.T) {
	outputFile := tempFile(t, "output.xlsx")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "excel")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Excel_To_Parquet(t *testing.T) {
	outputFile := tempFile(t, "output.parquet")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "parquet")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

// ============================================
// Parquet Input Conversions
// ============================================

func TestConversion_Parquet_To_CSV(t *testing.T) {
	outputFile := tempFile(t, "output.csv")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Parquet_To_JSONL(t *testing.T) {
	outputFile := tempFile(t, "output.jsonl")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Parquet_To_JSON(t *testing.T) {
	outputFile := tempFile(t, "output.json")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "json")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Parquet_To_XML(t *testing.T) {
	outputFile := tempFile(t, "output.xml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "xml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Parquet_To_YAML(t *testing.T) {
	outputFile := tempFile(t, "output.yaml")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "yaml")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Parquet_To_Excel(t *testing.T) {
	outputFile := tempFile(t, "output.xlsx")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "excel")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

func TestConversion_Parquet_To_Parquet(t *testing.T) {
	outputFile := tempFile(t, "output.parquet")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "parquet")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")
}

// ============================================
// Data Integrity Tests
// ============================================

func TestConversion_DataIntegrity_CSV_To_JSON_To_CSV(t *testing.T) {
	jsonFile := tempFile(t, "intermediate.json")
	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", jsonFile,
		"-t", "json")
	assertNoError(t, err, stderr)

	csvFile := tempFile(t, "output.csv")
	_, stderr, err = runDataQL(t, "run",
		"-f", jsonFile,
		"-q", "SELECT * FROM intermediate",
		"-e", csvFile,
		"-t", "csv")
	assertNoError(t, err, stderr)

	content := readFile(t, csvFile)
	assertContains(t, content, "John")
}

func TestConversion_DataIntegrity_JSON_To_YAML_To_JSON(t *testing.T) {
	yamlFile := tempFile(t, "intermediate.yaml")
	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people",
		"-e", yamlFile,
		"-t", "yaml")
	assertNoError(t, err, stderr)

	jsonFile := tempFile(t, "output.json")
	_, stderr, err = runDataQL(t, "run",
		"-f", yamlFile,
		"-q", "SELECT * FROM intermediate",
		"-e", jsonFile,
		"-t", "json")
	assertNoError(t, err, stderr)

	if !fileExists(jsonFile) {
		t.Error("Expected JSON file to be created")
	}
}

func TestConversion_DataIntegrity_XML_To_CSV_To_XML(t *testing.T) {
	csvFile := tempFile(t, "intermediate.csv")
	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", csvFile,
		"-t", "csv")
	assertNoError(t, err, stderr)

	xmlFile := tempFile(t, "output.xml")
	_, stderr, err = runDataQL(t, "run",
		"-f", csvFile,
		"-q", "SELECT * FROM intermediate",
		"-e", xmlFile,
		"-t", "xml")
	assertNoError(t, err, stderr)

	if !fileExists(xmlFile) {
		t.Error("Expected XML file to be created")
	}
}
