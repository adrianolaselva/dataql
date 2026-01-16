package e2e_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// JSON Export Tests

func TestExport_JSON_BasicExport(t *testing.T) {
	outputFile := tempFile(t, "output.json")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "json")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}

	// Read and verify JSON structure
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	var data []map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if len(data) == 0 {
		t.Error("JSON array is empty")
	}
}

func TestExport_JSON_WithQuery(t *testing.T) {
	outputFile := tempFile(t, "filtered.json")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, name FROM simple WHERE id = '1'",
		"-e", outputFile,
		"-t", "json")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Read and verify
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	var data []map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if len(data) != 1 {
		t.Errorf("Expected 1 row, got %d", len(data))
	}

	// Check columns exist
	if _, ok := data[0]["id"]; !ok {
		t.Error("Expected 'id' column in output")
	}
	if _, ok := data[0]["name"]; !ok {
		t.Error("Expected 'name' column in output")
	}
}

func TestExport_JSON_FromJSON(t *testing.T) {
	outputFile := tempFile(t, "json_to_json.json")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/array.json"),
		"-q", "SELECT * FROM array",
		"-e", outputFile,
		"-t", "json")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify JSON structure
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	var data []map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if len(data) == 0 {
		t.Error("JSON array is empty")
	}
}

func TestExport_JSON_FromXML(t *testing.T) {
	outputFile := tempFile(t, "xml_to_json.json")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "json")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify JSON structure
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	var data []map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if len(data) == 0 {
		t.Error("JSON array is empty")
	}
}

func TestExport_JSON_FromYAML(t *testing.T) {
	outputFile := tempFile(t, "yaml_to_json.json")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "json")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify JSON structure
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	var data []map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if len(data) == 0 {
		t.Error("JSON array is empty")
	}
}

func TestExport_JSON_Formatted(t *testing.T) {
	outputFile := tempFile(t, "formatted.json")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple LIMIT 2",
		"-e", outputFile,
		"-t", "json")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify JSON is formatted (indented)
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	// Check for indentation (formatted JSON)
	if !strings.Contains(string(content), "\n  ") {
		t.Error("Expected formatted (indented) JSON output")
	}
}

func TestExport_JSON_EmptyResult(t *testing.T) {
	outputFile := tempFile(t, "empty.json")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id = 'nonexistent'",
		"-e", outputFile,
		"-t", "json")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file contains empty array
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	var data []map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if len(data) != 0 {
		t.Errorf("Expected empty array, got %d items", len(data))
	}
}

func TestExport_JSON_NestedJSON(t *testing.T) {
	outputFile := tempFile(t, "nested.json")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/nested.json"),
		"-q", "SELECT * FROM nested",
		"-e", outputFile,
		"-t", "json")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify JSON is valid
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	var data []map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}
}

func TestExport_JSON_LargeData(t *testing.T) {
	outputFile := tempFile(t, "large.json")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "json")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created and is valid JSON
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	var data []map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}
}

// Excel Export Tests

func TestExport_Excel_BasicExport(t *testing.T) {
	outputFile := tempFile(t, "output.xlsx")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "excel")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}

	// Verify file is not empty
	info, err := os.Stat(outputFile)
	if err != nil {
		t.Fatalf("Failed to stat output file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("Export file is empty")
	}
}

func TestExport_Excel_XLSXType(t *testing.T) {
	outputFile := tempFile(t, "output.xlsx")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "xlsx")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_Excel_WithQuery(t *testing.T) {
	outputFile := tempFile(t, "filtered.xlsx")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, name FROM simple WHERE id = '1'",
		"-e", outputFile,
		"-t", "excel")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_Excel_FromJSON(t *testing.T) {
	outputFile := tempFile(t, "json_to_excel.xlsx")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/array.json"),
		"-q", "SELECT * FROM array",
		"-e", outputFile,
		"-t", "excel")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_Excel_FromXML(t *testing.T) {
	outputFile := tempFile(t, "xml_to_excel.xlsx")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "excel")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_Excel_FromYAML(t *testing.T) {
	outputFile := tempFile(t, "yaml_to_excel.xlsx")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "excel")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_Excel_EmptyResult(t *testing.T) {
	outputFile := tempFile(t, "empty.xlsx")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id = 'nonexistent'",
		"-e", outputFile,
		"-t", "excel")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created (even if empty data)
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

// Parquet Export Tests

func TestExport_Parquet_BasicExport(t *testing.T) {
	outputFile := tempFile(t, "output.parquet")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "parquet")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}

	// Verify file is not empty
	info, err := os.Stat(outputFile)
	if err != nil {
		t.Fatalf("Failed to stat output file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("Export file is empty")
	}
}

func TestExport_Parquet_WithQuery(t *testing.T) {
	outputFile := tempFile(t, "filtered.parquet")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, name FROM simple WHERE id = '1'",
		"-e", outputFile,
		"-t", "parquet")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_Parquet_FromJSON(t *testing.T) {
	outputFile := tempFile(t, "json_to_parquet.parquet")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/array.json"),
		"-q", "SELECT * FROM array",
		"-e", outputFile,
		"-t", "parquet")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_Parquet_FromXML(t *testing.T) {
	outputFile := tempFile(t, "xml_to_parquet.parquet")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "parquet")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_Parquet_FromYAML(t *testing.T) {
	outputFile := tempFile(t, "yaml_to_parquet.parquet")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "parquet")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_Parquet_FromParquet(t *testing.T) {
	outputFile := tempFile(t, "parquet_to_parquet.parquet")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "parquet")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

// XML Export Tests

func TestExport_XML_BasicExport(t *testing.T) {
	outputFile := tempFile(t, "output.xml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "xml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}

	// Verify XML structure
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	// Check for XML header and structure
	if !strings.Contains(string(content), "<?xml") {
		t.Error("Expected XML header in output")
	}
	if !strings.Contains(string(content), "<data>") {
		t.Error("Expected <data> root element")
	}
	if !strings.Contains(string(content), "<row>") {
		t.Error("Expected <row> elements")
	}
}

func TestExport_XML_WithQuery(t *testing.T) {
	outputFile := tempFile(t, "filtered.xml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, name FROM simple WHERE id = '1'",
		"-e", outputFile,
		"-t", "xml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}

	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	// Check columns exist
	if !strings.Contains(string(content), "<id>") {
		t.Error("Expected 'id' element in output")
	}
	if !strings.Contains(string(content), "<name>") {
		t.Error("Expected 'name' element in output")
	}
}

func TestExport_XML_FromJSON(t *testing.T) {
	outputFile := tempFile(t, "json_to_xml.xml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/array.json"),
		"-q", "SELECT * FROM array",
		"-e", outputFile,
		"-t", "xml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_XML_FromXML(t *testing.T) {
	outputFile := tempFile(t, "xml_to_xml.xml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "xml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_XML_FromYAML(t *testing.T) {
	outputFile := tempFile(t, "yaml_to_xml.xml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "xml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_XML_EmptyResult(t *testing.T) {
	outputFile := tempFile(t, "empty.xml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id = 'nonexistent'",
		"-e", outputFile,
		"-t", "xml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_XML_Formatted(t *testing.T) {
	outputFile := tempFile(t, "formatted.xml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple LIMIT 2",
		"-e", outputFile,
		"-t", "xml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify XML is formatted (indented)
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	// Check for indentation (formatted XML)
	if !strings.Contains(string(content), "\n  ") {
		t.Error("Expected formatted (indented) XML output")
	}
}

// YAML Export Tests

func TestExport_YAML_BasicExport(t *testing.T) {
	outputFile := tempFile(t, "output.yaml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "yaml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}

	// Verify file is not empty
	info, err := os.Stat(outputFile)
	if err != nil {
		t.Fatalf("Failed to stat output file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("Export file is empty")
	}
}

func TestExport_YAML_YMLType(t *testing.T) {
	outputFile := tempFile(t, "output.yml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "yml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_YAML_WithQuery(t *testing.T) {
	outputFile := tempFile(t, "filtered.yaml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, name FROM simple WHERE id = '1'",
		"-e", outputFile,
		"-t", "yaml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}

	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	// Check fields exist
	if !strings.Contains(string(content), "id:") {
		t.Error("Expected 'id' field in output")
	}
	if !strings.Contains(string(content), "name:") {
		t.Error("Expected 'name' field in output")
	}
}

func TestExport_YAML_FromJSON(t *testing.T) {
	outputFile := tempFile(t, "json_to_yaml.yaml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/array.json"),
		"-q", "SELECT * FROM array",
		"-e", outputFile,
		"-t", "yaml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_YAML_FromXML(t *testing.T) {
	outputFile := tempFile(t, "xml_to_yaml.yaml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "yaml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_YAML_FromYAML(t *testing.T) {
	outputFile := tempFile(t, "yaml_to_yaml.yaml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-q", "SELECT * FROM users",
		"-e", outputFile,
		"-t", "yaml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}

func TestExport_YAML_EmptyResult(t *testing.T) {
	outputFile := tempFile(t, "empty.yaml")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id = 'nonexistent'",
		"-e", outputFile,
		"-t", "yaml")

	if err != nil {
		t.Fatalf("Export failed: %v\nstderr: %s", err, stderr)
	}

	// Verify file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatal("Export file was not created")
	}
}
