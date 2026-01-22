package e2e_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Markdown export tests

func TestExport_Markdown_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "output.md")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple ORDER BY id",
		"-e", exportPath,
		"-t", "markdown")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	// Verify exported file
	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	contentStr := string(content)

	// Check header row
	if !strings.Contains(contentStr, "| id |") {
		t.Errorf("Missing id header in markdown output")
	}

	// Check separator
	if !strings.Contains(contentStr, "| --- |") {
		t.Errorf("Missing separator in markdown output")
	}

	// Check data
	if !strings.Contains(contentStr, "John") {
		t.Errorf("Missing data in markdown output")
	}
}

func TestExport_Markdown_MD_Alias(t *testing.T) {
	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "output.md")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple ORDER BY id",
		"-e", exportPath,
		"-t", "md") // Using 'md' alias

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	// Verify file was created
	if _, err := os.Stat(exportPath); err != nil {
		t.Errorf("Markdown file not created: %v", err)
	}
}

func TestExport_Markdown_JSON_Source(t *testing.T) {
	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "output.md")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people ORDER BY id",
		"-e", exportPath,
		"-t", "markdown")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	// Check that it's valid markdown table
	lines := strings.Split(string(content), "\n")
	if len(lines) < 3 {
		t.Errorf("Markdown table should have at least 3 lines (header, separator, data)")
	}

	// First line should be header with pipes
	if !strings.HasPrefix(lines[0], "|") || !strings.HasSuffix(strings.TrimSpace(lines[0]), "|") {
		t.Errorf("Header line should start and end with |")
	}

	// Second line should be separator with dashes
	if !strings.Contains(lines[1], "---") {
		t.Errorf("Second line should be separator with ---")
	}
}

func TestExport_Markdown_WithAggregation(t *testing.T) {
	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "output.md")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT COUNT(*) as total FROM simple",
		"-e", exportPath,
		"-t", "markdown")

	assertNoError(t, err, stderr)

	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	if !strings.Contains(string(content), "total") {
		t.Errorf("Missing aggregation column in output")
	}

	if !strings.Contains(string(content), "3") {
		t.Errorf("Missing aggregation value in output")
	}
}

// HTML export tests

func TestExport_HTML_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "output.html")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple ORDER BY id",
		"-e", exportPath,
		"-t", "html")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	// Verify exported file
	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	contentStr := string(content)

	// Check HTML structure
	if !strings.Contains(contentStr, "<!DOCTYPE html>") {
		t.Errorf("Missing DOCTYPE declaration")
	}

	if !strings.Contains(contentStr, "<table>") {
		t.Errorf("Missing table element")
	}

	if !strings.Contains(contentStr, "<th>") {
		t.Errorf("Missing header cells")
	}

	if !strings.Contains(contentStr, "<td>") {
		t.Errorf("Missing data cells")
	}

	if !strings.Contains(contentStr, "</table>") {
		t.Errorf("Missing closing table tag")
	}

	if !strings.Contains(contentStr, "</html>") {
		t.Errorf("Missing closing html tag")
	}
}

func TestExport_HTML_JSON_Source(t *testing.T) {
	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "output.html")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people ORDER BY id",
		"-e", exportPath,
		"-t", "html")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	contentStr := string(content)

	// Check for user data
	if !strings.Contains(contentStr, "Alice") {
		t.Errorf("Missing Alice in HTML output")
	}

	if !strings.Contains(contentStr, "Bob") {
		t.Errorf("Missing Bob in HTML output")
	}
}

func TestExport_HTML_HasStyles(t *testing.T) {
	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "output.html")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple LIMIT 1",
		"-e", exportPath,
		"-t", "html")

	assertNoError(t, err, stderr)

	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	contentStr := string(content)

	// Check for CSS styles
	if !strings.Contains(contentStr, "<style>") {
		t.Errorf("Missing style element")
	}

	if !strings.Contains(contentStr, "border-collapse") {
		t.Errorf("Missing table border styling")
	}
}

func TestExport_HTML_SpecialCharacters(t *testing.T) {
	// Create a temp CSV with special characters
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "special.csv")
	exportPath := filepath.Join(tmpDir, "output.html")

	// Create CSV with HTML special characters
	csvContent := "id,description\n1,\"<script>alert('xss')</script>\"\n"
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to create test CSV: %v", err)
	}

	_, stderr, err := runDataQL(t, "run",
		"-f", csvPath,
		"-q", "SELECT * FROM special",
		"-e", exportPath,
		"-t", "html")

	assertNoError(t, err, stderr)

	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	contentStr := string(content)

	// Script tags should be escaped
	if strings.Contains(contentStr, "<script>") {
		t.Errorf("Script tags not escaped - potential XSS vulnerability!")
	}

	// Should contain escaped version
	if !strings.Contains(contentStr, "&lt;script&gt;") {
		t.Errorf("HTML not properly escaped")
	}
}

func TestExport_HTML_WithAggregation(t *testing.T) {
	tmpDir := t.TempDir()
	exportPath := filepath.Join(tmpDir, "output.html")

	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT COUNT(*) as total FROM simple",
		"-e", exportPath,
		"-t", "html")

	assertNoError(t, err, stderr)

	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	contentStr := string(content)

	if !strings.Contains(contentStr, "<th>total</th>") {
		t.Errorf("Missing aggregation column header")
	}

	if !strings.Contains(contentStr, "<td>3</td>") {
		t.Errorf("Missing aggregation value")
	}
}
