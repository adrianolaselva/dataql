package e2e_test

import (
	"testing"
)

// ============================================
// Verbose Mode Basic Tests
// ============================================

func TestVerbose_CSV_Import(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

func TestVerbose_JSON_Import(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-v",
		"-q", "SELECT * FROM people")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

func TestVerbose_JSONL_Import(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-v",
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

func TestVerbose_XML_Import(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("xml/users.xml"),
		"-v",
		"-q", "SELECT * FROM users")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

func TestVerbose_YAML_Import(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("yaml/users.yaml"),
		"-v",
		"-q", "SELECT * FROM users")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

func TestVerbose_Excel_Import(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("excel/users.xlsx"),
		"-v",
		"-q", "SELECT * FROM users")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

func TestVerbose_Parquet_Import(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("parquet/users.parquet"),
		"-v",
		"-q", "SELECT * FROM users")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

// ============================================
// Verbose Mode Shows Details
// ============================================

func TestVerbose_ShowsFileInputs(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "File inputs")
}

func TestVerbose_ShowsInitialization(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	// Should show initialization details
	assertContains(t, combined, "[VERBOSE]")
}

func TestVerbose_ShowsQueryExecution(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-q", "SELECT COUNT(*) FROM simple")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

// ============================================
// Verbose Mode with Export
// ============================================

func TestVerbose_WithExport_CSV(t *testing.T) {
	outputFile := tempFile(t, "output.csv")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "csv")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
	assertContains(t, stdout, "successfully exported")
}

func TestVerbose_WithExport_JSON(t *testing.T) {
	outputFile := tempFile(t, "output.json")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-q", "SELECT * FROM simple",
		"-e", outputFile,
		"-t", "json")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
	assertContains(t, stdout, "successfully exported")
}

// ============================================
// Verbose Mode with Multiple Files
// ============================================

func TestVerbose_MultipleFiles(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("csv/users.csv"),
		"-v",
		"-q", "SELECT COUNT(*) as total FROM simple")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

// ============================================
// Verbose Mode with Stdin
// ============================================

func TestVerbose_Stdin_CSV(t *testing.T) {
	csvData := `name,age
Alice,30
Bob,25`
	stdout, stderr, err := runDataQLWithStdin(t, csvData, "run",
		"-f", "-",
		"-i", "csv",
		"-v",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

func TestVerbose_Stdin_JSON(t *testing.T) {
	jsonData := `[{"name": "Alice"}, {"name": "Bob"}]`
	stdout, stderr, err := runDataQLWithStdin(t, jsonData, "run",
		"-f", "-",
		"-i", "json",
		"-v",
		"-q", "SELECT * FROM stdin_data")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

// ============================================
// Verbose Mode with Lines Limit
// ============================================

func TestVerbose_WithLinesLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-l", "2",
		"-q", "SELECT COUNT(*) as total FROM simple")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
	assertContains(t, stdout, "2")
}

// ============================================
// Verbose Mode with Collection
// ============================================

func TestVerbose_WithCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-c", "my_data",
		"-q", "SELECT * FROM my_data")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

// ============================================
// Verbose Mode with Storage
// ============================================

func TestVerbose_WithStorage(t *testing.T) {
	dbFile := tempFile(t, "test.db")
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-v",
		"-s", dbFile,
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

// ============================================
// Verbose Mode in REPL
// ============================================

func TestVerbose_REPL_Mode(t *testing.T) {
	commands := `SELECT * FROM simple
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"),
		"-v")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	assertContains(t, combined, "[VERBOSE]")
}

// ============================================
// Without Verbose Mode (Control)
// ============================================

func TestNoVerbose_NoVerboseOutput(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple")

	assertNoError(t, err, stderr)
	combined := stdout + stderr
	// Should NOT contain verbose markers
	assertNotContains(t, combined, "[VERBOSE]")
}
