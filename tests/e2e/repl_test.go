package e2e_test

import (
	"testing"
)

// TestREPL_TablesCommand tests the \d and .tables commands
func TestREPL_TablesCommand_Backslash(t *testing.T) {
	// Load CSV and then execute \d command via stdin
	commands := `\d
\q`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "simple") // Table name should appear
}

func TestREPL_TablesCommand_Dot(t *testing.T) {
	commands := `.tables
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "simple")
}

// TestREPL_SchemaCommand tests the \dt and .schema commands
func TestREPL_SchemaCommand_Backslash(t *testing.T) {
	commands := `\dt simple
\q`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	// Should show column info from PRAGMA table_info
	assertContains(t, stdout, "name")
}

func TestREPL_SchemaCommand_Dot(t *testing.T) {
	commands := `.schema simple
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestREPL_SchemaCommand_MissingTable(t *testing.T) {
	commands := `\dt
\q`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	// Should show usage error
	_ = err
	combined := stdout + stderr
	assertContains(t, combined, "usage")
}

// TestREPL_CountCommand tests the \c and .count commands
func TestREPL_CountCommand_Backslash(t *testing.T) {
	commands := `\c simple
\q`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows")
}

func TestREPL_CountCommand_Dot(t *testing.T) {
	commands := `.count simple
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows")
}

func TestREPL_CountCommand_MissingTable(t *testing.T) {
	commands := `\c
\q`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	_ = err
	combined := stdout + stderr
	assertContains(t, combined, "usage")
}

// TestREPL_HelpCommand tests the \h, .help, and \? commands
func TestREPL_HelpCommand_Backslash_h(t *testing.T) {
	commands := `\h
\q`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "REPL Commands")
	assertContains(t, stdout, ".tables")
	assertContains(t, stdout, ".schema")
}

func TestREPL_HelpCommand_Dot(t *testing.T) {
	commands := `.help
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "REPL Commands")
}

func TestREPL_HelpCommand_QuestionMark(t *testing.T) {
	commands := `\?
\q`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "REPL Commands")
}

// TestREPL_VersionCommand tests the .version command
func TestREPL_VersionCommand(t *testing.T) {
	commands := `.version
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "dataql")
	assertContains(t, stdout, "version")
}

// TestREPL_QuitCommands tests various quit commands
func TestREPL_QuitCommand_Backslash(t *testing.T) {
	commands := `\q`
	_, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
}

func TestREPL_QuitCommand_Dot_quit(t *testing.T) {
	commands := `.quit`
	_, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
}

func TestREPL_QuitCommand_Dot_exit(t *testing.T) {
	commands := `.exit`
	_, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
}

// TestREPL_TimingCommand tests the .timing command
func TestREPL_TimingCommand_On(t *testing.T) {
	commands := `.timing on
SELECT * FROM simple
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Timing enabled")
	// When timing is on, output should include time
	assertContains(t, stdout, "rows in")
}

func TestREPL_TimingCommand_Off(t *testing.T) {
	commands := `.timing on
.timing off
SELECT * FROM simple
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Timing disabled")
}

func TestREPL_TimingCommand_Status(t *testing.T) {
	commands := `.timing
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Timing is")
}

// TestREPL_PagingCommand tests the .paging command
func TestREPL_PagingCommand_On(t *testing.T) {
	commands := `.paging on
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Paging enabled")
}

func TestREPL_PagingCommand_Off(t *testing.T) {
	commands := `.paging on
.paging off
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Paging disabled")
}

func TestREPL_PagingCommand_Status(t *testing.T) {
	commands := `.paging
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Paging is")
}

// TestREPL_PagesizeCommand tests the .pagesize command
func TestREPL_PagesizeCommand_Set(t *testing.T) {
	commands := `.pagesize 50
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Page size set to 50")
}

func TestREPL_PagesizeCommand_Status(t *testing.T) {
	commands := `.pagesize
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Current page size")
}

func TestREPL_PagesizeCommand_Invalid(t *testing.T) {
	commands := `.pagesize abc
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	_ = err
	combined := stdout + stderr
	assertContains(t, combined, "invalid page size")
}

// TestREPL_ClearCommand tests the .clear command
func TestREPL_ClearCommand(t *testing.T) {
	commands := `.clear
.quit`
	_, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	// .clear should not cause an error
	assertNoError(t, err, stderr)
}

// TestREPL_SQLQuery tests executing SQL queries in REPL
func TestREPL_SQLQuery_Select(t *testing.T) {
	commands := `SELECT * FROM simple
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

func TestREPL_SQLQuery_SelectWithWhere(t *testing.T) {
	commands := `SELECT * FROM simple WHERE name = 'John'
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
}

func TestREPL_SQLQuery_Count(t *testing.T) {
	commands := `SELECT COUNT(*) FROM simple
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "rows)")
}

// TestREPL_EmptyInput tests empty input handling
func TestREPL_EmptyInput(t *testing.T) {
	commands := `

.quit`
	_, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	// Empty lines should not cause errors
	assertNoError(t, err, stderr)
}

// TestREPL_InvalidCommand tests invalid command handling
func TestREPL_InvalidCommand(t *testing.T) {
	commands := `.invalidcommand
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	// Invalid commands are treated as SQL, which should error
	_ = err
	_ = stdout
	_ = stderr
	// Just verify it doesn't crash
}

// TestREPL_MultipleCommands tests multiple commands in sequence
func TestREPL_MultipleCommands(t *testing.T) {
	commands := `.tables
SELECT * FROM simple LIMIT 1
\c simple
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "simple")
	assertContains(t, stdout, "rows")
}

// TestREPL_WithJSON tests REPL with JSON input
func TestREPL_WithJSON(t *testing.T) {
	commands := `.tables
SELECT * FROM array LIMIT 2
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("json/array.json"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "array")
}

// TestREPL_WithMultipleFiles tests REPL with multiple input files
func TestREPL_WithMultipleFiles(t *testing.T) {
	commands := `.tables
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("csv/users.csv"))

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "simple")
	assertContains(t, stdout, "users")
}
