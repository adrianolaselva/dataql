package e2e_test

import (
	"strings"
	"testing"
)

// ============================================
// Truncate Flag Tests (Issue #35)
// ============================================

func TestOutput_TruncateFlag_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"--truncate", "5")

	assertNoError(t, err, stderr)
	// Emails should be truncated with "..."
	assertContains(t, stdout, "...")
	assertContains(t, stdout, "rows)")
}

func TestOutput_TruncateFlag_ZeroDisabled(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"--truncate", "0")

	assertNoError(t, err, stderr)
	// Full email should be shown
	assertContains(t, stdout, "john@example.com")
	assertContains(t, stdout, "rows)")
}

func TestOutput_TruncateFlag_ShortFlag(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"-T", "5")

	assertNoError(t, err, stderr)
	// Emails should be truncated with "..."
	assertContains(t, stdout, "...")
}

func TestOutput_TruncateFlag_LongValue(t *testing.T) {
	// Test with a longer truncate value that still shows data but truncates emails
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"--truncate", "10")

	assertNoError(t, err, stderr)
	// Names should still be complete (under 10 chars)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "Jane")
	assertContains(t, stdout, "Bob")
	// But emails should be truncated
	if !strings.Contains(stdout, "...") {
		t.Errorf("expected truncated emails with ..., got: %s", stdout)
	}
}

// ============================================
// Vertical Display Flag Tests (Issue #35)
// ============================================

func TestOutput_VerticalFlag_Basic(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple LIMIT 1",
		"--vertical")

	assertNoError(t, err, stderr)
	// Should show row header
	assertContains(t, stdout, "***")
	assertContains(t, stdout, "1. row")
	// Should show key-value pairs
	assertContains(t, stdout, "id:")
	assertContains(t, stdout, "name:")
	assertContains(t, stdout, "email:")
}

func TestOutput_VerticalFlag_ShortFlag(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple LIMIT 1",
		"-G")

	assertNoError(t, err, stderr)
	// Should show vertical format
	assertContains(t, stdout, "***")
	assertContains(t, stdout, "1. row")
}

func TestOutput_VerticalFlag_MultipleRows(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple",
		"--vertical")

	assertNoError(t, err, stderr)
	// Should show all three row headers
	assertContains(t, stdout, "1. row")
	assertContains(t, stdout, "2. row")
	assertContains(t, stdout, "3. row")
	// Should show row count at end
	assertContains(t, stdout, "(3 rows)")
}

func TestOutput_VerticalFlag_WithTruncate(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple LIMIT 1",
		"--vertical",
		"--truncate", "8")

	assertNoError(t, err, stderr)
	// Should be vertical format
	assertContains(t, stdout, "***")
	assertContains(t, stdout, "1. row")
	// Email should be truncated
	assertContains(t, stdout, "...")
}

func TestOutput_VerticalFlag_JSON(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/people.json"),
		"-q", "SELECT * FROM people LIMIT 1",
		"--vertical")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "***")
	assertContains(t, stdout, "1. row")
}

// ============================================
// REPL Output Format Commands Tests
// ============================================

func TestOutput_REPL_TruncateCommand(t *testing.T) {
	commands := `.truncate 5
SELECT * FROM simple LIMIT 1
.truncate
.truncate 0
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	// Should show truncated output
	assertContains(t, stdout, "...")
	// Should show current truncate setting
	assertContains(t, stdout, "5 characters")
}

func TestOutput_REPL_VerticalCommand(t *testing.T) {
	commands := `.vertical on
SELECT * FROM simple LIMIT 1
.vertical
.vertical off
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	// Should show vertical format
	assertContains(t, stdout, "***")
	assertContains(t, stdout, "1. row")
	// Should show status
	assertContains(t, stdout, "Vertical display")
}

func TestOutput_REPL_HelpShowsNewCommands(t *testing.T) {
	commands := `.help
.quit`
	stdout, stderr, err := runDataQLWithStdin(t, commands, "run",
		"-f", fixture("csv/simple.csv"))

	assertNoError(t, err, stderr)
	// Help should mention new commands
	assertContains(t, stdout, ".truncate")
	assertContains(t, stdout, ".vertical")
}

// ============================================
// Edge Cases
// ============================================

func TestOutput_TruncateFlag_VerySmall(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple LIMIT 1",
		"--truncate", "4")

	assertNoError(t, err, stderr)
	// Even small values should work (min 4 chars: 1 char + "...")
	assertContains(t, stdout, "...")
}

func TestOutput_VerticalFlag_EmptyResult(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM simple WHERE id > 100",
		"--vertical")

	assertNoError(t, err, stderr)
	// Should show 0 rows
	assertContains(t, stdout, "(0 rows)")
	// Should NOT have row headers
	if strings.Contains(stdout, "1. row") {
		t.Errorf("expected no rows, but found row header in: %s", stdout)
	}
}

func TestOutput_VerticalFlag_ManyColumns(t *testing.T) {
	// Test with a file that has more columns
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/dates_data.csv"),
		"-q", "SELECT * FROM dates_data LIMIT 1",
		"--vertical")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "***")
	assertContains(t, stdout, "1. row")
	// Should show all column names
	assertContains(t, stdout, "id:")
	assertContains(t, stdout, "event_name:")
	assertContains(t, stdout, "event_date:")
}
