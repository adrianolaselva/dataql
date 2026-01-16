package e2e_test

import (
	"testing"
)

func TestCSV_BasicWithComma(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT COUNT(*) as count FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestCSV_WithSemicolonDelimiter(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/semicolon.csv"),
		"-d", ";",
		"-q", "SELECT COUNT(*) as count FROM semicolon")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestCSV_QuotedFields(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/quoted.csv"),
		"-q", "SELECT name FROM quoted WHERE id = '1'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Smith, John")
}

func TestCSV_UTF8Characters(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/utf8.csv"),
		"-q", "SELECT name, city FROM utf8 WHERE id = '1'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "José")
	assertContains(t, stdout, "São Paulo")
}

func TestCSV_EmptyFile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/empty.csv"),
		"-q", "SELECT COUNT(*) as count FROM empty")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "0")
}

func TestCSV_WithLineLimit(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/large.csv"),
		"-l", "10",
		"-q", "SELECT COUNT(*) as count FROM large")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "10")
}

func TestCSV_WithCollection(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-c", "my_custom_table",
		"-q", "SELECT COUNT(*) as count FROM my_custom_table")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestCSV_FileNotFound(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/nonexistent.csv"),
		"-q", "SELECT * FROM nonexistent")

	assertError(t, err)
}

func TestCSV_SelectSpecificColumns(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, name FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
	assertContains(t, stdout, "name")
	assertContains(t, stdout, "John")
}
