package e2e_test

import (
	"testing"
)

func TestMultiFile_TwoCSVs(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT COUNT(*) as count FROM users")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestMultiFile_JoinBetweenFiles(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT u.name, d.department_name FROM users u JOIN departments d ON u.department_id = d.id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Engineering")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Sales")
}

func TestMultiFile_MixedFormatsError(t *testing.T) {
	_, _, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-f", fixture("json/array.json"),
		"-q", "SELECT * FROM simple")

	assertError(t, err)
}

func TestMultiFile_MultipleJSONL(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("jsonl/simple.jsonl"),
		"-f", fixture("jsonl/data.ndjson"),
		"-q", "SELECT COUNT(*) as count FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestMultiFile_QuerySecondFile(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT * FROM departments")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Engineering")
	assertContains(t, stdout, "Sales")
	assertContains(t, stdout, "Marketing")
}

func TestMultiFile_CountFromBothTables(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/users.csv"),
		"-f", fixture("csv/departments.csv"),
		"-q", "SELECT (SELECT COUNT(*) FROM users) as users_count, (SELECT COUNT(*) FROM departments) as depts_count")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}
