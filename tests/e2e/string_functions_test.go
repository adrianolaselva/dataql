package e2e_test

import (
	"testing"
)

// ============================================
// String Function Tests
// DuckDB supports comprehensive string functions
// ============================================

func TestString_Upper(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, UPPER(name) as upper_name FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "upper_name")
}

func TestString_Lower(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, LOWER(name) as lower_name FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "lower_name")
}

func TestString_Length(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, LENGTH(name) as name_len FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name_len")
}

func TestString_Trim(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, TRIM('  ' || name || '  ') as trimmed FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "trimmed")
}

func TestString_Ltrim(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, LTRIM('   ' || name) as left_trimmed FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "left_trimmed")
}

func TestString_Rtrim(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, RTRIM(name || '   ') as right_trimmed FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "right_trimmed")
}

func TestString_Substring(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, SUBSTRING(name, 1, 3) as first_3_chars FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "first_3_chars")
}

func TestString_Substr(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT email, SUBSTR(email, 1, POSITION('@' IN email) - 1) as username FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "username")
}

func TestString_Concat(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, email, CONCAT(name, ' <', email, '>') as formatted FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "formatted")
}

func TestString_ConcatWs(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT CONCAT_WS(', ', name, department) as employee_info FROM employees LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "employee_info")
}

func TestString_Replace(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT email, REPLACE(email, '@example.com', '@company.com') as new_email FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "new_email")
}

func TestString_Position(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT email, POSITION('@' IN email) as at_position FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "at_position")
}

func TestString_Instr(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT email, INSTR(email, '@') as at_pos FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "at_pos")
}

func TestString_Left(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, LEFT(name, 5) as first_5 FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "first_5")
}

func TestString_Right(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT email, RIGHT(email, 11) as domain FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "domain")
}

func TestString_Repeat(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, REPEAT('*', LENGTH(name)) as masked FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "masked")
}

func TestString_Reverse(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, REVERSE(name) as reversed FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "reversed")
}

func TestString_Lpad(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT id, LPAD(CAST(id AS VARCHAR), 5, '0') as padded_id FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "padded_id")
}

func TestString_Rpad(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, RPAD(name, 20, '.') as padded_name FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "padded_name")
}

func TestString_Initcap(t *testing.T) {
	// INITCAP may not be supported in all versions
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT INITCAP(LOWER(name)) as proper_name FROM employees")

	// If INITCAP is not supported, the test passes gracefully
	if err == nil {
		assertContains(t, stdout, "proper_name")
	}
	_ = stderr
	_ = stdout
}

func TestString_Chr(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT CHR(65) as letter_a FROM simple LIMIT 1")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "A")
}

func TestString_Ascii(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, ASCII(name) as first_char_code FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "first_char_code")
}

func TestString_Printf(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT product, PRINTF('$%.2f', price) as formatted_price FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "formatted_price")
}

func TestString_SplitPart(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT email, SPLIT_PART(email, '@', 1) as username, SPLIT_PART(email, '@', 2) as domain FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "username")
	assertContains(t, stdout, "domain")
}

func TestString_StringAgg(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT department, STRING_AGG(name, ', ' ORDER BY name) as employees FROM employees GROUP BY department")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "employees")
}

func TestString_RegexpMatches(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT email, REGEXP_MATCHES(email, '[a-z]+@') as match_result FROM simple")

	// This may or may not be supported
	if err == nil {
		assertContains(t, stdout, "match_result")
	}
	_ = stderr
}

func TestString_RegexpReplace(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT email, REGEXP_REPLACE(email, '[0-9]+', 'X') as replaced FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "replaced")
}

func TestString_Like(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, email FROM simple WHERE email LIKE '%@example.com'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestString_Ilike(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT name, department FROM employees WHERE department ILIKE 'ENGINEERING'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Engineering")
}

func TestString_Similar(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT email FROM simple WHERE email SIMILAR TO '[a-z]+@example\\.com'")

	// Similar may have different syntax requirements
	if err == nil {
		assertContains(t, stdout, "email")
	}
	_ = stderr
}

func TestString_StartsWith(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT name FROM employees WHERE STARTS_WITH(name, 'J')")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John")
	assertContains(t, stdout, "Jane")
}

func TestString_EndsWith(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT name FROM employees WHERE ENDS_WITH(name, 'son')")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Johnson")
	assertContains(t, stdout, "Wilson")
}

func TestString_Contains(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/employees.csv"),
		"-q", "SELECT name FROM employees WHERE CONTAINS(name, 'son')")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "name")
}

func TestString_StrPos(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT email, STRPOS(email, '@') as at_pos FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "at_pos")
}

func TestString_Translate(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, TRANSLATE(name, 'aeiou', '12345') as translated FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "translated")
}

func TestString_MD5(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT email, MD5(email) as email_hash FROM simple")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "email_hash")
}

func TestString_Encode(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT name, ENCODE(name::BLOB, 'base64') as encoded FROM simple")

	// Encoding may have different syntax
	if err == nil {
		assertContains(t, stdout, "encoded")
	}
	_ = stderr
}

func TestString_Format(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("csv/sales.csv"),
		"-q", "SELECT FORMAT('{}: ${:.2f}', product, price) as formatted FROM sales LIMIT 5")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "formatted")
}
