package e2e_test

import (
	"testing"
)

// ============================================
// JSON Function Tests
// DuckDB supports JSON extraction and manipulation
// ============================================

func TestJSON_BasicQuery(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/complex_nested.json"),
		"-q", "SELECT * FROM complex_nested LIMIT 3")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
}

func TestJSON_NestedFieldAccess(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/complex_nested.json"),
		"-q", "SELECT id, user_name, user_email FROM complex_nested")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "user_name")
}

func TestJSON_DeepNestedAccess(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/complex_nested.json"),
		"-q", "SELECT id, user_address_city, user_address_country FROM complex_nested")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "user_address_city")
}

func TestJSON_Count(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/complex_nested.json"),
		"-q", "SELECT COUNT(*) as total FROM complex_nested")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestJSON_FilterOnNestedField(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/complex_nested.json"),
		"-q", "SELECT id, user_name FROM complex_nested WHERE user_address_city = 'New York'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "John Doe")
}

func TestJSON_OrderByNestedField(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/complex_nested.json"),
		"-q", "SELECT id, user_name, user_address_city FROM complex_nested ORDER BY user_address_city")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "user_address_city")
}

func TestJSON_GroupByNestedField(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/complex_nested.json"),
		"-q", "SELECT user_address_country, COUNT(*) as cnt FROM complex_nested GROUP BY user_address_country")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "USA")
}

func TestJSON_ProductVariants(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/products_with_variants.json"),
		"-q", "SELECT sku, name, brand FROM products_with_variants")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "sku")
	assertContains(t, stdout, "name")
}

func TestJSON_ProductSpecs(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/products_with_variants.json"),
		"-q", "SELECT sku, specs_cpu, specs_ram, specs_storage FROM products_with_variants")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "specs_cpu")
}

func TestJSON_FilterByBoolean(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/products_with_variants.json"),
		"-q", "SELECT sku, name FROM products_with_variants WHERE active = true")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "LAPTOP-001")
	assertContains(t, stdout, "PHONE-001")
}

func TestJSON_AggregateNumeric(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/products_with_variants.json"),
		"-q", "SELECT AVG(rating) as avg_rating, MAX(rating) as max_rating FROM products_with_variants")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "avg_rating")
}

func TestJSON_EventsTimeline(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/events_timeline.json"),
		"-q", "SELECT event_id, type, user_id, timestamp FROM events_timeline ORDER BY event_id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "type")
}

func TestJSON_EventsFilterByType(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/events_timeline.json"),
		"-q", "SELECT * FROM events_timeline WHERE type = 'purchase'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "purchase")
}

func TestJSON_EventsCountByType(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/events_timeline.json"),
		"-q", "SELECT type, COUNT(*) as cnt FROM events_timeline GROUP BY type ORDER BY cnt DESC")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "login")
	assertContains(t, stdout, "purchase")
}

func TestJSON_EventsNestedData(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/events_timeline.json"),
		"-q", "SELECT event_id, type, data_amount FROM events_timeline WHERE type = 'purchase'")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "data_amount")
}

func TestJSON_MixedTypes(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/mixed_types.json"),
		"-q", "SELECT * FROM mixed_types ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "id")
}

func TestJSON_NullValues(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/mixed_types.json"),
		"-q", "SELECT id, active FROM mixed_types WHERE active IS NULL")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "3")
}

func TestJSON_ExportToJSONL(t *testing.T) {
	exportPath := tempFile(t, "export_json.jsonl")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/complex_nested.json"),
		"-q", "SELECT id, user_name, user_email FROM complex_nested ORDER BY id",
		"-e", exportPath,
		"-t", "jsonl")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	content := readFile(t, exportPath)
	assertContains(t, content, `"id"`)
	assertContains(t, content, `"user_name"`)
}

func TestJSON_ExportToCSV(t *testing.T) {
	exportPath := tempFile(t, "export_json.csv")

	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/complex_nested.json"),
		"-q", "SELECT id, user_name, user_email FROM complex_nested ORDER BY id",
		"-e", exportPath,
		"-t", "csv")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "successfully exported")

	content := readFile(t, exportPath)
	assertContains(t, content, "id,user_name,user_email")
}

func TestJSON_LimitOffset(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/events_timeline.json"),
		"-q", "SELECT event_id, type FROM events_timeline ORDER BY event_id LIMIT 3 OFFSET 2")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "event_id")
}

func TestJSON_Distinct(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/events_timeline.json"),
		"-q", "SELECT DISTINCT type FROM events_timeline ORDER BY type")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "error")
	assertContains(t, stdout, "login")
	assertContains(t, stdout, "logout")
	assertContains(t, stdout, "purchase")
}

func TestJSON_ComplexWhere(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/events_timeline.json"),
		"-q", "SELECT * FROM events_timeline WHERE type IN ('login', 'logout') AND user_id = 101")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "login")
}

func TestJSON_JoinWithCSV(t *testing.T) {
	// Note: Mixed file formats (JSON + CSV) are not supported
	// This test documents the expected behavior
	_, stderr, err := runDataQL(t, "run",
		"-f", fixture("json/events_timeline.json"),
		"-f", fixture("csv/simple.csv"),
		"-q", "SELECT * FROM events_timeline LIMIT 5")

	// Mixed formats should fail with a clear error message
	assertError(t, err)
	assertContains(t, stderr, "mixed file formats not supported")
}
