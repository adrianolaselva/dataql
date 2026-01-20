#!/bin/bash
# DataQL E2E Tests - DynamoDB (via LocalStack)
# Comprehensive tests for DynamoDB table reading functionality
#
# Test Coverage:
# - Basic table scanning
# - SELECT queries on table data (columns, alias, distinct)
# - WHERE clause filtering (=, <, >, <=, >=, <>, AND, OR, IN, BETWEEN, LIKE)
# - COUNT and aggregation (COUNT, SUM, AVG, MIN, MAX)
# - GROUP BY operations
# - LIMIT and OFFSET functionality
# - ORDER BY operations (ASC, DESC)
# - Export formats (CSV, JSON, JSONL, XML, YAML, Excel, Parquet)
#
# Prerequisites:
# - LocalStack running with DynamoDB service
# - AWS_ENDPOINT_URL set to LocalStack endpoint
# - Test table 'dataql-test-table' with sample data

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$E2E_DIR")"

# Export all variables from .env file
set -a
source "$E2E_DIR/.env"
set +a

DATAQL_BIN="${DATAQL_BIN:-$PROJECT_DIR/dataql}"
PASSED=0
FAILED=0
SKIPPED=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_pass() {
    echo -e "  ${GREEN}[PASS]${NC} $1"
    ((PASSED++)) || true
}

log_fail() {
    echo -e "  ${RED}[FAIL]${NC} $1"
    if [ -n "$2" ]; then
        echo -e "         ${RED}Error: $2${NC}"
    fi
    ((FAILED++)) || true
}

log_skip() {
    echo -e "  ${YELLOW}[SKIP]${NC} $1"
    ((SKIPPED++)) || true
}

log_info() {
    echo -e "  ${YELLOW}[INFO]${NC} $1"
}

log_section() {
    echo ""
    echo -e "${CYAN}── $1 ──${NC}"
}

# Check if LocalStack DynamoDB is available
check_localstack() {
    log_info "Checking LocalStack DynamoDB availability..."

    if ! curl -s "${AWS_ENDPOINT_URL}/_localstack/health" | grep -q "running"; then
        echo -e "${RED}LocalStack is not running. Skipping DynamoDB tests.${NC}"
        return 1
    fi

    # Verify table exists
    if ! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url="${AWS_ENDPOINT_URL}" dynamodb describe-table --table-name dataql-test-table > /dev/null 2>&1; then
        echo -e "${RED}DynamoDB table 'dataql-test-table' not found. Skipping DynamoDB tests.${NC}"
        return 1
    fi

    log_pass "LocalStack DynamoDB is available"
    return 0
}

# ==============================================================================
# BASIC TABLE READING TESTS
# ==============================================================================

test_dynamodb_basic_scan() {
    log_info "Test: Basic scan from DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT * FROM dataql_test_table" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob"; then
        log_pass "Basic DynamoDB scan works"
    else
        log_fail "Basic DynamoDB scan failed" "$result"
    fi
}

test_dynamodb_select_columns() {
    log_info "Test: SELECT specific columns from DynamoDB"
    result=$($DATAQL_BIN run -q "SELECT name, email FROM dataql_test_table" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "alice@example.com"; then
        log_pass "SELECT specific columns from DynamoDB works"
    else
        log_fail "SELECT specific columns from DynamoDB failed" "$result"
    fi
}

test_dynamodb_where_clause() {
    log_info "Test: WHERE clause on DynamoDB data"
    result=$($DATAQL_BIN run -q "SELECT name FROM dataql_test_table WHERE age > 30" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "Bob" && echo "$result" | grep -q "Charlie"; then
        log_pass "WHERE clause on DynamoDB works"
    else
        log_fail "WHERE clause on DynamoDB failed" "$result"
    fi
}

test_dynamodb_where_equals() {
    log_info "Test: WHERE with equals condition"
    result=$($DATAQL_BIN run -q "SELECT name, age FROM dataql_test_table WHERE name = 'Alice'" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "28"; then
        log_pass "WHERE equals on DynamoDB works"
    else
        log_fail "WHERE equals on DynamoDB failed" "$result"
    fi
}

test_dynamodb_where_less_than() {
    log_info "Test: WHERE with less than condition"
    result=$($DATAQL_BIN run -q "SELECT name FROM dataql_test_table WHERE age < 30" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Eve"; then
        log_pass "WHERE less than on DynamoDB works"
    else
        log_fail "WHERE less than on DynamoDB failed" "$result"
    fi
}

test_dynamodb_where_like() {
    log_info "Test: WHERE with LIKE condition"
    result=$($DATAQL_BIN run -q "SELECT name, email FROM dataql_test_table WHERE email LIKE '%@example.com'" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "example.com"; then
        log_pass "WHERE LIKE on DynamoDB works"
    else
        log_fail "WHERE LIKE on DynamoDB failed" "$result"
    fi
}

test_dynamodb_select_alias() {
    log_info "Test: SELECT with alias"
    result=$($DATAQL_BIN run -q "SELECT name AS user_name, email AS user_email FROM dataql_test_table" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q -i "user_name\|Alice"; then
        log_pass "SELECT with alias on DynamoDB works"
    else
        log_fail "SELECT with alias on DynamoDB failed" "$result"
    fi
}

test_dynamodb_select_distinct() {
    log_info "Test: SELECT DISTINCT"
    # All names are distinct, so count should be 5
    result=$($DATAQL_BIN run -q "SELECT DISTINCT name FROM dataql_test_table" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob"; then
        log_pass "SELECT DISTINCT on DynamoDB works"
    else
        log_fail "SELECT DISTINCT on DynamoDB failed" "$result"
    fi
}

test_dynamodb_where_and() {
    log_info "Test: WHERE with AND condition"
    result=$($DATAQL_BIN run -q "SELECT name FROM dataql_test_table WHERE age > 25 AND age < 40" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    # Should include Alice (28), Bob (35), Diana (31) but not Eve (25) or Charlie (42)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob" && ! echo "$result" | grep -q "Eve"; then
        log_pass "WHERE AND on DynamoDB works"
    else
        log_fail "WHERE AND on DynamoDB failed" "$result"
    fi
}

test_dynamodb_where_or() {
    log_info "Test: WHERE with OR condition"
    result=$($DATAQL_BIN run -q "SELECT name FROM dataql_test_table WHERE name = 'Alice' OR name = 'Eve'" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Eve"; then
        log_pass "WHERE OR on DynamoDB works"
    else
        log_fail "WHERE OR on DynamoDB failed" "$result"
    fi
}

test_dynamodb_where_in() {
    log_info "Test: WHERE with IN clause"
    result=$($DATAQL_BIN run -q "SELECT name FROM dataql_test_table WHERE name IN ('Alice', 'Bob')" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob" && ! echo "$result" | grep -q "Charlie"; then
        log_pass "WHERE IN on DynamoDB works"
    else
        log_fail "WHERE IN on DynamoDB failed" "$result"
    fi
}

test_dynamodb_where_between() {
    log_info "Test: WHERE with BETWEEN condition"
    result=$($DATAQL_BIN run -q "SELECT name, age FROM dataql_test_table WHERE age BETWEEN 25 AND 35" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    # Should include Alice (28), Bob (35), Diana (31), Eve (25) but not Charlie (42)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Eve" && ! echo "$result" | grep -q "Charlie"; then
        log_pass "WHERE BETWEEN on DynamoDB works"
    else
        log_fail "WHERE BETWEEN on DynamoDB failed" "$result"
    fi
}

test_dynamodb_where_not_equal() {
    log_info "Test: WHERE with NOT EQUAL (<>) condition"
    result=$($DATAQL_BIN run -q "SELECT name FROM dataql_test_table WHERE name <> 'Alice'" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if ! echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob"; then
        log_pass "WHERE NOT EQUAL on DynamoDB works"
    else
        log_fail "WHERE NOT EQUAL on DynamoDB failed" "$result"
    fi
}

test_dynamodb_where_greater_equal() {
    log_info "Test: WHERE with >= condition"
    result=$($DATAQL_BIN run -q "SELECT name FROM dataql_test_table WHERE age >= 35" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    # Should include Bob (35) and Charlie (42)
    if echo "$result" | grep -q "Bob" && echo "$result" | grep -q "Charlie"; then
        log_pass "WHERE >= on DynamoDB works"
    else
        log_fail "WHERE >= on DynamoDB failed" "$result"
    fi
}

test_dynamodb_where_less_equal() {
    log_info "Test: WHERE with <= condition"
    result=$($DATAQL_BIN run -q "SELECT name FROM dataql_test_table WHERE age <= 28" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    # Should include Alice (28) and Eve (25)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Eve"; then
        log_pass "WHERE <= on DynamoDB works"
    else
        log_fail "WHERE <= on DynamoDB failed" "$result"
    fi
}

# ==============================================================================
# COUNT AND AGGREGATION TESTS
# ==============================================================================

test_dynamodb_count() {
    log_info "Test: COUNT on DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT COUNT(*) AS total FROM dataql_test_table" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "5"; then
        log_pass "COUNT on DynamoDB works"
    else
        log_fail "COUNT on DynamoDB failed" "$result"
    fi
}

test_dynamodb_count_with_where() {
    log_info "Test: COUNT with WHERE clause"
    result=$($DATAQL_BIN run -q "SELECT COUNT(*) AS over_30 FROM dataql_test_table WHERE age > 30" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "3"; then
        log_pass "COUNT with WHERE on DynamoDB works"
    else
        log_fail "COUNT with WHERE on DynamoDB failed" "$result"
    fi
}

test_dynamodb_sum() {
    log_info "Test: SUM on DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT SUM(age) AS total_age FROM dataql_test_table" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    # 28 + 35 + 42 + 31 + 25 = 161
    if echo "$result" | grep -q "161"; then
        log_pass "SUM on DynamoDB works"
    else
        log_fail "SUM on DynamoDB failed" "$result"
    fi
}

test_dynamodb_avg() {
    log_info "Test: AVG on DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT AVG(age) AS avg_age FROM dataql_test_table" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    # Average of 28, 35, 42, 31, 25 = 32.2
    if echo "$result" | grep -q "32"; then
        log_pass "AVG on DynamoDB works"
    else
        log_fail "AVG on DynamoDB failed" "$result"
    fi
}

test_dynamodb_min_max() {
    log_info "Test: MIN and MAX on DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT MIN(age) AS min_age, MAX(age) AS max_age FROM dataql_test_table" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "25" && echo "$result" | grep -q "42"; then
        log_pass "MIN/MAX on DynamoDB works"
    else
        log_fail "MIN/MAX on DynamoDB failed" "$result"
    fi
}

test_dynamodb_group_concat() {
    log_info "Test: GROUP_CONCAT on DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT GROUP_CONCAT(name) AS names FROM dataql_test_table" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob"; then
        log_pass "GROUP_CONCAT on DynamoDB works"
    else
        log_fail "GROUP_CONCAT on DynamoDB failed" "$result"
    fi
}

# ==============================================================================
# LIMIT TESTS
# ==============================================================================

test_dynamodb_limit() {
    log_info "Test: LIMIT on DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT * FROM dataql_test_table LIMIT 2" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    count=$(echo "$result" | grep -c "@example.com" || true)
    if [ "$count" -eq 2 ]; then
        log_pass "LIMIT on DynamoDB works"
    else
        log_fail "LIMIT on DynamoDB failed, expected 2 rows, got $count"
    fi
}

test_dynamodb_limit_one() {
    log_info "Test: LIMIT 1 on DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT name FROM dataql_test_table LIMIT 1" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    count=$(echo "$result" | grep -c -E "(Alice|Bob|Charlie|Diana|Eve)" || true)
    if [ "$count" -eq 1 ]; then
        log_pass "LIMIT 1 on DynamoDB works"
    else
        log_fail "LIMIT 1 on DynamoDB failed, expected 1 row, got $count"
    fi
}

test_dynamodb_limit_offset() {
    log_info "Test: LIMIT with OFFSET on DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT name FROM dataql_test_table ORDER BY name ASC LIMIT 2 OFFSET 1" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    # When sorted by name ASC: Alice, Bob, Charlie, Diana, Eve
    # OFFSET 1, LIMIT 2 should return Bob, Charlie (skip Alice)
    if echo "$result" | grep -q "Bob" && ! echo "$result" | grep -q "Alice"; then
        log_pass "LIMIT with OFFSET on DynamoDB works"
    else
        log_fail "LIMIT with OFFSET on DynamoDB failed" "$result"
    fi
}

# ==============================================================================
# ORDER BY TESTS
# ==============================================================================

test_dynamodb_order_by_asc() {
    log_info "Test: ORDER BY ASC on DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT name, age FROM dataql_test_table ORDER BY age ASC" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    # Eve (25) should appear before Charlie (42)
    eve_pos=$(echo "$result" | grep -n "Eve" | head -1 | cut -d: -f1)
    charlie_pos=$(echo "$result" | grep -n "Charlie" | head -1 | cut -d: -f1)
    if [ -n "$eve_pos" ] && [ -n "$charlie_pos" ] && [ "$eve_pos" -lt "$charlie_pos" ]; then
        log_pass "ORDER BY ASC on DynamoDB works"
    else
        log_fail "ORDER BY ASC on DynamoDB failed" "$result"
    fi
}

test_dynamodb_order_by_desc() {
    log_info "Test: ORDER BY DESC on DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT name, age FROM dataql_test_table ORDER BY age DESC" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    # Charlie (42) should appear before Eve (25)
    charlie_pos=$(echo "$result" | grep -n "Charlie" | head -1 | cut -d: -f1)
    eve_pos=$(echo "$result" | grep -n "Eve" | head -1 | cut -d: -f1)
    if [ -n "$eve_pos" ] && [ -n "$charlie_pos" ] && [ "$charlie_pos" -lt "$eve_pos" ]; then
        log_pass "ORDER BY DESC on DynamoDB works"
    else
        log_fail "ORDER BY DESC on DynamoDB failed" "$result"
    fi
}

test_dynamodb_order_by_name() {
    log_info "Test: ORDER BY name on DynamoDB table"
    result=$($DATAQL_BIN run -q "SELECT name FROM dataql_test_table ORDER BY name ASC" -f "$DATAQL_TEST_DYNAMODB_URL" 2>&1)
    # Alice should appear before Eve
    alice_pos=$(echo "$result" | grep -n "Alice" | head -1 | cut -d: -f1)
    eve_pos=$(echo "$result" | grep -n "Eve" | head -1 | cut -d: -f1)
    if [ -n "$alice_pos" ] && [ -n "$eve_pos" ] && [ "$alice_pos" -lt "$eve_pos" ]; then
        log_pass "ORDER BY name on DynamoDB works"
    else
        log_fail "ORDER BY name on DynamoDB failed" "$result"
    fi
}

# ==============================================================================
# EXPORT FORMAT TESTS
# ==============================================================================

test_dynamodb_export_csv() {
    log_info "Test: Export DynamoDB data to CSV"
    output_file="/tmp/dynamodb_export_$$.csv"
    $DATAQL_BIN run -q "SELECT name, email FROM dataql_test_table" -f "$DATAQL_TEST_DYNAMODB_URL" -e "$output_file" -t csv 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file"; then
        log_pass "Export DynamoDB data to CSV works"
        rm -f "$output_file"
    else
        log_fail "Export DynamoDB data to CSV failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_dynamodb_export_jsonl() {
    log_info "Test: Export DynamoDB data to JSONL"
    output_file="/tmp/dynamodb_export_$$.jsonl"
    $DATAQL_BIN run -q "SELECT name, email FROM dataql_test_table LIMIT 2" -f "$DATAQL_TEST_DYNAMODB_URL" -e "$output_file" -t jsonl 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "example.com" "$output_file"; then
        log_pass "Export DynamoDB data to JSONL works"
        rm -f "$output_file"
    else
        log_fail "Export DynamoDB data to JSONL failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_dynamodb_export_json() {
    log_info "Test: Export DynamoDB data to JSON"
    output_file="/tmp/dynamodb_export_$$.json"
    $DATAQL_BIN run -q "SELECT name, email FROM dataql_test_table LIMIT 2" -f "$DATAQL_TEST_DYNAMODB_URL" -e "$output_file" -t json 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "example.com" "$output_file"; then
        log_pass "Export DynamoDB data to JSON works"
        rm -f "$output_file"
    else
        log_fail "Export DynamoDB data to JSON failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_dynamodb_export_xml() {
    log_info "Test: Export DynamoDB data to XML"
    output_file="/tmp/dynamodb_export_$$.xml"
    $DATAQL_BIN run -q "SELECT name, email FROM dataql_test_table LIMIT 2" -f "$DATAQL_TEST_DYNAMODB_URL" -e "$output_file" -t xml 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file"; then
        log_pass "Export DynamoDB data to XML works"
        rm -f "$output_file"
    else
        log_fail "Export DynamoDB data to XML failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_dynamodb_export_yaml() {
    log_info "Test: Export DynamoDB data to YAML"
    output_file="/tmp/dynamodb_export_$$.yaml"
    $DATAQL_BIN run -q "SELECT name, email FROM dataql_test_table LIMIT 2" -f "$DATAQL_TEST_DYNAMODB_URL" -e "$output_file" -t yaml 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file"; then
        log_pass "Export DynamoDB data to YAML works"
        rm -f "$output_file"
    else
        log_fail "Export DynamoDB data to YAML failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_dynamodb_export_excel() {
    log_info "Test: Export DynamoDB data to Excel"
    output_file="/tmp/dynamodb_export_$$.xlsx"
    $DATAQL_BIN run -q "SELECT name, email FROM dataql_test_table LIMIT 2" -f "$DATAQL_TEST_DYNAMODB_URL" -e "$output_file" -t excel 2>&1 > /dev/null
    if [ -f "$output_file" ] && [ -s "$output_file" ]; then
        log_pass "Export DynamoDB data to Excel works"
        rm -f "$output_file"
    else
        log_fail "Export DynamoDB data to Excel failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_dynamodb_export_parquet() {
    log_info "Test: Export DynamoDB data to Parquet"
    output_file="/tmp/dynamodb_export_$$.parquet"
    $DATAQL_BIN run -q "SELECT name, email FROM dataql_test_table LIMIT 2" -f "$DATAQL_TEST_DYNAMODB_URL" -e "$output_file" -t parquet 2>&1 > /dev/null
    if [ -f "$output_file" ] && [ -s "$output_file" ]; then
        log_pass "Export DynamoDB data to Parquet works"
        rm -f "$output_file"
    else
        log_fail "Export DynamoDB data to Parquet failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

# ==============================================================================
# CUSTOM TABLE NAME TESTS
# ==============================================================================

test_dynamodb_custom_collection() {
    log_info "Test: Custom table name with -c flag"
    result=$($DATAQL_BIN run -q "SELECT * FROM custom_table" -f "$DATAQL_TEST_DYNAMODB_URL" -c custom_table 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob"; then
        log_pass "Custom table name with -c flag works"
    else
        log_fail "Custom table name with -c flag failed" "$result"
    fi
}

# ==============================================================================
# RUN ALL TESTS
# ==============================================================================

echo "======================================"
echo "DataQL E2E Tests - DynamoDB (LocalStack)"
echo "======================================"

log_section "Prerequisites Check"
if ! check_localstack; then
    echo ""
    echo "======================================"
    echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}, ${YELLOW}1 skipped${NC}"
    echo "======================================"
    exit 0
fi

log_section "Basic SELECT Tests"
test_dynamodb_basic_scan
test_dynamodb_select_columns
test_dynamodb_select_alias
test_dynamodb_select_distinct

log_section "WHERE Clause Tests"
test_dynamodb_where_equals
test_dynamodb_where_clause
test_dynamodb_where_less_than
test_dynamodb_where_greater_equal
test_dynamodb_where_less_equal
test_dynamodb_where_not_equal
test_dynamodb_where_and
test_dynamodb_where_or
test_dynamodb_where_in
test_dynamodb_where_between
test_dynamodb_where_like

log_section "Aggregation Tests"
test_dynamodb_count
test_dynamodb_count_with_where
test_dynamodb_sum
test_dynamodb_avg
test_dynamodb_min_max
test_dynamodb_group_concat

log_section "Limit/Offset Tests"
test_dynamodb_limit
test_dynamodb_limit_one
test_dynamodb_limit_offset

log_section "Order By Tests"
test_dynamodb_order_by_asc
test_dynamodb_order_by_desc
test_dynamodb_order_by_name

log_section "Export Format Tests"
test_dynamodb_export_csv
test_dynamodb_export_jsonl
test_dynamodb_export_json
test_dynamodb_export_xml
test_dynamodb_export_yaml
test_dynamodb_export_excel
test_dynamodb_export_parquet

log_section "Custom Table Name Tests"
test_dynamodb_custom_collection

echo ""
echo "======================================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}, ${YELLOW}$SKIPPED skipped${NC}"
echo "======================================"

exit $FAILED
