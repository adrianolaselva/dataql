#!/bin/bash
# DataQL E2E Tests - DynamoDB (via LocalStack)
# Comprehensive tests for DynamoDB table reading functionality
#
# Test Coverage:
# - Basic table scanning
# - SELECT queries on table data
# - WHERE clause filtering
# - COUNT and aggregation
# - LIMIT functionality
# - ORDER BY operations
# - Export formats (CSV, JSON, JSONL)
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

log_section "Basic Table Reading Tests"
test_dynamodb_basic_scan
test_dynamodb_select_columns
test_dynamodb_where_clause
test_dynamodb_where_equals
test_dynamodb_where_less_than
test_dynamodb_where_like

log_section "Count and Aggregation Tests"
test_dynamodb_count
test_dynamodb_count_with_where
test_dynamodb_sum
test_dynamodb_avg
test_dynamodb_min_max

log_section "Limit Tests"
test_dynamodb_limit
test_dynamodb_limit_one

log_section "Order By Tests"
test_dynamodb_order_by_asc
test_dynamodb_order_by_desc
test_dynamodb_order_by_name

log_section "Export Format Tests"
test_dynamodb_export_csv
test_dynamodb_export_jsonl
test_dynamodb_export_json

log_section "Custom Table Name Tests"
test_dynamodb_custom_collection

echo ""
echo "======================================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}, ${YELLOW}$SKIPPED skipped${NC}"
echo "======================================"

exit $FAILED
