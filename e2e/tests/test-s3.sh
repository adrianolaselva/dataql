#!/bin/bash
# DataQL E2E Tests - S3 (via LocalStack)
# Comprehensive tests for S3 file reading functionality
#
# Test Coverage:
# - CSV file reading from S3
# - JSON file reading from S3
# - JSONL file reading from S3
# - SELECT queries on S3 data
# - WHERE clause filtering
# - Export formats
#
# Prerequisites:
# - LocalStack running with S3 service
# - AWS_ENDPOINT_URL set to LocalStack endpoint
# - Test fixtures uploaded to s3://dataql-test-bucket/fixtures/

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

# Check if LocalStack S3 is available
check_localstack() {
    log_info "Checking LocalStack S3 availability..."

    if ! curl -s "${AWS_ENDPOINT_URL}/_localstack/health" | grep -q "running"; then
        echo -e "${RED}LocalStack is not running. Skipping S3 tests.${NC}"
        return 1
    fi

    # Verify bucket exists
    if ! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url="${AWS_ENDPOINT_URL}" s3 ls s3://dataql-test-bucket/ > /dev/null 2>&1; then
        echo -e "${RED}S3 bucket 'dataql-test-bucket' not found. Skipping S3 tests.${NC}"
        return 1
    fi

    log_pass "LocalStack S3 is available"
    return 0
}

# ==============================================================================
# CSV FILE TESTS
# ==============================================================================

test_s3_csv_basic_read() {
    log_info "Test: Read CSV file from S3"
    result=$($DATAQL_BIN run -q "SELECT * FROM simple" -f "$DATAQL_TEST_S3_CSV" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob"; then
        log_pass "Basic CSV read from S3 works"
    else
        log_fail "Basic CSV read from S3 failed" "$result"
    fi
}

test_s3_csv_select_columns() {
    log_info "Test: SELECT specific columns from S3 CSV"
    result=$($DATAQL_BIN run -q "SELECT name, email FROM simple" -f "$DATAQL_TEST_S3_CSV" 2>&1)
    if echo "$result" | grep -q "alice@example.com"; then
        log_pass "SELECT specific columns from S3 CSV works"
    else
        log_fail "SELECT specific columns from S3 CSV failed" "$result"
    fi
}

test_s3_csv_where_clause() {
    log_info "Test: WHERE clause on S3 CSV data"
    result=$($DATAQL_BIN run -q "SELECT name FROM simple WHERE age > 30" -f "$DATAQL_TEST_S3_CSV" 2>&1)
    if echo "$result" | grep -q "Bob" && echo "$result" | grep -q "Charlie"; then
        log_pass "WHERE clause on S3 CSV works"
    else
        log_fail "WHERE clause on S3 CSV failed" "$result"
    fi
}

test_s3_csv_count() {
    log_info "Test: COUNT on S3 CSV data"
    result=$($DATAQL_BIN run -q "SELECT COUNT(*) AS total FROM simple" -f "$DATAQL_TEST_S3_CSV" 2>&1)
    if echo "$result" | grep -q "5"; then
        log_pass "COUNT on S3 CSV works"
    else
        log_fail "COUNT on S3 CSV failed" "$result"
    fi
}

test_s3_csv_limit() {
    log_info "Test: LIMIT on S3 CSV data"
    result=$($DATAQL_BIN run -q "SELECT * FROM simple LIMIT 2" -f "$DATAQL_TEST_S3_CSV" 2>&1)
    count=$(echo "$result" | grep -c "@example.com" || true)
    if [ "$count" -eq 2 ]; then
        log_pass "LIMIT on S3 CSV works"
    else
        log_fail "LIMIT on S3 CSV failed, expected 2 rows, got $count"
    fi
}

# ==============================================================================
# JSON FILE TESTS
# ==============================================================================

test_s3_json_basic_read() {
    log_info "Test: Read JSON array file from S3"
    result=$($DATAQL_BIN run -q "SELECT * FROM people" -f "$DATAQL_TEST_S3_JSON" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob"; then
        log_pass "Basic JSON read from S3 works"
    else
        log_fail "Basic JSON read from S3 failed" "$result"
    fi
}

test_s3_json_where_clause() {
    log_info "Test: WHERE clause on S3 JSON data"
    result=$($DATAQL_BIN run -q "SELECT name, age FROM people WHERE age < 30" -f "$DATAQL_TEST_S3_JSON" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Eve"; then
        log_pass "WHERE clause on S3 JSON works"
    else
        log_fail "WHERE clause on S3 JSON failed" "$result"
    fi
}

# ==============================================================================
# JSONL FILE TESTS
# ==============================================================================

test_s3_jsonl_basic_read() {
    log_info "Test: Read JSONL file from S3"
    result=$($DATAQL_BIN run -q "SELECT * FROM data" -f "$DATAQL_TEST_S3_JSONL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Charlie"; then
        log_pass "Basic JSONL read from S3 works"
    else
        log_fail "Basic JSONL read from S3 failed" "$result"
    fi
}

test_s3_jsonl_order_by() {
    log_info "Test: ORDER BY on S3 JSONL data"
    result=$($DATAQL_BIN run -q "SELECT name, age FROM data ORDER BY age ASC" -f "$DATAQL_TEST_S3_JSONL" 2>&1)
    # Eve (25) should appear before Charlie (42)
    eve_pos=$(echo "$result" | grep -n "Eve" | head -1 | cut -d: -f1)
    charlie_pos=$(echo "$result" | grep -n "Charlie" | head -1 | cut -d: -f1)
    if [ -n "$eve_pos" ] && [ -n "$charlie_pos" ] && [ "$eve_pos" -lt "$charlie_pos" ]; then
        log_pass "ORDER BY on S3 JSONL works"
    else
        log_fail "ORDER BY on S3 JSONL failed" "$result"
    fi
}

# ==============================================================================
# EXPORT FORMAT TESTS
# ==============================================================================

test_s3_export_csv() {
    log_info "Test: Export S3 data to CSV"
    output_file="/tmp/s3_export_$$.csv"
    $DATAQL_BIN run -q "SELECT name, email FROM simple" -f "$DATAQL_TEST_S3_CSV" -e "$output_file" -t csv 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file"; then
        log_pass "Export S3 data to CSV works"
        rm -f "$output_file"
    else
        log_fail "Export S3 data to CSV failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_s3_export_jsonl() {
    log_info "Test: Export S3 data to JSONL"
    output_file="/tmp/s3_export_$$.jsonl"
    $DATAQL_BIN run -q "SELECT name, email FROM simple LIMIT 2" -f "$DATAQL_TEST_S3_CSV" -e "$output_file" -t jsonl 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file"; then
        log_pass "Export S3 data to JSONL works"
        rm -f "$output_file"
    else
        log_fail "Export S3 data to JSONL failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_s3_export_json() {
    log_info "Test: Export S3 data to JSON"
    output_file="/tmp/s3_export_$$.json"
    $DATAQL_BIN run -q "SELECT name, email FROM simple LIMIT 2" -f "$DATAQL_TEST_S3_CSV" -e "$output_file" -t json 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file"; then
        log_pass "Export S3 data to JSON works"
        rm -f "$output_file"
    else
        log_fail "Export S3 data to JSON failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

# ==============================================================================
# RUN ALL TESTS
# ==============================================================================

echo "======================================"
echo "DataQL E2E Tests - S3 (LocalStack)"
echo "======================================"

log_section "Prerequisites Check"
if ! check_localstack; then
    echo ""
    echo "======================================"
    echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}, ${YELLOW}1 skipped${NC}"
    echo "======================================"
    exit 0
fi

log_section "CSV File Tests"
test_s3_csv_basic_read
test_s3_csv_select_columns
test_s3_csv_where_clause
test_s3_csv_count
test_s3_csv_limit

log_section "JSON File Tests"
test_s3_json_basic_read
test_s3_json_where_clause

log_section "JSONL File Tests"
test_s3_jsonl_basic_read
test_s3_jsonl_order_by

log_section "Export Format Tests"
test_s3_export_csv
test_s3_export_jsonl
test_s3_export_json

echo ""
echo "======================================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}, ${YELLOW}$SKIPPED skipped${NC}"
echo "======================================"

exit $FAILED
