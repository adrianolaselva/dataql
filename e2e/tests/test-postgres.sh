#!/bin/bash
# DataQL E2E Tests - PostgreSQL
# Tests PostgreSQL connectivity and queries

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

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((PASSED++)) || true
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((FAILED++)) || true
}

log_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

# Test 1: Basic SELECT query
test_basic_select() {
    log_info "Test: Basic SELECT * query"
    result=$($DATAQL_BIN run -q "SELECT * FROM test_data" -f "$DATAQL_TEST_POSTGRES_URL" 2>&1)
    if echo "$result" | grep -q "Alice"; then
        log_pass "Basic SELECT returns data"
    else
        log_fail "Basic SELECT failed: $result"
    fi
}

# Test 2: SELECT with WHERE clause
test_where_clause() {
    log_info "Test: SELECT with WHERE clause"
    result=$($DATAQL_BIN run -q "SELECT name, email FROM test_data WHERE age > 30" -f "$DATAQL_TEST_POSTGRES_URL" 2>&1)
    if echo "$result" | grep -q "Bob" && echo "$result" | grep -q "Charlie"; then
        log_pass "WHERE clause filters correctly"
    else
        log_fail "WHERE clause failed: $result"
    fi
}

# Test 3: SELECT with ORDER BY
test_order_by() {
    log_info "Test: SELECT with ORDER BY"
    result=$($DATAQL_BIN run -q "SELECT name FROM test_data ORDER BY age ASC" -f "$DATAQL_TEST_POSTGRES_URL" 2>&1)
    # Eve (25) should be first - check data rows (skip header row showing table info)
    if echo "$result" | grep -A1 "^name" | tail -1 | grep -q "Eve"; then
        log_pass "ORDER BY works correctly"
    else
        log_fail "ORDER BY failed: $result"
    fi
}

# Test 4: SELECT with LIMIT
test_limit() {
    log_info "Test: SELECT with LIMIT"
    result=$($DATAQL_BIN run -q "SELECT * FROM test_data LIMIT 2" -f "$DATAQL_TEST_POSTGRES_URL" 2>&1)
    count=$(echo "$result" | grep -c "@")
    if [ "$count" -eq 2 ]; then
        log_pass "LIMIT restricts rows correctly"
    else
        log_fail "LIMIT failed, expected 2 rows, got $count"
    fi
}

# Test 5: Aggregate function COUNT
test_count() {
    log_info "Test: COUNT aggregate function"
    result=$($DATAQL_BIN run -q "SELECT COUNT(*) as total FROM test_data" -f "$DATAQL_TEST_POSTGRES_URL" 2>&1)
    if echo "$result" | grep -q "5"; then
        log_pass "COUNT returns correct total"
    else
        log_fail "COUNT failed: $result"
    fi
}

# Test 6: Export to CSV
test_export_csv() {
    log_info "Test: Export to CSV"
    output_file="/tmp/postgres_export_$$.csv"
    $DATAQL_BIN run -q "SELECT name, email FROM test_data" -f "$DATAQL_TEST_POSTGRES_URL" -e "$output_file" -t csv 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file"; then
        log_pass "Export to CSV works"
        rm -f "$output_file"
    else
        log_fail "Export to CSV failed"
    fi
}

# Test 7: Export to JSONL
test_export_jsonl() {
    log_info "Test: Export to JSONL"
    output_file="/tmp/postgres_export_$$.jsonl"
    $DATAQL_BIN run -q "SELECT name, email FROM test_data LIMIT 1" -f "$DATAQL_TEST_POSTGRES_URL" -e "$output_file" -t jsonl 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file"; then
        log_pass "Export to JSONL works"
        rm -f "$output_file"
    else
        log_fail "Export to JSONL failed"
    fi
}

# Test 8: Query with AVG aggregate
test_avg() {
    log_info "Test: AVG aggregate function"
    result=$($DATAQL_BIN run -q "SELECT AVG(age) as avg_age FROM test_data" -f "$DATAQL_TEST_POSTGRES_URL" 2>&1)
    # Average of 28, 35, 42, 31, 25 = 32.2
    if echo "$result" | grep -q -E "(32|32\.2)"; then
        log_pass "AVG aggregate function works"
    else
        log_fail "AVG failed: $result"
    fi
}

# Run all tests
echo "======================================"
echo "DataQL E2E Tests - PostgreSQL"
echo "======================================"
echo ""

test_basic_select
test_where_clause
test_order_by
test_limit
test_count
test_export_csv
test_export_jsonl
test_avg

echo ""
echo "======================================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}"
echo "======================================"

exit $FAILED
