#!/bin/bash
# DataQL E2E Tests - MongoDB
# Comprehensive tests for MongoDB connectivity and query functionality
#
# Test Coverage:
# - Basic SELECT queries on collections
# - WHERE clause with various operators
# - ORDER BY (ASC/DESC)
# - LIMIT
# - Aggregate functions (COUNT)
# - Export formats (CSV, JSONL, JSON)
# - Multiple collections support

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

log_info() {
    echo -e "  ${YELLOW}[INFO]${NC} $1"
}

log_section() {
    echo ""
    echo -e "${CYAN}── $1 ──${NC}"
}

# ==============================================================================
# BASIC SELECT TESTS
# ==============================================================================

test_basic_select_all() {
    log_info "Test: SELECT * FROM users"
    result=$($DATAQL_BIN run -q "SELECT * FROM users" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob"; then
        log_pass "Basic SELECT * returns all documents"
    else
        log_fail "Basic SELECT * failed" "$result"
    fi
}

test_select_specific_fields() {
    log_info "Test: SELECT name, email FROM users"
    result=$($DATAQL_BIN run -q "SELECT name, email FROM users" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "alice@example.com"; then
        log_pass "SELECT specific fields works"
    else
        log_fail "SELECT specific fields failed" "$result"
    fi
}

test_select_with_alias() {
    log_info "Test: SELECT name AS user_name FROM users"
    result=$($DATAQL_BIN run -q "SELECT name AS user_name FROM users" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q -i "user_name\|Alice"; then
        log_pass "SELECT with alias works"
    else
        log_fail "SELECT with alias failed" "$result"
    fi
}

# ==============================================================================
# WHERE CLAUSE TESTS
# ==============================================================================

test_where_equals() {
    log_info "Test: WHERE name = 'Alice'"
    result=$($DATAQL_BIN run -q "SELECT * FROM users WHERE name = 'Alice'" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && ! echo "$result" | grep -q "Bob"; then
        log_pass "WHERE equals works"
    else
        log_fail "WHERE equals failed" "$result"
    fi
}

test_where_greater_than() {
    log_info "Test: WHERE age > 30"
    result=$($DATAQL_BIN run -q "SELECT name, age FROM users WHERE age > 30" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "Bob" && echo "$result" | grep -q "Charlie"; then
        log_pass "WHERE greater than works"
    else
        log_fail "WHERE greater than failed" "$result"
    fi
}

test_where_less_than() {
    log_info "Test: WHERE age < 30"
    result=$($DATAQL_BIN run -q "SELECT name, age FROM users WHERE age < 30" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Eve"; then
        log_pass "WHERE less than works"
    else
        log_fail "WHERE less than failed" "$result"
    fi
}

test_where_and() {
    log_info "Test: WHERE age > 25 AND age < 40"
    result=$($DATAQL_BIN run -q "SELECT name FROM users WHERE age > 25 AND age < 40" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob" && ! echo "$result" | grep -q "Eve"; then
        log_pass "WHERE AND condition works"
    else
        log_fail "WHERE AND condition failed" "$result"
    fi
}

test_where_or() {
    log_info "Test: WHERE name = 'Alice' OR name = 'Eve'"
    result=$($DATAQL_BIN run -q "SELECT name FROM users WHERE name = 'Alice' OR name = 'Eve'" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Eve"; then
        log_pass "WHERE OR condition works"
    else
        log_fail "WHERE OR condition failed" "$result"
    fi
}

test_where_in() {
    log_info "Test: WHERE name IN ('Alice', 'Bob')"
    result=$($DATAQL_BIN run -q "SELECT name FROM users WHERE name IN ('Alice', 'Bob')" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob" && ! echo "$result" | grep -q "Charlie"; then
        log_pass "WHERE IN clause works"
    else
        log_fail "WHERE IN clause failed" "$result"
    fi
}

test_where_like() {
    log_info "Test: WHERE email LIKE '%@example.com'"
    result=$($DATAQL_BIN run -q "SELECT email FROM users WHERE email LIKE '%@example.com'" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "@example.com"; then
        log_pass "WHERE LIKE pattern works"
    else
        log_fail "WHERE LIKE pattern failed" "$result"
    fi
}

test_where_role() {
    log_info "Test: WHERE role = 'admin'"
    result=$($DATAQL_BIN run -q "SELECT name, role FROM users WHERE role = 'admin'" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "admin"; then
        log_pass "WHERE on string field works"
    else
        log_fail "WHERE on string field failed" "$result"
    fi
}

# ==============================================================================
# ORDER BY TESTS
# ==============================================================================

test_order_by_asc() {
    log_info "Test: ORDER BY age ASC"
    result=$($DATAQL_BIN run -q "SELECT name, age FROM users ORDER BY age ASC" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    # Eve (25) should appear before Charlie (42)
    eve_pos=$(echo "$result" | grep -n "Eve" | head -1 | cut -d: -f1)
    charlie_pos=$(echo "$result" | grep -n "Charlie" | head -1 | cut -d: -f1)
    if [ -n "$eve_pos" ] && [ -n "$charlie_pos" ] && [ "$eve_pos" -lt "$charlie_pos" ]; then
        log_pass "ORDER BY ASC works"
    else
        log_fail "ORDER BY ASC failed" "$result"
    fi
}

test_order_by_desc() {
    log_info "Test: ORDER BY age DESC"
    result=$($DATAQL_BIN run -q "SELECT name, age FROM users ORDER BY age DESC" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    # Charlie (42) should appear before Eve (25)
    charlie_pos=$(echo "$result" | grep -n "Charlie" | head -1 | cut -d: -f1)
    eve_pos=$(echo "$result" | grep -n "Eve" | head -1 | cut -d: -f1)
    if [ -n "$charlie_pos" ] && [ -n "$eve_pos" ] && [ "$charlie_pos" -lt "$eve_pos" ]; then
        log_pass "ORDER BY DESC works"
    else
        log_fail "ORDER BY DESC failed" "$result"
    fi
}

# ==============================================================================
# LIMIT TESTS
# ==============================================================================

test_limit() {
    log_info "Test: LIMIT 2"
    result=$($DATAQL_BIN run -q "SELECT * FROM users LIMIT 2" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    count=$(echo "$result" | grep -c "@example.com" || true)
    if [ "$count" -eq 2 ]; then
        log_pass "LIMIT restricts documents correctly"
    else
        log_fail "LIMIT failed, expected 2 documents, got $count"
    fi
}

# ==============================================================================
# AGGREGATE FUNCTION TESTS
# ==============================================================================

test_count() {
    log_info "Test: COUNT(*)"
    result=$($DATAQL_BIN run -q "SELECT COUNT(*) AS total FROM users" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "5"; then
        log_pass "COUNT(*) returns correct total"
    else
        log_fail "COUNT(*) failed" "$result"
    fi
}

test_count_with_where() {
    log_info "Test: COUNT(*) with WHERE"
    result=$($DATAQL_BIN run -q "SELECT COUNT(*) AS count FROM users WHERE age > 30" -f "$DATAQL_TEST_MONGODB_URL" 2>&1)
    if echo "$result" | grep -q "3"; then
        log_pass "COUNT(*) with WHERE works"
    else
        log_fail "COUNT(*) with WHERE failed" "$result"
    fi
}

# ==============================================================================
# EXPORT FORMAT TESTS
# ==============================================================================

test_export_csv() {
    log_info "Test: Export to CSV"
    output_file="/tmp/mongodb_export_$$.csv"
    $DATAQL_BIN run -q "SELECT name, email FROM users" -f "$DATAQL_TEST_MONGODB_URL" -e "$output_file" -t csv 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file" && grep -q "," "$output_file"; then
        log_pass "Export to CSV works"
        rm -f "$output_file"
    else
        log_fail "Export to CSV failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_export_jsonl() {
    log_info "Test: Export to JSONL"
    output_file="/tmp/mongodb_export_$$.jsonl"
    $DATAQL_BIN run -q "SELECT name, email FROM users LIMIT 2" -f "$DATAQL_TEST_MONGODB_URL" -e "$output_file" -t jsonl 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file"; then
        log_pass "Export to JSONL works"
        rm -f "$output_file"
    else
        log_fail "Export to JSONL failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_export_json() {
    log_info "Test: Export to JSON"
    output_file="/tmp/mongodb_export_$$.json"
    $DATAQL_BIN run -q "SELECT name, email FROM users LIMIT 2" -f "$DATAQL_TEST_MONGODB_URL" -e "$output_file" -t json 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file"; then
        log_pass "Export to JSON works"
        rm -f "$output_file"
    else
        log_fail "Export to JSON failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

# ==============================================================================
# MULTIPLE COLLECTIONS TESTS
# ==============================================================================

test_query_test_data_collection() {
    log_info "Test: Query test_data collection"
    test_data_url="mongodb://dataql:dataql_pass@localhost:27117/dataql_test/test_data?authSource=admin"
    result=$($DATAQL_BIN run -q "SELECT * FROM test_data" -f "$test_data_url" 2>&1)
    if echo "$result" | grep -q "Alice"; then
        log_pass "Query test_data collection works"
    else
        log_fail "Query test_data collection failed" "$result"
    fi
}

test_query_orders_collection() {
    log_info "Test: Query orders collection"
    orders_url="mongodb://dataql:dataql_pass@localhost:27117/dataql_test/orders?authSource=admin"
    result=$($DATAQL_BIN run -q "SELECT * FROM orders" -f "$orders_url" 2>&1)
    if echo "$result" | grep -q "Laptop\|Mouse\|product"; then
        log_pass "Query orders collection works"
    else
        log_fail "Query orders collection failed" "$result"
    fi
}

test_query_orders_with_filter() {
    log_info "Test: Query orders with amount filter"
    orders_url="mongodb://dataql:dataql_pass@localhost:27117/dataql_test/orders?authSource=admin"
    result=$($DATAQL_BIN run -q "SELECT product, amount FROM orders WHERE amount > 100" -f "$orders_url" 2>&1)
    if echo "$result" | grep -q "Laptop"; then
        log_pass "Query orders with filter works"
    else
        log_fail "Query orders with filter failed" "$result"
    fi
}

# ==============================================================================
# RUN ALL TESTS
# ==============================================================================

echo "======================================"
echo "DataQL E2E Tests - MongoDB"
echo "======================================"

log_section "Basic SELECT Tests"
test_basic_select_all
test_select_specific_fields
test_select_with_alias

log_section "WHERE Clause Tests"
test_where_equals
test_where_greater_than
test_where_less_than
test_where_and
test_where_or
test_where_in
test_where_like
test_where_role

log_section "ORDER BY Tests"
test_order_by_asc
test_order_by_desc

log_section "LIMIT Tests"
test_limit

log_section "Aggregate Function Tests"
test_count
test_count_with_where

log_section "Export Format Tests"
test_export_csv
test_export_jsonl
test_export_json

log_section "Multiple Collections Tests"
test_query_test_data_collection
test_query_orders_collection
test_query_orders_with_filter

echo ""
echo "======================================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}"
echo "======================================"

exit $FAILED
