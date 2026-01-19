#!/bin/bash
# DataQL E2E Tests - SQS (via LocalStack)
# Comprehensive tests for SQS message queue reading functionality
#
# Test Coverage:
# - Basic message peeking (read without consume)
# - SELECT queries on message data
# - WHERE clause filtering
# - COUNT and aggregation
# - LIMIT functionality
# - Export formats (CSV, JSON, JSONL)
#
# Prerequisites:
# - LocalStack running with SQS service
# - AWS_ENDPOINT_URL set to LocalStack endpoint
# - Test queue 'dataql-test-queue' with sample messages

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

# Check if LocalStack SQS is available
check_localstack() {
    log_info "Checking LocalStack SQS availability..."

    if ! curl -s "${AWS_ENDPOINT_URL}/_localstack/health" | grep -q "running"; then
        echo -e "${RED}LocalStack is not running. Skipping SQS tests.${NC}"
        return 1
    fi

    # Verify queue exists
    if ! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url="${AWS_ENDPOINT_URL}" sqs get-queue-url --queue-name dataql-test-queue > /dev/null 2>&1; then
        echo -e "${RED}SQS queue 'dataql-test-queue' not found. Skipping SQS tests.${NC}"
        return 1
    fi

    log_pass "LocalStack SQS is available"
    return 0
}

# Repopulate messages in the queue for testing
# NOTE: LocalStack doesn't properly support VisibilityTimeout=0, so messages
# are consumed when reading even with peek mode. We repopulate before each test.
repopulate_queue() {
    QUEUE_URL="${AWS_ENDPOINT_URL}/000000000000/dataql-test-queue"

    # Purge existing messages (quick operation on LocalStack)
    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url="${AWS_ENDPOINT_URL}" sqs purge-queue --queue-url "$QUEUE_URL" 2>/dev/null || true

    # Brief wait for purge
    sleep 1

    # Send fresh test messages
    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url="${AWS_ENDPOINT_URL}" sqs send-message --queue-url "$QUEUE_URL" \
        --message-body '{"id": 1, "name": "Alice", "email": "alice@example.com", "age": 28}' > /dev/null

    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url="${AWS_ENDPOINT_URL}" sqs send-message --queue-url "$QUEUE_URL" \
        --message-body '{"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35}' > /dev/null

    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url="${AWS_ENDPOINT_URL}" sqs send-message --queue-url "$QUEUE_URL" \
        --message-body '{"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 42}' > /dev/null

    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url="${AWS_ENDPOINT_URL}" sqs send-message --queue-url "$QUEUE_URL" \
        --message-body '{"id": 4, "name": "Diana", "email": "diana@example.com", "age": 31}' > /dev/null

    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url="${AWS_ENDPOINT_URL}" sqs send-message --queue-url "$QUEUE_URL" \
        --message-body '{"id": 5, "name": "Eve", "email": "eve@example.com", "age": 25}' > /dev/null
}

# ==============================================================================
# BASIC MESSAGE READING TESTS
# ==============================================================================

test_sqs_basic_peek() {
    log_info "Test: Basic peek messages from SQS"
    result=$($DATAQL_BIN run -q "SELECT * FROM dataql_test_queue" -f "$DATAQL_TEST_SQS_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Bob"; then
        log_pass "Basic SQS message peek works"
    else
        log_fail "Basic SQS message peek failed" "$result"
    fi
}

test_sqs_select_columns() {
    log_info "Test: SELECT specific columns from SQS messages"
    result=$($DATAQL_BIN run -q "SELECT body_name, body_email FROM dataql_test_queue" -f "$DATAQL_TEST_SQS_URL" 2>&1)
    if echo "$result" | grep -q "alice@example.com"; then
        log_pass "SELECT specific columns from SQS works"
    else
        log_fail "SELECT specific columns from SQS failed" "$result"
    fi
}

test_sqs_where_clause() {
    log_info "Test: WHERE clause on SQS message data"
    result=$($DATAQL_BIN run -q "SELECT body_name FROM dataql_test_queue WHERE body_age > 30" -f "$DATAQL_TEST_SQS_URL" 2>&1)
    if echo "$result" | grep -q "Bob" && echo "$result" | grep -q "Charlie"; then
        log_pass "WHERE clause on SQS works"
    else
        log_fail "WHERE clause on SQS failed" "$result"
    fi
}

test_sqs_where_equals() {
    log_info "Test: WHERE with equals condition"
    result=$($DATAQL_BIN run -q "SELECT body_name, body_age FROM dataql_test_queue WHERE body_name = 'Alice'" -f "$DATAQL_TEST_SQS_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "28"; then
        log_pass "WHERE equals on SQS works"
    else
        log_fail "WHERE equals on SQS failed" "$result"
    fi
}

test_sqs_where_less_than() {
    log_info "Test: WHERE with less than condition"
    result=$($DATAQL_BIN run -q "SELECT body_name FROM dataql_test_queue WHERE body_age < 30" -f "$DATAQL_TEST_SQS_URL" 2>&1)
    if echo "$result" | grep -q "Alice" && echo "$result" | grep -q "Eve"; then
        log_pass "WHERE less than on SQS works"
    else
        log_fail "WHERE less than on SQS failed" "$result"
    fi
}

# ==============================================================================
# COUNT AND AGGREGATION TESTS
# ==============================================================================

test_sqs_count() {
    log_info "Test: COUNT on SQS messages"
    result=$($DATAQL_BIN run -q "SELECT COUNT(*) AS total FROM dataql_test_queue" -f "$DATAQL_TEST_SQS_URL" 2>&1)
    if echo "$result" | grep -q "5"; then
        log_pass "COUNT on SQS works"
    else
        log_fail "COUNT on SQS failed" "$result"
    fi
}

test_sqs_count_with_where() {
    log_info "Test: COUNT with WHERE clause"
    result=$($DATAQL_BIN run -q "SELECT COUNT(*) AS over_30 FROM dataql_test_queue WHERE body_age > 30" -f "$DATAQL_TEST_SQS_URL" 2>&1)
    if echo "$result" | grep -q "3"; then
        log_pass "COUNT with WHERE on SQS works"
    else
        log_fail "COUNT with WHERE on SQS failed" "$result"
    fi
}

# ==============================================================================
# LIMIT TESTS
# ==============================================================================

test_sqs_limit() {
    log_info "Test: LIMIT on SQS messages"
    result=$($DATAQL_BIN run -q "SELECT * FROM dataql_test_queue LIMIT 2" -f "$DATAQL_TEST_SQS_URL" 2>&1)
    count=$(echo "$result" | grep -c "@example.com" || true)
    if [ "$count" -eq 2 ]; then
        log_pass "LIMIT on SQS works"
    else
        log_fail "LIMIT on SQS failed, expected 2 rows, got $count"
    fi
}

test_sqs_limit_one() {
    log_info "Test: LIMIT 1 on SQS messages"
    result=$($DATAQL_BIN run -q "SELECT body_name FROM dataql_test_queue LIMIT 1" -f "$DATAQL_TEST_SQS_URL" 2>&1)
    count=$(echo "$result" | grep -c -E "(Alice|Bob|Charlie|Diana|Eve)" || true)
    if [ "$count" -eq 1 ]; then
        log_pass "LIMIT 1 on SQS works"
    else
        log_fail "LIMIT 1 on SQS failed, expected 1 row, got $count"
    fi
}

# ==============================================================================
# ORDER BY TESTS
# ==============================================================================

test_sqs_order_by_asc() {
    log_info "Test: ORDER BY ASC on SQS messages"
    result=$($DATAQL_BIN run -q "SELECT body_name, body_age FROM dataql_test_queue ORDER BY body_age ASC" -f "$DATAQL_TEST_SQS_URL" 2>&1)
    # Eve (25) should appear before Charlie (42)
    eve_pos=$(echo "$result" | grep -n "Eve" | head -1 | cut -d: -f1)
    charlie_pos=$(echo "$result" | grep -n "Charlie" | head -1 | cut -d: -f1)
    if [ -n "$eve_pos" ] && [ -n "$charlie_pos" ] && [ "$eve_pos" -lt "$charlie_pos" ]; then
        log_pass "ORDER BY ASC on SQS works"
    else
        log_fail "ORDER BY ASC on SQS failed" "$result"
    fi
}

test_sqs_order_by_desc() {
    log_info "Test: ORDER BY DESC on SQS messages"
    result=$($DATAQL_BIN run -q "SELECT body_name, body_age FROM dataql_test_queue ORDER BY body_age DESC" -f "$DATAQL_TEST_SQS_URL" 2>&1)
    # Charlie (42) should appear before Eve (25)
    charlie_pos=$(echo "$result" | grep -n "Charlie" | head -1 | cut -d: -f1)
    eve_pos=$(echo "$result" | grep -n "Eve" | head -1 | cut -d: -f1)
    if [ -n "$eve_pos" ] && [ -n "$charlie_pos" ] && [ "$charlie_pos" -lt "$eve_pos" ]; then
        log_pass "ORDER BY DESC on SQS works"
    else
        log_fail "ORDER BY DESC on SQS failed" "$result"
    fi
}

# ==============================================================================
# EXPORT FORMAT TESTS
# ==============================================================================

test_sqs_export_csv() {
    log_info "Test: Export SQS data to CSV"
    output_file="/tmp/sqs_export_$$.csv"
    $DATAQL_BIN run -q "SELECT body_name, body_email FROM dataql_test_queue" -f "$DATAQL_TEST_SQS_URL" -e "$output_file" -t csv 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "Alice" "$output_file"; then
        log_pass "Export SQS data to CSV works"
        rm -f "$output_file"
    else
        log_fail "Export SQS data to CSV failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_sqs_export_jsonl() {
    log_info "Test: Export SQS data to JSONL"
    output_file="/tmp/sqs_export_$$.jsonl"
    $DATAQL_BIN run -q "SELECT body_name, body_email FROM dataql_test_queue LIMIT 2" -f "$DATAQL_TEST_SQS_URL" -e "$output_file" -t jsonl 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "example.com" "$output_file"; then
        log_pass "Export SQS data to JSONL works"
        rm -f "$output_file"
    else
        log_fail "Export SQS data to JSONL failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

test_sqs_export_json() {
    log_info "Test: Export SQS data to JSON"
    output_file="/tmp/sqs_export_$$.json"
    $DATAQL_BIN run -q "SELECT body_name, body_email FROM dataql_test_queue LIMIT 2" -f "$DATAQL_TEST_SQS_URL" -e "$output_file" -t json 2>&1 > /dev/null
    if [ -f "$output_file" ] && grep -q "example.com" "$output_file"; then
        log_pass "Export SQS data to JSON works"
        rm -f "$output_file"
    else
        log_fail "Export SQS data to JSON failed"
        rm -f "$output_file" 2>/dev/null
    fi
}

# ==============================================================================
# PEEK MODE VERIFICATION
# NOTE: LocalStack doesn't properly support VisibilityTimeout=0, so this test
# verifies that messages are read successfully, but cannot verify non-consumption
# ==============================================================================

test_sqs_peek_mode() {
    log_info "Test: Verify messages can be read from queue"

    # Read messages - check for the COUNT value in the result
    result=$($DATAQL_BIN run -q "SELECT COUNT(*) AS total FROM dataql_test_queue" -f "$DATAQL_TEST_SQS_URL" 2>&1)

    # The output format has the count value followed by "(N rows)"
    # We look for "5" followed by newline and "(1 rows)"
    if echo "$result" | grep -E "^5[[:space:]]*$" > /dev/null || echo "$result" | grep -q "total.*5"; then
        log_pass "SQS message reading works (5 messages read)"
        log_info "Note: LocalStack consumes messages despite VisibilityTimeout=0"
    else
        log_fail "SQS message reading failed" "$result"
    fi
}

# ==============================================================================
# RUN ALL TESTS
# ==============================================================================

echo "======================================"
echo "DataQL E2E Tests - SQS (LocalStack)"
echo "======================================"

log_section "Prerequisites Check"
if ! check_localstack; then
    echo ""
    echo "======================================"
    echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}, ${YELLOW}1 skipped${NC}"
    echo "======================================"
    exit 0
fi

# NOTE: LocalStack doesn't properly support VisibilityTimeout=0, so messages
# are consumed even when peeking. We repopulate before each test section.

log_section "Basic Message Reading Tests"
repopulate_queue
test_sqs_basic_peek
repopulate_queue
test_sqs_select_columns
repopulate_queue
test_sqs_where_clause
repopulate_queue
test_sqs_where_equals
repopulate_queue
test_sqs_where_less_than

log_section "Count and Aggregation Tests"
repopulate_queue
test_sqs_count
repopulate_queue
test_sqs_count_with_where

log_section "Limit Tests"
repopulate_queue
test_sqs_limit
repopulate_queue
test_sqs_limit_one

log_section "Order By Tests"
repopulate_queue
test_sqs_order_by_asc
repopulate_queue
test_sqs_order_by_desc

log_section "Export Format Tests"
repopulate_queue
test_sqs_export_csv
repopulate_queue
test_sqs_export_jsonl
repopulate_queue
test_sqs_export_json

log_section "Peek Mode Verification"
repopulate_queue
test_sqs_peek_mode

echo ""
echo "======================================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}, ${YELLOW}$SKIPPED skipped${NC}"
echo "======================================"

exit $FAILED
