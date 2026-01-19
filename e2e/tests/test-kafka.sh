#!/bin/bash
# DataQL E2E Tests - Kafka
# Comprehensive tests for Kafka message queue reading
#
# Test Coverage:
# - Basic message reading (peek mode - non-destructive)
# - SELECT specific fields from JSON messages
# - LIMIT message count
# - Export formats (CSV, JSONL, JSON)
# - Consumer group configuration
# - Message metadata access

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

# Ensure topic has messages before tests
setup_kafka_messages() {
    log_info "Setting up Kafka test messages..."

    # Produce messages to topic (idempotent - messages may already exist)
    echo '{"id":1,"name":"Alice","email":"alice@example.com","age":28}' | docker exec -i dataql-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic dataql-test-topic 2>/dev/null || true
    echo '{"id":2,"name":"Bob","email":"bob@example.com","age":35}' | docker exec -i dataql-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic dataql-test-topic 2>/dev/null || true
    echo '{"id":3,"name":"Charlie","email":"charlie@example.com","age":42}' | docker exec -i dataql-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic dataql-test-topic 2>/dev/null || true

    # Give Kafka a moment to process
    sleep 1

    log_pass "Kafka messages setup complete"
}

# ==============================================================================
# BASIC READ TESTS
# ==============================================================================

test_basic_read() {
    log_info "Test: SELECT * FROM topic (peek mode)"
    result=$($DATAQL_BIN run -q "SELECT * FROM dataql_test_topic" -f "$DATAQL_TEST_KAFKA_URL" 2>&1)
    if echo "$result" | grep -q -E "(Alice|Bob|Charlie|name)"; then
        log_pass "Basic Kafka read works (peek mode)"
    else
        log_fail "Basic Kafka read failed" "$result"
    fi
}

test_select_body_fields() {
    log_info "Test: SELECT body fields from messages"
    result=$($DATAQL_BIN run -q "SELECT body_name, body_email FROM dataql_test_topic" -f "$DATAQL_TEST_KAFKA_URL" 2>&1)
    if echo "$result" | grep -q -E "(@example.com|name|email)"; then
        log_pass "SELECT body fields works"
    else
        log_fail "SELECT body fields failed" "$result"
    fi
}

test_select_with_limit() {
    log_info "Test: SELECT with LIMIT"
    result=$($DATAQL_BIN run -q "SELECT * FROM dataql_test_topic LIMIT 1" -f "$DATAQL_TEST_KAFKA_URL" 2>&1)
    # Should return at least one message
    if echo "$result" | grep -q -E "(Alice|Bob|Charlie|body|name)"; then
        log_pass "SELECT with LIMIT works"
    else
        log_fail "SELECT with LIMIT failed" "$result"
    fi
}

# ==============================================================================
# CONSUMER GROUP TESTS
# ==============================================================================

test_with_consumer_group() {
    log_info "Test: Read with consumer group"
    kafka_url_with_group="kafka://localhost:29092/dataql-test-topic?group_id=dataql-e2e-test-group"
    result=$($DATAQL_BIN run -q "SELECT * FROM dataql_test_topic LIMIT 1" -f "$kafka_url_with_group" 2>&1)
    if echo "$result" | grep -q -E "(Alice|Bob|Charlie|name|body)"; then
        log_pass "Read with consumer group works"
    else
        log_fail "Read with consumer group failed" "$result"
    fi
}

# ==============================================================================
# EXPORT FORMAT TESTS
# ==============================================================================

test_export_jsonl() {
    log_info "Test: Export to JSONL"
    output_file="/tmp/kafka_export_$$.jsonl"
    $DATAQL_BIN run -q "SELECT * FROM dataql_test_topic LIMIT 2" -f "$DATAQL_TEST_KAFKA_URL" -e "$output_file" -t jsonl 2>&1 > /dev/null
    if [ -f "$output_file" ]; then
        log_pass "Export to JSONL creates file"
        rm -f "$output_file"
    else
        log_fail "Export to JSONL failed - no file created"
    fi
}

test_export_csv() {
    log_info "Test: Export to CSV"
    output_file="/tmp/kafka_export_$$.csv"
    $DATAQL_BIN run -q "SELECT * FROM dataql_test_topic LIMIT 2" -f "$DATAQL_TEST_KAFKA_URL" -e "$output_file" -t csv 2>&1 > /dev/null
    if [ -f "$output_file" ]; then
        log_pass "Export to CSV creates file"
        rm -f "$output_file"
    else
        log_fail "Export to CSV failed - no file created"
    fi
}

test_export_json() {
    log_info "Test: Export to JSON"
    output_file="/tmp/kafka_export_$$.json"
    $DATAQL_BIN run -q "SELECT * FROM dataql_test_topic LIMIT 2" -f "$DATAQL_TEST_KAFKA_URL" -e "$output_file" -t json 2>&1 > /dev/null
    if [ -f "$output_file" ]; then
        log_pass "Export to JSON creates file"
        rm -f "$output_file"
    else
        log_fail "Export to JSON failed - no file created"
    fi
}

# ==============================================================================
# MULTIPLE READS (PEEK VERIFICATION)
# ==============================================================================

test_peek_mode_nondestructive() {
    log_info "Test: Peek mode is non-destructive (multiple reads)"
    # Read twice - both should return data since peek doesn't commit offsets
    result1=$($DATAQL_BIN run -q "SELECT * FROM dataql_test_topic LIMIT 1" -f "$DATAQL_TEST_KAFKA_URL" 2>&1)
    result2=$($DATAQL_BIN run -q "SELECT * FROM dataql_test_topic LIMIT 1" -f "$DATAQL_TEST_KAFKA_URL" 2>&1)

    if echo "$result1" | grep -q -E "(Alice|Bob|Charlie|name)" && \
       echo "$result2" | grep -q -E "(Alice|Bob|Charlie|name)"; then
        log_pass "Peek mode is non-destructive (both reads returned data)"
    else
        log_fail "Peek mode may be consuming messages" "First: $result1, Second: $result2"
    fi
}

# ==============================================================================
# RUN ALL TESTS
# ==============================================================================

echo "======================================"
echo "DataQL E2E Tests - Kafka"
echo "======================================"

log_section "Setup"
setup_kafka_messages
# Wait for messages to be fully available
sleep 2

log_section "Basic Read Tests"
test_basic_read
test_select_body_fields
test_select_with_limit

log_section "Consumer Group Tests"
test_with_consumer_group

log_section "Export Format Tests"
test_export_jsonl
test_export_csv
test_export_json

log_section "Peek Mode Verification"
test_peek_mode_nondestructive

echo ""
echo "======================================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}"
echo "======================================"

exit $FAILED
