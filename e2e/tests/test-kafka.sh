#!/bin/bash
# DataQL E2E Tests - Kafka
# Tests Kafka message reading and queries

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
NC='\033[0m'

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

# Ensure topic has messages before tests
setup_kafka_messages() {
    log_info "Setting up Kafka test messages..."

    # Produce messages to topic
    echo '{"id":1,"name":"Alice","email":"alice@example.com"}' | docker exec -i dataql-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic dataql-test-topic 2>/dev/null
    echo '{"id":2,"name":"Bob","email":"bob@example.com"}' | docker exec -i dataql-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic dataql-test-topic 2>/dev/null
    echo '{"id":3,"name":"Charlie","email":"charlie@example.com"}' | docker exec -i dataql-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic dataql-test-topic 2>/dev/null

    log_pass "Kafka messages setup complete"
}

# Test 1: Basic Kafka read (peek mode)
test_kafka_basic_read() {
    log_info "Test: Basic Kafka read (peek mode)"
    result=$($DATAQL_BIN run -q "SELECT * FROM dataql_test_topic" -f "$DATAQL_TEST_KAFKA_URL" 2>&1)
    if echo "$result" | grep -q -E "(Alice|Bob|Charlie|name|messages)"; then
        log_pass "Kafka basic read works"
    else
        log_fail "Kafka basic read failed: $result"
    fi
}

# Test 2: Kafka select specific fields
test_kafka_select_fields() {
    log_info "Test: Kafka select specific fields"
    result=$($DATAQL_BIN run -q "SELECT body_name, body_email FROM dataql_test_topic" -f "$DATAQL_TEST_KAFKA_URL" 2>&1)
    if echo "$result" | grep -q -E "(@example.com|name|email)"; then
        log_pass "Kafka select specific fields works"
    else
        log_fail "Kafka select specific fields failed: $result"
    fi
}

# Test 3: Kafka export to file
test_kafka_export() {
    log_info "Test: Kafka export to file"
    output_file="/tmp/kafka_export_$$.jsonl"
    $DATAQL_BIN run -q "SELECT * FROM dataql_test_topic" -f "$DATAQL_TEST_KAFKA_URL" -e "$output_file" -t jsonl 2>&1 > /dev/null
    if [ -f "$output_file" ]; then
        log_pass "Kafka export to file works"
        rm -f "$output_file"
    else
        log_fail "Kafka export failed - no file created"
    fi
}

# Test 4: Kafka with consumer group
test_kafka_consumer_group() {
    log_info "Test: Kafka with consumer group"
    KAFKA_URL_WITH_GROUP="kafka://localhost:29092/dataql-test-topic?group=dataql-e2e-test-group"
    result=$($DATAQL_BIN run -q "SELECT * FROM dataql_test_topic LIMIT 1" -f "$KAFKA_URL_WITH_GROUP" 2>&1)
    if echo "$result" | grep -q -E "(Alice|Bob|Charlie|name|messages)"; then
        log_pass "Kafka consumer group works"
    else
        log_fail "Kafka consumer group failed: $result"
    fi
}

# Run all tests
echo "======================================"
echo "DataQL E2E Tests - Kafka"
echo "======================================"
echo ""

setup_kafka_messages
# Wait a bit for messages to be available
sleep 2

test_kafka_basic_read
test_kafka_select_fields
test_kafka_export
test_kafka_consumer_group

echo ""
echo "======================================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}"
echo "======================================"

exit $FAILED
