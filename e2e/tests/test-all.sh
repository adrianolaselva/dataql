#!/bin/bash
# DataQL E2E Tests - Run All Tests
# Executes all integration tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

TOTAL_PASSED=0
TOTAL_FAILED=0
FAILED_SUITES=""

run_test_suite() {
    local name="$1"
    local script="$2"

    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Running: $name${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    if [ -x "$script" ]; then
        if "$script"; then
            echo -e "${GREEN}$name: PASSED${NC}"
        else
            echo -e "${RED}$name: FAILED${NC}"
            FAILED_SUITES="$FAILED_SUITES $name"
            ((TOTAL_FAILED++))
            return 1
        fi
    else
        echo -e "${YELLOW}$name: SKIPPED (script not found or not executable)${NC}"
        return 0
    fi

    ((TOTAL_PASSED++))
    return 0
}

echo ""
echo "======================================================"
echo "        DataQL E2E Integration Tests"
echo "======================================================"
echo ""
echo "Test suites to run:"
echo "  - PostgreSQL"
echo "  - MySQL"
echo "  - MongoDB"
echo "  - S3 (LocalStack)"
echo "  - SQS (LocalStack)"
echo "  - Kafka"
echo ""

# Ensure scripts are executable
chmod +x "$SCRIPT_DIR"/*.sh 2>/dev/null || true

# Run all test suites
run_test_suite "PostgreSQL" "$SCRIPT_DIR/test-postgres.sh" || true
run_test_suite "MySQL" "$SCRIPT_DIR/test-mysql.sh" || true
run_test_suite "MongoDB" "$SCRIPT_DIR/test-mongodb.sh" || true
run_test_suite "S3" "$SCRIPT_DIR/test-s3.sh" || true
run_test_suite "SQS" "$SCRIPT_DIR/test-sqs.sh" || true
run_test_suite "Kafka" "$SCRIPT_DIR/test-kafka.sh" || true

echo ""
echo "======================================================"
echo "                 SUMMARY"
echo "======================================================"
echo ""
echo -e "Test Suites Passed: ${GREEN}$TOTAL_PASSED${NC}"
echo -e "Test Suites Failed: ${RED}$TOTAL_FAILED${NC}"

if [ -n "$FAILED_SUITES" ]; then
    echo ""
    echo -e "${RED}Failed suites:$FAILED_SUITES${NC}"
    echo ""
    exit 1
fi

echo ""
echo -e "${GREEN}All test suites passed!${NC}"
echo ""
exit 0
