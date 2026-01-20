#!/bin/bash
# DataQL E2E Tests - Run All Tests
# Executes all integration tests and provides comprehensive summary
#
# Usage: ./test-all.sh [--verbose] [--stop-on-failure]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(dirname "$SCRIPT_DIR")"

# Parse arguments
VERBOSE=false
STOP_ON_FAILURE=false
for arg in "$@"; do
    case $arg in
        --verbose|-v)
            VERBOSE=true
            ;;
        --stop-on-failure|-s)
            STOP_ON_FAILURE=true
            ;;
    esac
done

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

TOTAL_PASSED=0
TOTAL_FAILED=0
TOTAL_SKIPPED=0
FAILED_SUITES=""
SKIPPED_SUITES=""
SUITE_RESULTS=""

# Run a test suite and capture results
run_test_suite() {
    local name="$1"
    local script="$2"
    local result=""
    local exit_code=0

    echo ""
    echo -e "${BLUE}════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Running: $name${NC}"
    echo -e "${BLUE}════════════════════════════════════════${NC}"
    echo ""

    if [ ! -x "$script" ]; then
        echo -e "${YELLOW}[SKIP] $name: Script not found or not executable${NC}"
        SKIPPED_SUITES="$SKIPPED_SUITES $name"
        ((TOTAL_SKIPPED++)) || true
        SUITE_RESULTS="$SUITE_RESULTS\n  ${YELLOW}⊘${NC} $name: SKIPPED"
        return 0
    fi

    # Run the test and capture exit code
    if "$script"; then
        exit_code=0
    else
        exit_code=$?
    fi

    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ $name: PASSED${NC}"
        ((TOTAL_PASSED++)) || true
        SUITE_RESULTS="$SUITE_RESULTS\n  ${GREEN}✓${NC} $name: PASSED"
    else
        echo -e "${RED}✗ $name: FAILED (exit code: $exit_code)${NC}"
        FAILED_SUITES="$FAILED_SUITES $name"
        ((TOTAL_FAILED++)) || true
        SUITE_RESULTS="$SUITE_RESULTS\n  ${RED}✗${NC} $name: FAILED"

        if [ "$STOP_ON_FAILURE" = true ]; then
            echo -e "${RED}Stopping on first failure as requested.${NC}"
            return 1
        fi
    fi

    return 0
}

# Print header
clear_line() {
    echo ""
}

print_header() {
    clear_line
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║          DataQL E2E Integration Test Suite               ║${NC}"
    echo -e "${CYAN}╠══════════════════════════════════════════════════════════╣${NC}"
    echo -e "${CYAN}║  Testing all data source implementations                 ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
    clear_line
}

print_test_plan() {
    echo -e "${YELLOW}Test Suites:${NC}"
    echo "  • PostgreSQL - Relational database queries"
    echo "  • MySQL      - Relational database queries"
    echo "  • MongoDB    - Document database queries"
    echo "  • Kafka      - Message queue reading"
    echo "  • S3         - Object storage (LocalStack)"
    echo "  • SQS        - Message queue (LocalStack)"
    echo "  • DynamoDB   - NoSQL database (LocalStack)"
    clear_line
}

print_summary() {
    clear_line
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║                    TEST SUMMARY                          ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
    clear_line

    echo -e "Suite Results:"
    echo -e "$SUITE_RESULTS"
    clear_line

    echo -e "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "  ${GREEN}Passed:${NC}  $TOTAL_PASSED"
    echo -e "  ${RED}Failed:${NC}  $TOTAL_FAILED"
    echo -e "  ${YELLOW}Skipped:${NC} $TOTAL_SKIPPED"
    echo -e "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    if [ -n "$FAILED_SUITES" ]; then
        clear_line
        echo -e "${RED}Failed suites:$FAILED_SUITES${NC}"
    fi

    clear_line
}

# Main execution
print_header
print_test_plan

# Ensure scripts are executable
chmod +x "$SCRIPT_DIR"/*.sh 2>/dev/null || true

# Run all test suites
# Core databases
run_test_suite "PostgreSQL" "$SCRIPT_DIR/test-postgres.sh" || true
run_test_suite "MySQL" "$SCRIPT_DIR/test-mysql.sh" || true
run_test_suite "MongoDB" "$SCRIPT_DIR/test-mongodb.sh" || true

# Message queues
run_test_suite "Kafka" "$SCRIPT_DIR/test-kafka.sh" || true

# AWS services (LocalStack)
run_test_suite "S3" "$SCRIPT_DIR/test-s3.sh" || true
run_test_suite "SQS" "$SCRIPT_DIR/test-sqs.sh" || true
run_test_suite "DynamoDB" "$SCRIPT_DIR/test-dynamodb.sh" || true

# Print summary
print_summary

# Exit with appropriate code
if [ $TOTAL_FAILED -gt 0 ]; then
    echo -e "${RED}Some tests failed. Please review the output above.${NC}"
    exit 1
fi

echo -e "${GREEN}All test suites completed successfully!${NC}"
exit 0
