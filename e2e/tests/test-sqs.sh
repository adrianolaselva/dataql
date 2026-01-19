#!/bin/bash
# DataQL E2E Tests - SQS (via LocalStack)
#
# STATUS: SKIPPED
#
# These tests are currently skipped due to LocalStack endpoint configuration limitations:
# - DataQL's SQS handler requires specific AWS endpoint configuration
# - LocalStack endpoint format differs from standard AWS SQS endpoints
# - Additional SDK configuration would be needed for LocalStack compatibility
#
# To run SQS tests against real AWS:
# 1. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
# 2. Create a test queue
# 3. Send test messages
# 4. Update DATAQL_TEST_SQS_URL variable
# 5. Remove the skip exit below

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

# ==============================================================================
# PRINT SKIP MESSAGE AND EXIT
# ==============================================================================

echo "======================================"
echo "DataQL E2E Tests - SQS (LocalStack)"
echo "======================================"
echo ""

log_section "Status"
log_skip "SQS tests skipped - LocalStack endpoint configuration"
echo ""
echo "  LocalStack Limitation:"
echo "    DataQL's SQS handler uses standard AWS SDK configuration"
echo "    LocalStack requires custom endpoint configuration"
echo "    URL format: sqs://queue-name?region=us-east-1&endpoint=http://localhost:24566"
echo ""
echo "  To test SQS functionality:"
echo "    1. Use real AWS SQS with proper credentials"
echo "    2. Or implement custom endpoint support in mqreader/sqs"
echo ""
echo "  The SQS implementation has unit tests that cover functionality."
echo "  See: pkg/mqreader/sqs/sqs_test.go"
echo ""

echo "======================================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}, ${YELLOW}$SKIPPED skipped${NC}"
echo "======================================"

exit 0
