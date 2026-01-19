#!/bin/bash
# DataQL E2E Tests - S3 (via LocalStack)
#
# STATUS: SKIPPED
#
# These tests are currently skipped due to LocalStack limitations:
# - LocalStack uses path-style S3 addressing (http://localhost:4566/bucket/key)
# - DataQL's S3 handler uses virtual-hosted-style addressing (bucket.s3.amazonaws.com)
# - Virtual-hosted-style doesn't work with LocalStack without additional DNS configuration
#
# To run S3 tests against real AWS:
# 1. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
# 2. Create a test bucket
# 3. Upload test fixtures
# 4. Update DATAQL_TEST_S3_* variables
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
echo "DataQL E2E Tests - S3 (LocalStack)"
echo "======================================"
echo ""

log_section "Status"
log_skip "S3 tests skipped - LocalStack requires path-style S3 addressing"
echo ""
echo "  LocalStack Limitation:"
echo "    DataQL uses virtual-hosted-style S3 addressing (bucket.s3.region.amazonaws.com)"
echo "    LocalStack requires path-style addressing (localhost:4566/bucket/key)"
echo ""
echo "  To test S3 functionality:"
echo "    1. Use real AWS S3 with proper credentials"
echo "    2. Or configure LocalStack with DNS tricks for virtual-hosted-style"
echo ""
echo "  The S3 implementation has unit tests that cover functionality."
echo "  See: pkg/s3handler/s3handler_test.go"
echo ""

echo "======================================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}, ${YELLOW}$SKIPPED skipped${NC}"
echo "======================================"

exit 0
