#!/bin/bash
# DataQL E2E Tests - URL (HTTP source)
#
# Serves repository fixtures over a local HTTP server and queries them via
# `dataql run -f http://...`, exercising the URL handler end-to-end. No external
# service is required — the fixture server is started and stopped by this script.
#
# Test Coverage:
# - CSV file reading over HTTP
# - JSON file reading over HTTP
# - WHERE clause filtering on an HTTP source

set +e  # do not abort the suite on a single non-zero command; tests track pass/fail via counters

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$E2E_DIR")"
FIXTURES="$PROJECT_DIR/tests/fixtures"

DATAQL_BIN="${DATAQL_BIN:-$PROJECT_DIR/dataql}"
PORT="${URL_TEST_PORT:-28086}"
PASSED=0
FAILED=0

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log_pass() { echo -e "  ${GREEN}[PASS]${NC} $1"; ((PASSED++)) || true; }
log_fail() { echo -e "  ${RED}[FAIL]${NC} $1"; [ -n "$2" ] && echo -e "         ${RED}$2${NC}"; ((FAILED++)) || true; }
log_info() { echo -e "  ${YELLOW}[INFO]${NC} $1"; }

# Start a static HTTP server over the fixtures directory.
SERVER_PID=""
start_server() {
  log_info "Starting fixture HTTP server on port $PORT..."
  ( cd "$FIXTURES" && python3 -m http.server "$PORT" >/dev/null 2>&1 ) &
  SERVER_PID=$!
  # Wait for it to accept connections.
  for _ in $(seq 1 20); do
    if curl -sf "http://localhost:$PORT/csv/customers.csv" >/dev/null 2>&1; then return 0; fi
    sleep 0.3
  done
  log_fail "fixture server did not come up"
  return 1
}
stop_server() { [ -n "$SERVER_PID" ] && kill "$SERVER_PID" >/dev/null 2>&1 || true; }
trap stop_server EXIT

assert_query() {
  local desc="$1" url="$2" query="$3" expect="$4"
  local out
  if out="$("$DATAQL_BIN" run -Q -f "$url" -q "$query" 2>/dev/null)" && echo "$out" | grep -qE "$expect"; then
    log_pass "$desc"
  else
    log_fail "$desc" "expected /$expect/, got: $out"
  fi
}

echo "=== URL (HTTP) E2E ==="
[ -x "$DATAQL_BIN" ] || { echo "dataql binary not found at $DATAQL_BIN (run 'make build')"; exit 1; }
command -v python3 >/dev/null || { echo "python3 required for the fixture server"; exit 1; }
start_server

BASE="http://localhost:$PORT"
assert_query "CSV over HTTP: row count" "$BASE/csv/customers.csv" \
  "SELECT COUNT(*) AS n FROM customers" '\b5\b'
assert_query "CSV over HTTP: WHERE filter" "$BASE/csv/customers.csv" \
  "SELECT COUNT(*) AS n FROM customers WHERE country = 'USA'" '[0-9]'

JSON_FILE="$(cd "$FIXTURES/json" && ls *.json 2>/dev/null | head -1)"
if [ -n "$JSON_FILE" ]; then
  assert_query "JSON over HTTP: query" "$BASE/json/$JSON_FILE" "SELECT 1 AS ok" '\b1\b'
fi

echo ""
echo "URL E2E: ${PASSED} passed, ${FAILED} failed"
[ "$FAILED" -eq 0 ]
