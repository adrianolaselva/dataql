#!/bin/bash
# DataQL E2E Tests - Google Cloud Storage (via fake-gcs-server)
#
# Seeds a bucket/object in the GCS emulator (no auth) and queries it through the
# real dataql binary using STORAGE_EMULATOR_HOST.

set +e  # do not abort the suite on a single non-zero command; tests track pass/fail via counters

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$E2E_DIR")"

# Load env (STORAGE_EMULATOR_HOST, bucket, etc.)
if [ -f "$E2E_DIR/.env" ]; then set -a; source "$E2E_DIR/.env"; set +a; fi

DATAQL_BIN="${DATAQL_BIN:-$PROJECT_DIR/dataql}"
EMU="${STORAGE_EMULATOR_HOST:-localhost:24443}"
BUCKET="${GCS_TEST_BUCKET:-dataql-test-bucket}"
PASSED=0; FAILED=0
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log_pass() { echo -e "  ${GREEN}[PASS]${NC} $1"; ((PASSED++)) || true; }
log_fail() { echo -e "  ${RED}[FAIL]${NC} $1"; [ -n "$2" ] && echo -e "         ${RED}$2${NC}"; ((FAILED++)) || true; }

echo "=== GCS (fake-gcs-server) E2E ==="
[ -x "$DATAQL_BIN" ] || { echo "dataql binary not found ($DATAQL_BIN)"; exit 1; }

# Skip gracefully if the emulator isn't up.
if ! curl -sf "http://${EMU}/storage/v1/b?project=test" >/dev/null 2>&1; then
  echo -e "  ${YELLOW}[SKIP]${NC} GCS emulator not reachable at ${EMU}"
  exit 0
fi

# Seed: create bucket (idempotent) + upload a CSV object.
curl -sf -X POST "http://${EMU}/storage/v1/b?project=test" \
  -H "Content-Type: application/json" -d "{\"name\":\"${BUCKET}\"}" >/dev/null 2>&1
curl -sf -X POST "http://${EMU}/upload/storage/v1/b/${BUCKET}/o?uploadType=media&name=simple.csv" \
  -H "Content-Type: text/csv" \
  --data-binary $'id,name,age\n1,Alice,28\n2,Bob,35\n3,Charlie,42' >/dev/null 2>&1

# Query the object through dataql (STORAGE_EMULATOR_HOST is exported from .env).
out="$(STORAGE_EMULATOR_HOST="$EMU" "$DATAQL_BIN" run -Q -f "gs://${BUCKET}/simple.csv" \
        -q "SELECT name, age FROM simple WHERE age > 30" 2>/dev/null)"
if echo "$out" | grep -q "Bob" && echo "$out" | grep -q "Charlie"; then
  log_pass "GCS object read + numeric filter"
else
  log_fail "GCS query failed" "got: $out"
fi

out2="$(STORAGE_EMULATOR_HOST="$EMU" "$DATAQL_BIN" run -Q -f "gs://${BUCKET}/simple.csv" \
         -q "SELECT COUNT(*) AS n FROM simple" 2>/dev/null)"
echo "$out2" | grep -qE '\b3\b' && log_pass "GCS row count" || log_fail "GCS count failed" "got: $out2"

echo ""
echo "GCS E2E: ${PASSED} passed, ${FAILED} failed"
[ "$FAILED" -eq 0 ]
