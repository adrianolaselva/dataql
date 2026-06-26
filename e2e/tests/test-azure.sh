#!/bin/bash
# DataQL E2E Tests - Azure Blob Storage (via azurite)
#
# Seeds a container/blob in azurite (using the az CLI) and queries it through the
# real dataql binary via the AZURE_STORAGE_CONNECTION_STRING. The dataql Azure
# handler creates its client from that connection string, whose BlobEndpoint
# points at azurite.
#
# Skips gracefully when the az CLI or azurite is unavailable.

set +e  # do not abort the suite on a single non-zero command; tests track pass/fail via counters

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$E2E_DIR")"

if [ -f "$E2E_DIR/.env" ]; then set -a; source "$E2E_DIR/.env"; set +a; fi

DATAQL_BIN="${DATAQL_BIN:-$PROJECT_DIR/dataql}"
CONNSTR="${AZURE_STORAGE_CONNECTION_STRING:-}"
CONTAINER="${AZURE_TEST_CONTAINER:-dataql-test}"
PASSED=0; FAILED=0
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log_pass() { echo -e "  ${GREEN}[PASS]${NC} $1"; ((PASSED++)) || true; }
log_fail() { echo -e "  ${RED}[FAIL]${NC} $1"; [ -n "$2" ] && echo -e "         ${RED}$2${NC}"; ((FAILED++)) || true; }

echo "=== Azure Blob (azurite) E2E ==="
[ -x "$DATAQL_BIN" ] || { echo "dataql binary not found ($DATAQL_BIN)"; exit 1; }

# Seeding azurite uses the az CLI (present on CI runners). Skip if unavailable.
if ! command -v az >/dev/null 2>&1; then
  echo -e "  ${YELLOW}[SKIP]${NC} az CLI not installed; cannot seed azurite"
  exit 0
fi
if [ -z "$CONNSTR" ]; then
  echo -e "  ${YELLOW}[SKIP]${NC} AZURE_STORAGE_CONNECTION_STRING not set"
  exit 0
fi

# Wait briefly for azurite to accept connections (the blob endpoint returns 400
# for an unauthenticated root GET, which still proves it is listening).
AZURITE_HOST="${AZURITE_HOST:-127.0.0.1}"; AZURITE_PORT="${AZURITE_PORT:-20000}"
ready=false
for _ in $(seq 1 15); do
  if curl -s -o /dev/null "http://${AZURITE_HOST}:${AZURITE_PORT}/devstoreaccount1" 2>/dev/null; then ready=true; break; fi
  sleep 2
done
if [ "$ready" != "true" ]; then
  echo -e "  ${YELLOW}[SKIP]${NC} azurite not reachable at ${AZURITE_HOST}:${AZURITE_PORT}"
  exit 0
fi

# Seed: container + blob (idempotent). Surface az errors to the log to aid
# diagnosis; still skip (advisory) rather than fail the whole suite on a seed
# hiccup.
tmp="$(mktemp).csv"; printf 'id,name,age\n1,Alice,28\n2,Bob,35\n3,Charlie,42\n' > "$tmp"
az storage container create --name "$CONTAINER" --connection-string "$CONNSTR" >/dev/null 2>&1
seed_out="$(az storage blob upload --container-name "$CONTAINER" --name simple.csv --file "$tmp" \
              --connection-string "$CONNSTR" --overwrite 2>&1)"
seed_rc=$?
rm -f "$tmp"
if [ "$seed_rc" -ne 0 ]; then
  echo -e "  ${YELLOW}[SKIP]${NC} azurite seed via az CLI failed:"
  echo "$seed_out" | tail -3 | sed 's/^/         /'
  exit 0
fi

out="$("$DATAQL_BIN" run -Q -f "azure://${CONTAINER}/simple.csv" \
        -q "SELECT name, age FROM simple WHERE age > 30" 2>/dev/null)"
if echo "$out" | grep -q "Bob" && echo "$out" | grep -q "Charlie"; then
  log_pass "Azure blob read + numeric filter"
else
  log_fail "Azure query failed" "got: $out"
fi

echo ""
echo "Azure E2E: ${PASSED} passed, ${FAILED} failed"
[ "$FAILED" -eq 0 ]
