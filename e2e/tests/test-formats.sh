#!/bin/bash
# DataQL E2E Tests - File format matrix
#
# Runs the real dataql binary against a fixture for every shipped file format
# and asserts a query returns rows. This proves each format works end-to-end
# through the full load → query pipeline. No external services required.

set +e  # do not abort the suite on a single non-zero command; tests track pass/fail via counters

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$E2E_DIR")"
FIXTURES="$PROJECT_DIR/tests/fixtures"
DATAQL_BIN="${DATAQL_BIN:-$PROJECT_DIR/dataql}"

PASSED=0
FAILED=0
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log_pass() { echo -e "  ${GREEN}[PASS]${NC} $1"; ((PASSED++)) || true; }
log_fail() { echo -e "  ${RED}[FAIL]${NC} $1"; [ -n "$2" ] && echo -e "         ${RED}$2${NC}"; ((FAILED++)) || true; }

# format | fixture (relative to FIXTURES) | table name
MATRIX=(
  "CSV|csv/customers.csv|customers"
  "JSON|json/people.json|people"
  "JSONL|jsonl/simple.jsonl|simple"
  "Parquet|parquet/products.parquet|products"
  "Excel|excel/users.xlsx|users"
  "XML|xml/users.xml|users"
  "YAML|yaml/users.yaml|users"
  "Avro|avro/users.avro|users"
  "ORC|orc/products.orc|products"
)

echo "=== File format matrix E2E ==="
[ -x "$DATAQL_BIN" ] || { echo "dataql binary not found at $DATAQL_BIN (run 'make build')"; exit 1; }

for entry in "${MATRIX[@]}"; do
  IFS='|' read -r fmt file table <<< "$entry"
  path="$FIXTURES/$file"
  if [ ! -f "$path" ]; then
    log_fail "$fmt: fixture missing ($file)"
    continue
  fi
  out="$("$DATAQL_BIN" run -Q -f "$path" -q "SELECT COUNT(*) AS n FROM $table" 2>/dev/null)"
  # Expect a positive row count in the output.
  if echo "$out" | grep -qE '\b[1-9][0-9]*\b'; then
    log_pass "$fmt query returned rows"
  else
    log_fail "$fmt query failed" "got: $out"
  fi
done

echo ""
echo "Format matrix: ${PASSED} passed, ${FAILED} failed"
[ "$FAILED" -eq 0 ]
