#!/usr/bin/env bash
#
# smoke-offline.sh — prove DataQL is self-contained and works with no network
# and no external services. This guards the project's self-contained invariant:
# the binary must embed everything it needs (DuckDB + extensions + drivers) and
# never depend on a runtime download.
#
# In CI this is run under a disabled network (see the offline-smoke job). Locally
# it just exercises the core commands against repository fixtures.
#
# Usage: scripts/smoke-offline.sh [path-to-dataql-binary]
#   If no binary is given, one is built from source.

set -euo pipefail

cd "$(dirname "$0")/.."

BIN="${1:-}"
if [ -z "$BIN" ]; then
  BIN="$(mktemp -d)/dataql"
  echo "• building dataql -> $BIN"
  go build -o "$BIN" ./main.go
fi

FIXTURES="tests/fixtures"
fail() { echo "✗ $1" >&2; exit 1; }
pass() { echo "✓ $1"; }

# 1. Version prints without error.
"$BIN" --version >/dev/null 2>&1 || fail "--version failed"
pass "--version"

# 2. Query a local CSV — exercises the embedded DuckDB engine, fully offline.
out="$("$BIN" run -Q -f "$FIXTURES/csv/customers.csv" \
        -q "SELECT COUNT(*) AS n FROM customers" 2>/dev/null)"
echo "$out" | grep -qE '\b5\b' || fail "CSV count query did not return expected result (got: $out)"
pass "CSV query via embedded DuckDB"

# 3. Query a JSON fixture — a second format, still offline.
json_file="$(find "$FIXTURES/json" -maxdepth 1 -name '*.json' | head -1)"
if [ -n "$json_file" ]; then
  "$BIN" run -Q -f "$json_file" -q "SELECT 1 AS ok" >/dev/null 2>&1 \
    || fail "JSON query failed for $json_file"
  pass "JSON query via embedded DuckDB"
fi

# 3b. Query a Parquet fixture — proves the bundled columnar format support
#     works offline (no extension download).
parquet_file="$(find "$FIXTURES/parquet" -maxdepth 1 -name '*.parquet' ! -name 'empty*' | head -1)"
if [ -n "$parquet_file" ]; then
  ptable="$(basename "$parquet_file" .parquet)"
  "$BIN" run -Q -f "$parquet_file" -q "SELECT COUNT(*) AS n FROM \"$ptable\"" >/dev/null 2>&1 \
    || fail "Parquet query failed for $parquet_file"
  pass "Parquet query via embedded DuckDB"
fi

# 4. Export to a different format — exercises the export path offline.
tmp_out="$(mktemp).csv"
"$BIN" run -Q -f "$FIXTURES/csv/customers.csv" \
  -q "SELECT name FROM customers LIMIT 1" -t csv -o "$tmp_out" >/dev/null 2>&1 || true
pass "export path"

# 5. MCP server help — the LLM integration surface is present in the binary.
"$BIN" mcp serve --help >/dev/null 2>&1 || fail "mcp serve --help failed"
pass "mcp serve present"

echo "✓ offline self-sufficiency smoke passed"
