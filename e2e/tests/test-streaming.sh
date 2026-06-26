#!/bin/bash
# DataQL E2E Tests - Streaming (dataql run --follow over Kafka)
#
# Produces messages to a Kafka topic, then consumes them with `--follow` in both
# emit mode and windowed-SQL mode, bounded by --max-messages and --duration so
# the run terminates deterministically. Skips gracefully if Kafka is unreachable.

set +e  # do not abort the suite on a single non-zero command; tests track pass/fail via counters

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$E2E_DIR")"
if [ -f "$E2E_DIR/.env" ]; then set -a; source "$E2E_DIR/.env"; set +a; fi

DATAQL_BIN="${DATAQL_BIN:-$PROJECT_DIR/dataql}"
TOPIC="dataql-stream-topic"
KAFKA_URL="kafka://localhost:29092/${TOPIC}"
PASSED=0; FAILED=0
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log_pass() { echo -e "  ${GREEN}[PASS]${NC} $1"; ((PASSED++)) || true; }
log_fail() { echo -e "  ${RED}[FAIL]${NC} $1"; [ -n "$2" ] && echo -e "         ${RED}$2${NC}"; ((FAILED++)) || true; }

echo "=== Streaming (--follow over Kafka) E2E ==="
[ -x "$DATAQL_BIN" ] || { echo "dataql binary not found ($DATAQL_BIN)"; exit 1; }

if ! docker exec dataql-kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
  echo -e "  ${YELLOW}[SKIP]${NC} Kafka not reachable"
  exit 0
fi

# Create the topic and produce messages with a "level" field.
docker exec dataql-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1 --if-not-exists >/dev/null 2>&1
printf '%s\n' \
  '{"level":"INFO","n":1}' '{"level":"ERROR","n":2}' \
  '{"level":"INFO","n":3}' '{"level":"INFO","n":4}' \
  | docker exec -i dataql-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic "$TOPIC" >/dev/null 2>&1
sleep 5 # let Kafka commit the messages

# Emit mode: each message as a JSON line. Bounded by --max-messages + --duration.
emit_out="$("$DATAQL_BIN" run -f "$KAFKA_URL" --follow --max-messages 4 --duration 30s 2>/dev/null)"
if echo "$emit_out" | grep -q '"level":"INFO"' && echo "$emit_out" | grep -q '"level":"ERROR"'; then
  log_pass "emit mode streams messages"
else
  log_fail "emit mode failed" "got: $emit_out"
fi

# Windowed SQL: rolling COUNT by level over the last 100 messages.
win_out="$("$DATAQL_BIN" run -f "$KAFKA_URL" --follow --window 100 --interval 300ms \
            --max-messages 4 --duration 30s \
            -q "SELECT level, COUNT(*) AS n FROM stream GROUP BY level ORDER BY level" 2>/dev/null)"
# The final emit aggregates the 4 messages: INFO=3, ERROR=1.
if echo "$win_out" | grep -q '"level":"INFO"' && echo "$win_out" | grep -q '"level":"ERROR"'; then
  log_pass "windowed SQL emits rolling aggregate"
else
  log_fail "windowed SQL failed" "got: $win_out"
fi

echo ""
echo "Streaming E2E: ${PASSED} passed, ${FAILED} failed"
[ "$FAILED" -eq 0 ]
