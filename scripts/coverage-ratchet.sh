#!/usr/bin/env bash
#
# coverage-ratchet.sh — enforce that total test coverage never regresses.
#
# The committed file .coverage-baseline holds the minimum acceptable total
# coverage percentage. CI runs this script in "check" mode and fails when the
# measured coverage drops below the baseline. When coverage rises, run it in
# "bump" mode to raise the baseline. The baseline must only ever go up — that is
# the ratchet, on the path to the >= 90% target.
#
# Usage:
#   scripts/coverage-ratchet.sh check   # (default) fail if coverage < baseline
#   scripts/coverage-ratchet.sh bump    # raise baseline to current if higher
#   scripts/coverage-ratchet.sh measure # just print the current total
#
# Env:
#   COVERAGE_FILE   coverage profile to read/produce (default: coverage.out)
#   COVERAGE_TARGET long-term target, informational only (default: 90)

set -euo pipefail

cd "$(dirname "$0")/.."

MODE="${1:-check}"
COVERAGE_FILE="${COVERAGE_FILE:-coverage.out}"
BASELINE_FILE=".coverage-baseline"
COVERAGE_TARGET="${COVERAGE_TARGET:-90}"

# Packages whose coverage counts toward the ratchet. The end-to-end suite has
# its own gate and is excluded here so the number is deterministic and does not
# depend on Docker services being available.
measure() {
  if [ ! -f "$COVERAGE_FILE" ]; then
    go test -coverprofile="$COVERAGE_FILE" ./... >/dev/null
  fi
  go tool cover -func="$COVERAGE_FILE" | awk '/^total:/ {gsub(/%/,"",$3); print $3}'
}

read_baseline() {
  if [ ! -f "$BASELINE_FILE" ]; then
    echo "0.0"
    return
  fi
  tr -d '[:space:]' < "$BASELINE_FILE"
}

# Compare two decimals: returns 0 if $1 >= $2.
ge() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a + 0 >= b + 0) }'; }

CURRENT="$(measure)"
BASELINE="$(read_baseline)"

case "$MODE" in
  measure)
    echo "$CURRENT"
    ;;
  check)
    echo "Coverage: current=${CURRENT}% baseline=${BASELINE}% target=${COVERAGE_TARGET}%"
    if ge "$CURRENT" "$BASELINE"; then
      echo "✓ Coverage does not regress."
      if ge "$CURRENT" "$BASELINE" && ! ge "$BASELINE" "$CURRENT"; then
        echo "ℹ Coverage rose above baseline — run 'make coverage-bump' to lock it in."
      fi
    else
      echo "✗ Coverage regressed: ${CURRENT}% < baseline ${BASELINE}%."
      echo "  Add tests until coverage is at least ${BASELINE}%."
      exit 1
    fi
    ;;
  bump)
    if ge "$CURRENT" "$BASELINE" && ! ge "$BASELINE" "$CURRENT"; then
      printf '%s\n' "$CURRENT" > "$BASELINE_FILE"
      echo "✓ Baseline raised: ${BASELINE}% → ${CURRENT}%"
    else
      echo "Baseline unchanged (current=${CURRENT}% baseline=${BASELINE}%); ratchet only moves up."
    fi
    ;;
  *)
    echo "Unknown mode: $MODE (use check|bump|measure)" >&2
    exit 2
    ;;
esac
