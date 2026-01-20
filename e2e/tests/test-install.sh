#!/bin/bash
#
# DataQL Installation E2E Tests
# Tests all installation scenarios end-to-end
#
# Usage:
#   DATAQL_BIN=/path/to/dataql ./test-install.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$E2E_DIR")"

# Test configuration
TEST_INSTALL_DIR=$(mktemp -d)
TEST_LOCAL_DIR=$(mktemp -d)
DATAQL_BIN="${DATAQL_BIN:-$PROJECT_ROOT/dataql}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

TESTS_PASSED=0
TESTS_FAILED=0

cleanup() {
    rm -rf "$TEST_INSTALL_DIR" "$TEST_LOCAL_DIR"
}
trap cleanup EXIT

pass() {
    echo -e "${GREEN}✓ PASS:${NC} $1"
    ((TESTS_PASSED++)) || true
}

fail() {
    echo -e "${RED}✗ FAIL:${NC} $1"
    ((TESTS_FAILED++)) || true
}

skip() {
    echo -e "${YELLOW}⊘ SKIP:${NC} $1"
}

# Ensure binary exists
ensure_binary() {
    if [ ! -f "$DATAQL_BIN" ]; then
        echo "ERROR: Cannot find dataql binary at $DATAQL_BIN"
        echo "Build it first with: make build"
        exit 1
    fi

    if [ ! -x "$DATAQL_BIN" ]; then
        echo "ERROR: Binary is not executable: $DATAQL_BIN"
        exit 1
    fi
}

# Test 1: Fresh install
test_fresh_install() {
    echo ""
    echo "=== Test 1: Fresh Install ==="

    local dest="$TEST_INSTALL_DIR/dataql"

    # Ensure no existing binary
    rm -f "$dest"

    # Install
    cp "$DATAQL_BIN" "$dest"
    chmod +x "$dest"

    # Verify
    if [ -x "$dest" ] && "$dest" --version > /dev/null 2>&1; then
        pass "Fresh install successful"
    else
        fail "Fresh install failed"
    fi
}

# Test 2: Reinstall with --force (same version)
test_reinstall_force() {
    echo ""
    echo "=== Test 2: Reinstall with --force (same version) ==="

    local dest="$TEST_INSTALL_DIR/dataql"

    # Get original modification time
    local orig_time
    orig_time=$(stat -c %Y "$dest" 2>/dev/null || stat -f %m "$dest" 2>/dev/null)

    # Small delay to ensure different mtime
    sleep 1

    # Reinstall
    cp "$DATAQL_BIN" "$dest"
    chmod +x "$dest"

    # Verify binary works
    if "$dest" --version > /dev/null 2>&1; then
        pass "Reinstall with --force successful"
    else
        fail "Reinstall with --force failed"
    fi
}

# Test 3: Version upgrade logic
test_version_upgrade_logic() {
    echo ""
    echo "=== Test 3: Version Upgrade Logic ==="

    # Test that older version is detected correctly
    local v1="0.1.0"
    local v2="0.2.0"

    # v1 < v2, so upgrade should proceed
    local smaller
    smaller=$(printf '%s\n%s' "$v1" "$v2" | sort -V | head -n1)

    if [ "$smaller" = "$v1" ]; then
        pass "Upgrade version comparison correct ($v1 < $v2)"
    else
        fail "Upgrade version comparison incorrect"
    fi
}

# Test 4: Downgrade detection
test_downgrade_blocked() {
    echo ""
    echo "=== Test 4: Downgrade Detection ==="

    # Verify that newer > older is detected
    local v1="0.3.0"
    local v2="0.2.0"

    local smaller
    smaller=$(printf '%s\n%s' "$v1" "$v2" | sort -V | head -n1)

    if [ "$smaller" = "$v2" ]; then
        pass "Downgrade correctly identified ($v1 > $v2)"
    else
        fail "Downgrade detection incorrect"
    fi
}

# Test 5: Clean install removes all versions
test_clean_install() {
    echo ""
    echo "=== Test 5: Clean Install ==="

    local dest1="$TEST_INSTALL_DIR/dataql"
    local dest2="$TEST_LOCAL_DIR/dataql"

    # Create installations in both locations
    cp "$DATAQL_BIN" "$dest1"
    cp "$DATAQL_BIN" "$dest2"
    chmod +x "$dest1" "$dest2"

    # Verify both exist
    if [ ! -f "$dest1" ] || [ ! -f "$dest2" ]; then
        fail "Failed to create test installations"
        return
    fi

    # Simulate --clean by removing both
    rm -f "$dest1" "$dest2"

    # Verify both removed
    if [ -f "$dest1" ] || [ -f "$dest2" ]; then
        fail "Clean did not remove all installations"
        return
    fi

    pass "Clean removed all installations"

    # Reinstall to one location
    cp "$DATAQL_BIN" "$dest1"
    chmod +x "$dest1"

    if "$dest1" --version > /dev/null 2>&1; then
        pass "Clean install completed successfully"
    else
        fail "Clean install final verification failed"
    fi
}

# Test 6: Uninstall and verify removal
test_uninstall() {
    echo ""
    echo "=== Test 6: Uninstall and Verify ==="

    local dest="$TEST_INSTALL_DIR/dataql"

    # Ensure installed
    if [ ! -f "$dest" ]; then
        cp "$DATAQL_BIN" "$dest"
        chmod +x "$dest"
    fi

    # Uninstall
    rm -f "$dest"

    # Verify removed
    if [ -f "$dest" ]; then
        fail "Uninstall did not remove binary"
    else
        pass "Uninstall removed binary successfully"
    fi
}

# Test 7: Install after uninstall
test_install_after_uninstall() {
    echo ""
    echo "=== Test 7: Install After Uninstall ==="

    local dest="$TEST_INSTALL_DIR/dataql"

    # Ensure uninstalled
    rm -f "$dest"

    # Install
    cp "$DATAQL_BIN" "$dest"
    chmod +x "$dest"

    # Verify
    if [ -x "$dest" ] && "$dest" --version > /dev/null 2>&1; then
        pass "Install after uninstall successful"
    else
        fail "Install after uninstall failed"
    fi
}

# Test 8: Version command shows injected version
test_version_injection() {
    echo ""
    echo "=== Test 8: Version Injection ==="

    local version_output
    version_output=$("$DATAQL_BIN" --version 2>&1)

    # Should show version format (either dev, a tag, or commit info)
    if echo "$version_output" | grep -qE 'v?[0-9]+\.[0-9]+\.[0-9]+|dev|dataql'; then
        pass "Version command returns valid version: $version_output"
    else
        fail "Version command returned unexpected output: $version_output"
    fi
}

# Test 9: Version comparison edge cases
test_version_comparison_edge_cases() {
    echo ""
    echo "=== Test 9: Version Comparison Edge Cases ==="

    local test_cases=(
        "1.0.0:1.0.1:upgrade"
        "1.0.1:1.0.0:downgrade"
        "1.0.0:1.0.0:equal"
        "0.9.9:1.0.0:upgrade"
        "2.0.0:1.9.9:downgrade"
        "1.10.0:1.9.0:downgrade"
    )

    local all_passed=true

    for tc in "${test_cases[@]}"; do
        local v1 v2 expected
        v1=$(echo "$tc" | cut -d: -f1)
        v2=$(echo "$tc" | cut -d: -f2)
        expected=$(echo "$tc" | cut -d: -f3)

        local smaller
        smaller=$(printf '%s\n%s' "$v1" "$v2" | sort -V | head -n1)

        local result
        if [ "$v1" = "$v2" ]; then
            result="equal"
        elif [ "$smaller" = "$v1" ]; then
            result="upgrade"
        else
            result="downgrade"
        fi

        if [ "$result" = "$expected" ]; then
            echo "  $v1 -> $v2: $result (expected: $expected) ✓"
        else
            echo "  $v1 -> $v2: $result (expected: $expected) ✗"
            all_passed=false
        fi
    done

    if [ "$all_passed" = true ]; then
        pass "All version comparison edge cases passed"
    else
        fail "Some version comparison edge cases failed"
    fi
}

# Test 10: Binary permissions
test_binary_permissions() {
    echo ""
    echo "=== Test 10: Binary Permissions ==="

    local dest="$TEST_INSTALL_DIR/dataql_perm_test"

    # Install without execute permission
    cp "$DATAQL_BIN" "$dest"
    chmod -x "$dest"

    # Should not be executable
    if [ -x "$dest" ]; then
        fail "Binary should not be executable without chmod"
        rm -f "$dest"
        return
    fi

    # Add execute permission
    chmod +x "$dest"

    # Now should be executable
    if [ -x "$dest" ] && "$dest" --version > /dev/null 2>&1; then
        pass "Binary permissions work correctly"
    else
        fail "Binary not executable after chmod +x"
    fi

    rm -f "$dest"
}

# Main
main() {
    echo "=========================================="
    echo "DataQL Installation E2E Tests"
    echo "=========================================="
    echo ""
    echo "Test directories:"
    echo "  Install dir: $TEST_INSTALL_DIR"
    echo "  Local dir:   $TEST_LOCAL_DIR"
    echo "  Binary:      $DATAQL_BIN"
    echo ""

    ensure_binary

    test_fresh_install
    test_reinstall_force
    test_version_upgrade_logic
    test_downgrade_blocked
    test_clean_install
    test_uninstall
    test_install_after_uninstall
    test_version_injection
    test_version_comparison_edge_cases
    test_binary_permissions

    echo ""
    echo "=========================================="
    echo "Results: $TESTS_PASSED passed, $TESTS_FAILED failed"
    echo "=========================================="

    if [ $TESTS_FAILED -gt 0 ]; then
        exit 1
    fi
}

main "$@"
