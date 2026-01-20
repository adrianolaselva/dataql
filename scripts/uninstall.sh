#!/usr/bin/env bash

#
#   DataQL Uninstallation Script
#   Supports: Linux, macOS
#
#   Usage:
#     curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/uninstall.sh | bash
#

set -e

# Configuration
BINARY_NAME="dataql"
INSTALL_DIR="/usr/local/bin"
LOCAL_INSTALL_DIR="${HOME}/.local/bin"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

# Additional locations to check
ADDITIONAL_LOCATIONS=(
    "/usr/bin"
    "${HOME}/bin"
    "${HOME}/.dataql/bin"
)

# Remove binary from a location with verification
remove_binary() {
    local path="$1"
    local dir
    dir=$(dirname "$path")

    if [ -w "$dir" ]; then
        rm -f "$path"
    else
        sudo rm -f "$path"
    fi

    # Verify removal
    if [ -f "$path" ]; then
        error "Failed to remove ${path}"
        return 1
    else
        success "Removed ${path}"
        return 0
    fi
}

# Clear shell command cache
clear_shell_cache() {
    # Clear bash hash table
    hash -r 2>/dev/null || true

    local shell_name
    shell_name=$(basename "$SHELL")

    echo ""
    echo "Shell cache cleared. Additional steps for your shell:"
    case "$shell_name" in
        bash)
            echo "  Run: hash -r"
            ;;
        zsh)
            echo "  Run: rehash"
            ;;
        fish)
            echo "  Restart your terminal"
            ;;
        *)
            echo "  Restart your terminal or run: hash -r"
            ;;
    esac
}

# Find and remove dataql
uninstall() {
    local found=false
    local removal_failed=false

    # Check system installation
    if [ -f "${INSTALL_DIR}/${BINARY_NAME}" ]; then
        info "Found ${BINARY_NAME} in ${INSTALL_DIR}"
        if ! remove_binary "${INSTALL_DIR}/${BINARY_NAME}"; then
            removal_failed=true
        fi
        found=true
    fi

    # Check user installation
    if [ -f "${LOCAL_INSTALL_DIR}/${BINARY_NAME}" ]; then
        info "Found ${BINARY_NAME} in ${LOCAL_INSTALL_DIR}"
        if ! remove_binary "${LOCAL_INSTALL_DIR}/${BINARY_NAME}"; then
            removal_failed=true
        fi
        found=true
    fi

    # Check additional locations
    for loc in "${ADDITIONAL_LOCATIONS[@]}"; do
        if [ -f "${loc}/${BINARY_NAME}" ]; then
            info "Found ${BINARY_NAME} in ${loc}"
            if ! remove_binary "${loc}/${BINARY_NAME}"; then
                removal_failed=true
            fi
            found=true
        fi
    done

    # Check if it's somewhere else in PATH
    local other_location
    other_location=$(which "$BINARY_NAME" 2>/dev/null || true)

    if [ -n "$other_location" ]; then
        warn "Found additional installation at: ${other_location}"
        echo "You may want to remove it manually"
    fi

    # Clear shell cache
    clear_shell_cache

    if [ "$found" = false ]; then
        warn "${BINARY_NAME} is not installed in standard locations"
        echo "Checked: ${INSTALL_DIR}, ${LOCAL_INSTALL_DIR}"
        for loc in "${ADDITIONAL_LOCATIONS[@]}"; do
            echo "         ${loc}"
        done
    elif [ "$removal_failed" = true ]; then
        echo ""
        error "Some removals failed. Please check permissions and try again."
        exit 1
    else
        echo ""
        success "${BINARY_NAME} has been uninstalled"

        # Final verification
        if command -v "$BINARY_NAME" &> /dev/null; then
            warn "Note: ${BINARY_NAME} is still found in PATH at: $(which $BINARY_NAME 2>/dev/null)"
            echo "You may need to restart your shell or remove it manually."
        fi
    fi
}

main() {
    echo ""
    echo "DataQL Uninstaller"
    echo "=================="
    echo ""

    uninstall
}

main "$@"
