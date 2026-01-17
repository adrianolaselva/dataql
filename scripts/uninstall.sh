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

# Find and remove dataql
uninstall() {
    local found=false

    # Check system installation
    if [ -f "${INSTALL_DIR}/${BINARY_NAME}" ]; then
        info "Found ${BINARY_NAME} in ${INSTALL_DIR}"

        if [ -w "${INSTALL_DIR}" ]; then
            rm -f "${INSTALL_DIR}/${BINARY_NAME}"
        else
            sudo rm -f "${INSTALL_DIR}/${BINARY_NAME}"
        fi

        success "Removed ${INSTALL_DIR}/${BINARY_NAME}"
        found=true
    fi

    # Check user installation
    if [ -f "${LOCAL_INSTALL_DIR}/${BINARY_NAME}" ]; then
        info "Found ${BINARY_NAME} in ${LOCAL_INSTALL_DIR}"
        rm -f "${LOCAL_INSTALL_DIR}/${BINARY_NAME}"
        success "Removed ${LOCAL_INSTALL_DIR}/${BINARY_NAME}"
        found=true
    fi

    # Check if it's somewhere else in PATH
    local other_location
    other_location=$(which "$BINARY_NAME" 2>/dev/null || true)

    if [ -n "$other_location" ]; then
        warn "Found additional installation at: ${other_location}"
        echo "You may want to remove it manually"
    fi

    if [ "$found" = false ]; then
        warn "${BINARY_NAME} is not installed in standard locations"
        echo "Checked: ${INSTALL_DIR}, ${LOCAL_INSTALL_DIR}"
    else
        echo ""
        success "${BINARY_NAME} has been uninstalled"
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
