#!/bin/bash
# DataQL Installation Verification Script
# This script verifies that DataQL is properly installed and all commands are working

set -e

echo "=========================================="
echo "DataQL Installation Verification"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counter for errors
ERRORS=0

# Function to print success
success() {
    echo -e "${GREEN}✓${NC} $1"
}

# Function to print error
error() {
    echo -e "${RED}✗${NC} $1"
    ERRORS=$((ERRORS + 1))
}

# Function to print warning
warning() {
    echo -e "${YELLOW}!${NC} $1"
}

# Check if dataql is in PATH
echo "Checking DataQL installation..."
echo ""

if command -v dataql &> /dev/null; then
    success "dataql found in PATH"
    DATAQL_PATH=$(which dataql)
    echo "  Location: $DATAQL_PATH"
else
    error "dataql not found in PATH"
    echo "  Please ensure dataql is installed and in your PATH"
    echo "  You can install it with: make install"
    exit 1
fi

echo ""

# Check version
echo "Checking version..."
if dataql --version &> /dev/null; then
    VERSION=$(dataql --version 2>&1)
    success "Version command works"
    echo "  Version: $VERSION"
else
    error "Version command failed"
fi

echo ""

# Check main commands
echo "Checking main commands..."

# Check 'run' command
if dataql run --help &> /dev/null; then
    success "run command is available"
else
    error "run command not working"
fi

# Check 'skills' command
if dataql skills --help &> /dev/null; then
    success "skills command is available"
else
    error "skills command not working"
fi

# Check 'mcp' command - CRITICAL
if dataql mcp --help &> /dev/null; then
    success "mcp command is available"
else
    error "mcp command not working (CRITICAL - needed for LLM integration)"
fi

echo ""

# Check subcommands
echo "Checking subcommands..."

# Skills subcommands
if dataql skills install --help &> /dev/null; then
    success "skills install subcommand is available"
else
    error "skills install subcommand not working"
fi

if dataql skills list --help &> /dev/null; then
    success "skills list subcommand is available"
else
    error "skills list subcommand not working"
fi

if dataql skills uninstall --help &> /dev/null; then
    success "skills uninstall subcommand is available"
else
    error "skills uninstall subcommand not working"
fi

# MCP subcommands
if dataql mcp serve --help &> /dev/null; then
    success "mcp serve subcommand is available"
else
    error "mcp serve subcommand not working (CRITICAL)"
fi

echo ""

# Check help output doesn't contain "unknown command"
echo "Checking for command registration issues..."

HELP_OUTPUT=$(dataql --help 2>&1)
if echo "$HELP_OUTPUT" | grep -qi "unknown command"; then
    error "Help output contains 'unknown command' error"
else
    success "No 'unknown command' errors in help output"
fi

# Verify all expected commands appear in help
if echo "$HELP_OUTPUT" | grep -q "run"; then
    success "'run' command listed in help"
else
    error "'run' command not listed in help"
fi

if echo "$HELP_OUTPUT" | grep -q "skills"; then
    success "'skills' command listed in help"
else
    error "'skills' command not listed in help"
fi

if echo "$HELP_OUTPUT" | grep -q "mcp"; then
    success "'mcp' command listed in help"
else
    error "'mcp' command not listed in help"
fi

echo ""
echo "=========================================="

# Summary
if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}All checks passed!${NC}"
    echo "DataQL is properly installed and all commands are working."
    exit 0
else
    echo -e "${RED}$ERRORS error(s) found!${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "1. Try rebuilding: make build"
    echo "2. Try reinstalling: make install"
    echo "3. Check if the binary is up to date"
    echo "4. Run 'dataql --help' to see available commands"
    exit 1
fi
