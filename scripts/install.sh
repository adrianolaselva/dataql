#!/usr/bin/env bash

#
#   DataQL Installation Script
#   Supports: Linux (amd64, arm64), macOS (amd64, arm64)
#
#   Usage:
#     curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash
#     curl -fsSL .../install.sh | bash -s -- --version v1.0.0
#     curl -fsSL .../install.sh | bash -s -- --local
#

set -e

# Configuration
REPO="adrianolaselva/dataql"
BINARY_NAME="dataql"
GITHUB_API="https://api.github.com/repos/${REPO}/releases"
GITHUB_DOWNLOAD="https://github.com/${REPO}/releases/download"

# Installation directories
INSTALL_DIR="/usr/local/bin"
LOCAL_INSTALL_DIR="${HOME}/.local/bin"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Default options
VERSION=""
LOCAL_INSTALL=false
FORCE=false

# Print functions
print_banner() {
    echo -e "${CYAN}"
    echo "  ____        _        ___  _     "
    echo " |  _ \\  __ _| |_ __ _/ _ \\| |    "
    echo " | | | |/ _\` | __/ _\` | | | | |    "
    echo " | |_| | (_| | || (_| | |_| | |___ "
    echo " |____/ \\__,_|\\__\\__,_|\\__\\_\\_____|"
    echo -e "${NC}"
    echo -e "${BOLD}Universal Data Query Tool${NC}"
    echo ""
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[OK]${NC} $1" >&2
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" >&2
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

die() {
    error "$1"
    exit 1
}

# Check for required commands
check_dependencies() {
    local missing=()

    for cmd in curl tar; do
        if ! command -v "$cmd" &> /dev/null; then
            missing+=("$cmd")
        fi
    done

    if [ ${#missing[@]} -ne 0 ]; then
        die "Missing required commands: ${missing[*]}"
    fi
}

# Detect operating system
detect_os() {
    local os
    os="$(uname -s)"

    case "$os" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        MINGW*|MSYS*|CYGWIN*)
            die "Windows detected. Please use the PowerShell installer instead:
    irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.ps1 | iex"
            ;;
        *)
            die "Unsupported operating system: $os"
            ;;
    esac
}

# Detect architecture
detect_arch() {
    local arch
    arch="$(uname -m)"

    case "$arch" in
        x86_64|amd64)  echo "amd64" ;;
        aarch64|arm64) echo "arm64" ;;
        armv7l)        echo "arm" ;;
        *)
            die "Unsupported architecture: $arch"
            ;;
    esac
}

# Get the latest release version from GitHub
get_latest_version() {
    local latest
    latest=$(curl -fsSL "${GITHUB_API}/latest" 2>/dev/null | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')

    if [ -z "$latest" ]; then
        die "Failed to fetch latest version from GitHub"
    fi

    echo "$latest"
}

# Global temp directory for cleanup
TMP_DIR=""

cleanup() {
    if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
        rm -rf "$TMP_DIR"
    fi
}

# Download and verify the binary
download_binary() {
    local version="$1"
    local os="$2"
    local arch="$3"

    TMP_DIR=$(mktemp -d)

    local archive_name="${BINARY_NAME}_${version#v}_${os}_${arch}.tar.gz"
    local download_url="${GITHUB_DOWNLOAD}/${version}/${archive_name}"
    local checksums_url="${GITHUB_DOWNLOAD}/${version}/checksums.txt"

    info "Downloading ${BINARY_NAME} ${version} for ${os}/${arch}..."

    # Download archive
    if ! curl -fsSL "$download_url" -o "${TMP_DIR}/${archive_name}"; then
        die "Failed to download ${download_url}"
    fi

    # Download and verify checksum if available
    if curl -fsSL "$checksums_url" -o "${TMP_DIR}/checksums.txt" 2>/dev/null; then
        info "Verifying checksum..."
        local expected_checksum
        expected_checksum=$(grep "${archive_name}" "${TMP_DIR}/checksums.txt" | awk '{print $1}')

        if [ -n "$expected_checksum" ]; then
            local actual_checksum
            if command -v sha256sum &> /dev/null; then
                actual_checksum=$(sha256sum "${TMP_DIR}/${archive_name}" | awk '{print $1}')
            elif command -v shasum &> /dev/null; then
                actual_checksum=$(shasum -a 256 "${TMP_DIR}/${archive_name}" | awk '{print $1}')
            fi

            if [ "$expected_checksum" != "$actual_checksum" ]; then
                die "Checksum verification failed!"
            fi
            success "Checksum verified"
        fi
    else
        warn "Checksums not available, skipping verification"
    fi

    # Extract archive
    info "Extracting archive..."
    tar -xzf "${TMP_DIR}/${archive_name}" -C "${TMP_DIR}"

    # Find the binary
    local binary_path="${TMP_DIR}/${BINARY_NAME}"
    if [ ! -f "$binary_path" ]; then
        # Try to find it in a subdirectory
        binary_path=$(find "${TMP_DIR}" -name "${BINARY_NAME}" -type f | head -1)
    fi

    if [ ! -f "$binary_path" ]; then
        die "Binary not found in archive"
    fi

    echo "$binary_path"
}

# Install the binary
install_binary() {
    local binary_path="$1"
    local install_dir="$2"

    # Create install directory if needed
    if [ ! -d "$install_dir" ]; then
        if [ "$LOCAL_INSTALL" = true ]; then
            mkdir -p "$install_dir"
        else
            sudo mkdir -p "$install_dir"
        fi
    fi

    local dest="${install_dir}/${BINARY_NAME}"

    info "Installing to ${dest}..."

    if [ "$LOCAL_INSTALL" = true ]; then
        cp "$binary_path" "$dest"
        chmod +x "$dest"
    else
        sudo cp "$binary_path" "$dest"
        sudo chmod +x "$dest"
    fi

    success "Installed successfully!"
}

# Check if binary is in PATH
check_path() {
    local install_dir="$1"

    if [[ ":$PATH:" != *":${install_dir}:"* ]]; then
        echo ""
        warn "${install_dir} is not in your PATH"
        echo ""
        echo "Add it to your shell configuration:"
        echo ""

        local shell_name
        shell_name=$(basename "$SHELL")

        case "$shell_name" in
            bash)
                echo "  echo 'export PATH=\"${install_dir}:\$PATH\"' >> ~/.bashrc"
                echo "  source ~/.bashrc"
                ;;
            zsh)
                echo "  echo 'export PATH=\"${install_dir}:\$PATH\"' >> ~/.zshrc"
                echo "  source ~/.zshrc"
                ;;
            fish)
                echo "  fish_add_path ${install_dir}"
                ;;
            *)
                echo "  export PATH=\"${install_dir}:\$PATH\""
                ;;
        esac
        echo ""
    fi
}

# Print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --version, -v VERSION   Install specific version (e.g., v1.0.0)"
    echo "  --local, -l             Install to ~/.local/bin (no sudo required)"
    echo "  --force, -f             Force reinstall even if already installed"
    echo "  --help, -h              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                      Install latest version"
    echo "  $0 --version v1.0.0     Install version v1.0.0"
    echo "  $0 --local              Install to ~/.local/bin"
}

# Parse arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --version|-v)
                VERSION="$2"
                shift 2
                ;;
            --local|-l)
                LOCAL_INSTALL=true
                shift
                ;;
            --force|-f)
                FORCE=true
                shift
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                die "Unknown option: $1"
                ;;
        esac
    done
}

# Main installation function
main() {
    trap cleanup EXIT
    parse_args "$@"

    print_banner
    check_dependencies

    local os arch
    os=$(detect_os)
    arch=$(detect_arch)

    info "Detected: ${os}/${arch}"

    # Get version
    if [ -z "$VERSION" ]; then
        info "Fetching latest version..."
        VERSION=$(get_latest_version)
    fi

    info "Version: ${VERSION}"

    # Determine install directory
    local install_dir
    if [ "$LOCAL_INSTALL" = true ]; then
        install_dir="$LOCAL_INSTALL_DIR"
    else
        install_dir="$INSTALL_DIR"
    fi

    # Check if already installed
    local existing_binary="${install_dir}/${BINARY_NAME}"
    if [ -f "$existing_binary" ] && [ "$FORCE" = false ]; then
        local existing_version
        existing_version=$("$existing_binary" --version 2>/dev/null | head -1 || echo "unknown")
        warn "${BINARY_NAME} is already installed: ${existing_version}"
        echo "Use --force to reinstall"
        exit 0
    fi

    # Download and install
    local binary_path
    binary_path=$(download_binary "$VERSION" "$os" "$arch")
    install_binary "$binary_path" "$install_dir"

    # Verify installation
    if [ -x "${install_dir}/${BINARY_NAME}" ]; then
        local installed_version
        installed_version=$("${install_dir}/${BINARY_NAME}" --version 2>/dev/null | head -1 || echo "$VERSION")

        echo ""
        success "${BINARY_NAME} ${installed_version} installed successfully!"
        echo ""

        check_path "$install_dir"

        echo "Get started:"
        echo "  ${BINARY_NAME} --help"
        echo "  ${BINARY_NAME} run -f data.csv -q \"SELECT * FROM data\""
    else
        die "Installation verification failed"
    fi
}

main "$@"
