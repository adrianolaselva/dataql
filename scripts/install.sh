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
UPGRADE=false
CLEAN=false

# Global temp directory
TMP_DIR=""

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

die() {
    error "$1"
    exit 1
}

cleanup() {
    if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
        rm -rf "$TMP_DIR"
    fi
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

# Extract version number from version string (e.g., "v0.1.0 (commit: abc, built: 2024-01-01)" -> "0.1.0")
extract_version_number() {
    local version_string="$1"
    # Remove 'v' prefix and everything after the version number
    echo "$version_string" | sed -E 's/^v?([0-9]+\.[0-9]+\.[0-9]+).*/\1/' | head -1
}

# Compare two semantic versions. Returns:
# 0 if equal, 1 if first > second, 2 if first < second
compare_versions() {
    local v1="$1"
    local v2="$2"

    # Extract just the version numbers
    v1=$(extract_version_number "$v1")
    v2=$(extract_version_number "$v2")

    if [ "$v1" = "$v2" ]; then
        return 0
    fi

    # Compare using sort -V
    local smaller
    smaller=$(printf '%s\n%s' "$v1" "$v2" | sort -V | head -n1)

    if [ "$smaller" = "$v1" ]; then
        return 2  # v1 < v2
    else
        return 1  # v1 > v2
    fi
}

# Check for multiple installations and warn user
check_multiple_installations() {
    local found=()
    local versions=()

    if [ -f "${INSTALL_DIR}/${BINARY_NAME}" ]; then
        local v
        v=$("${INSTALL_DIR}/${BINARY_NAME}" --version 2>/dev/null | head -1 || echo "unknown")
        found+=("${INSTALL_DIR}/${BINARY_NAME}")
        versions+=("$v")
    fi

    if [ -f "${LOCAL_INSTALL_DIR}/${BINARY_NAME}" ]; then
        local v
        v=$("${LOCAL_INSTALL_DIR}/${BINARY_NAME}" --version 2>/dev/null | head -1 || echo "unknown")
        found+=("${LOCAL_INSTALL_DIR}/${BINARY_NAME}")
        versions+=("$v")
    fi

    if [ ${#found[@]} -gt 1 ]; then
        echo ""
        warn "Multiple installations detected:"
        for i in "${!found[@]}"; do
            echo "  - ${found[$i]}: ${versions[$i]}"
        done
        echo ""
        echo "This may cause confusion. Consider removing duplicates:"
        echo "  sudo rm ${INSTALL_DIR}/${BINARY_NAME}    # remove system install"
        echo "  rm ${LOCAL_INSTALL_DIR}/${BINARY_NAME}   # remove local install"
        echo ""
        echo "Or use --clean to remove all installations before installing."
        echo ""
    fi
}

# Clean all existing installations
clean_all_installations() {
    info "Cleaning all existing installations..."

    if [ -f "${INSTALL_DIR}/${BINARY_NAME}" ]; then
        info "Removing ${INSTALL_DIR}/${BINARY_NAME}..."
        if [ -w "${INSTALL_DIR}" ]; then
            rm -f "${INSTALL_DIR}/${BINARY_NAME}"
        else
            sudo rm -f "${INSTALL_DIR}/${BINARY_NAME}"
        fi
        success "Removed ${INSTALL_DIR}/${BINARY_NAME}"
    fi

    if [ -f "${LOCAL_INSTALL_DIR}/${BINARY_NAME}" ]; then
        info "Removing ${LOCAL_INSTALL_DIR}/${BINARY_NAME}..."
        rm -f "${LOCAL_INSTALL_DIR}/${BINARY_NAME}"
        success "Removed ${LOCAL_INSTALL_DIR}/${BINARY_NAME}"
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
    echo "  --upgrade, -u           Upgrade to latest version (only if newer)"
    echo "  --clean, -c             Remove all existing installations first"
    echo "  --help, -h              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                      Install latest version (first time)"
    echo "  $0 --upgrade            Upgrade to latest version if newer"
    echo "  $0 --force              Force reinstall latest version"
    echo "  $0 --clean --force      Clean all installations, then install"
    echo "  $0 --version v1.0.0     Install specific version"
    echo "  $0 --local              Install to ~/.local/bin (no sudo)"
    echo "  $0 --local --upgrade    Upgrade local installation"
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
            --upgrade|-u)
                UPGRADE=true
                shift
                ;;
            --clean|-c)
                CLEAN=true
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

    # Check for multiple installations at the start
    check_multiple_installations

    local os arch
    os=$(detect_os)
    arch=$(detect_arch)

    info "Detected: ${os}/${arch}"

    # Clean all installations if requested
    if [ "$CLEAN" = true ]; then
        clean_all_installations
    fi

    # Get version
    if [ -z "$VERSION" ]; then
        info "Fetching latest version..."
        VERSION=$(get_latest_version)
    fi

    info "Target version: ${VERSION}"

    # Determine install directory
    local install_dir
    if [ "$LOCAL_INSTALL" = true ]; then
        install_dir="$LOCAL_INSTALL_DIR"
    else
        install_dir="$INSTALL_DIR"
    fi

    # Check if already installed
    local existing_binary="${install_dir}/${BINARY_NAME}"
    if [ -f "$existing_binary" ]; then
        local existing_version
        existing_version=$("$existing_binary" --version 2>/dev/null | head -1 || echo "unknown")
        info "Installed version: ${existing_version}"

        local installed_ver target_ver
        installed_ver=$(extract_version_number "$existing_version")
        target_ver=$(extract_version_number "$VERSION")

        # Compare versions
        compare_versions "$installed_ver" "$target_ver"
        local cmp_result=$?

        # Handle --upgrade flag
        if [ "$UPGRADE" = true ]; then
            if [ $cmp_result -eq 0 ]; then
                # Same version
                if [ "$FORCE" = true ]; then
                    info "Reinstalling same version ${VERSION} (--force specified)..."
                else
                    success "Already at version ${VERSION}. No upgrade needed."
                    exit 0
                fi
            elif [ $cmp_result -eq 1 ]; then
                # Installed is newer than target (downgrade)
                if [ "$FORCE" = true ]; then
                    warn "Downgrading from ${installed_ver} to ${target_ver} (--force specified)..."
                else
                    warn "Installed version (${installed_ver}) is newer than target (${target_ver})"
                    echo "Use --force to downgrade"
                    exit 0
                fi
            else
                # Target is newer (upgrade)
                info "Upgrading from ${installed_ver} to ${target_ver}..."
            fi
        elif [ "$FORCE" = true ]; then
            info "Force reinstalling ${VERSION}..."
        else
            warn "${BINARY_NAME} is already installed: ${existing_version}"
            echo ""
            echo "Options:"
            echo "  --upgrade  Upgrade to latest version (if newer)"
            echo "  --force    Force reinstall"
            echo "  --clean    Remove all installations first"
            exit 0
        fi
    fi

    # Create temp directory
    TMP_DIR=$(mktemp -d)

    # Download
    local archive_name="${BINARY_NAME}_${VERSION#v}_${os}_${arch}.tar.gz"
    local download_url="${GITHUB_DOWNLOAD}/${VERSION}/${archive_name}"
    local checksums_url="${GITHUB_DOWNLOAD}/${VERSION}/checksums.txt"

    info "Downloading ${BINARY_NAME} ${VERSION} for ${os}/${arch}..."

    if ! curl -fsSL "$download_url" -o "${TMP_DIR}/${archive_name}"; then
        die "Failed to download ${download_url}"
    fi

    # Verify checksum if available
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
        binary_path=$(find "${TMP_DIR}" -name "${BINARY_NAME}" -type f | head -1)
    fi

    if [ ! -f "$binary_path" ]; then
        die "Binary not found in archive"
    fi

    # Create install directory if needed
    if [ ! -d "$install_dir" ]; then
        if [ "$LOCAL_INSTALL" = true ]; then
            mkdir -p "$install_dir"
        else
            sudo mkdir -p "$install_dir"
        fi
    fi

    # Install
    local dest="${install_dir}/${BINARY_NAME}"
    info "Installing to ${dest}..."

    if [ "$LOCAL_INSTALL" = true ]; then
        cp "$binary_path" "$dest"
        chmod +x "$dest"
    else
        sudo cp "$binary_path" "$dest"
        sudo chmod +x "$dest"
    fi

    # Verify installation
    if [ -x "${dest}" ]; then
        local installed_version
        installed_version=$("${dest}" --version 2>/dev/null | head -1)

        if [ -z "$installed_version" ]; then
            die "Binary installed but --version command failed. Installation may be corrupted."
        fi

        # Verify the installed version matches expected
        local installed_ver_num target_ver_num
        installed_ver_num=$(extract_version_number "$installed_version")
        target_ver_num=$(extract_version_number "$VERSION")

        if [ "$installed_ver_num" != "$target_ver_num" ]; then
            warn "Version mismatch: expected ${target_ver_num}, got ${installed_ver_num}"
            warn "You may have another installation in your PATH taking precedence."
        fi

        # Clear shell hash table to ensure new binary is found
        hash -r 2>/dev/null || true

        echo ""
        success "${BINARY_NAME} ${installed_version} installed successfully!"
        echo ""

        check_path "$install_dir"

        echo "Get started:"
        echo "  ${BINARY_NAME} --help"
        echo "  ${BINARY_NAME} run -f data.csv -q \"SELECT * FROM data\""
        echo ""
        echo "Note: If you see an old version, restart your shell or run: hash -r"
    else
        die "Installation verification failed - binary not executable at ${dest}"
    fi
}

main "$@"
