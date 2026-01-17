#Requires -Version 5.1
<#
.SYNOPSIS
    DataQL Installation Script for Windows

.DESCRIPTION
    Downloads and installs DataQL from GitHub releases.
    Supports version selection and user-level installation.

.PARAMETER Version
    Specific version to install (e.g., v1.0.0). Defaults to latest.

.PARAMETER InstallDir
    Installation directory. Defaults to $env:ProgramFiles\dataql or $env:USERPROFILE\.dataql\bin

.PARAMETER UserInstall
    Install to user directory (no admin required)

.PARAMETER Force
    Force reinstall even if already installed

.EXAMPLE
    # Install latest version
    irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.ps1 | iex

.EXAMPLE
    # Install specific version
    $env:DATAQL_VERSION="v1.0.0"; irm .../install.ps1 | iex

.EXAMPLE
    # User installation (no admin)
    $env:DATAQL_USER_INSTALL="true"; irm .../install.ps1 | iex
#>

[CmdletBinding()]
param(
    [string]$Version = $env:DATAQL_VERSION,
    [string]$InstallDir = $env:DATAQL_INSTALL_DIR,
    [switch]$UserInstall = [bool]$env:DATAQL_USER_INSTALL,
    [switch]$Force = [bool]$env:DATAQL_FORCE
)

# Configuration
$Script:Repo = "adrianolaselva/dataql"
$Script:BinaryName = "dataql"
$Script:GitHubApi = "https://api.github.com/repos/$Repo/releases"
$Script:GitHubDownload = "https://github.com/$Repo/releases/download"

# Colors and formatting
function Write-Banner {
    $banner = @"

  ____        _        ___  _
 |  _ \  __ _| |_ __ _/ _ \| |
 | | | |/ _` | __/ _` | | | | |
 | |_| | (_| | || (_| | |_| | |___
 |____/ \__,_|\__\__,_|\___\_\_____|

"@
    Write-Host $banner -ForegroundColor Cyan
    Write-Host "Universal Data Query Tool" -ForegroundColor White
    Write-Host ""
}

function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] " -ForegroundColor Blue -NoNewline
    Write-Host $Message
}

function Write-Success {
    param([string]$Message)
    Write-Host "[OK] " -ForegroundColor Green -NoNewline
    Write-Host $Message
}

function Write-Warn {
    param([string]$Message)
    Write-Host "[WARN] " -ForegroundColor Yellow -NoNewline
    Write-Host $Message
}

function Write-Error {
    param([string]$Message)
    Write-Host "[ERROR] " -ForegroundColor Red -NoNewline
    Write-Host $Message
}

# Get the latest release version
function Get-LatestVersion {
    try {
        $response = Invoke-RestMethod -Uri "$Script:GitHubApi/latest" -UseBasicParsing
        return $response.tag_name
    }
    catch {
        throw "Failed to fetch latest version from GitHub: $_"
    }
}

# Download the binary
function Get-Binary {
    param(
        [string]$Version,
        [string]$TempDir
    )

    $arch = "amd64"  # Windows is typically x64
    if ([Environment]::Is64BitOperatingSystem -eq $false) {
        throw "32-bit Windows is not supported"
    }

    # Handle ARM64 Windows
    if ($env:PROCESSOR_ARCHITECTURE -eq "ARM64") {
        $arch = "arm64"
    }

    $archiveName = "${Script:BinaryName}_$($Version.TrimStart('v'))_windows_${arch}.zip"
    $downloadUrl = "$Script:GitHubDownload/$Version/$archiveName"
    $checksumsUrl = "$Script:GitHubDownload/$Version/checksums.txt"

    $archivePath = Join-Path $TempDir $archiveName
    $checksumsPath = Join-Path $TempDir "checksums.txt"

    Write-Info "Downloading $Script:BinaryName $Version for windows/$arch..."

    try {
        # Download archive
        Invoke-WebRequest -Uri $downloadUrl -OutFile $archivePath -UseBasicParsing

        # Try to download and verify checksum
        try {
            Invoke-WebRequest -Uri $checksumsUrl -OutFile $checksumsPath -UseBasicParsing -ErrorAction SilentlyContinue

            if (Test-Path $checksumsPath) {
                Write-Info "Verifying checksum..."

                $checksums = Get-Content $checksumsPath
                $expectedLine = $checksums | Where-Object { $_ -match $archiveName }

                if ($expectedLine) {
                    $expectedChecksum = ($expectedLine -split '\s+')[0]
                    $actualChecksum = (Get-FileHash -Path $archivePath -Algorithm SHA256).Hash.ToLower()

                    if ($expectedChecksum -ne $actualChecksum) {
                        throw "Checksum verification failed!"
                    }
                    Write-Success "Checksum verified"
                }
            }
        }
        catch {
            Write-Warn "Checksums not available, skipping verification"
        }

        # Extract archive
        Write-Info "Extracting archive..."
        Expand-Archive -Path $archivePath -DestinationPath $TempDir -Force

        # Find the binary
        $binaryPath = Get-ChildItem -Path $TempDir -Filter "$Script:BinaryName.exe" -Recurse | Select-Object -First 1 -ExpandProperty FullName

        if (-not $binaryPath) {
            throw "Binary not found in archive"
        }

        return $binaryPath
    }
    catch {
        throw "Failed to download: $_"
    }
}

# Add directory to PATH
function Add-ToPath {
    param([string]$Directory)

    $pathScope = if ($UserInstall) { "User" } else { "Machine" }

    try {
        $currentPath = [Environment]::GetEnvironmentVariable("Path", $pathScope)

        if ($currentPath -notlike "*$Directory*") {
            $newPath = "$Directory;$currentPath"
            [Environment]::SetEnvironmentVariable("Path", $newPath, $pathScope)

            # Update current session
            $env:Path = "$Directory;$env:Path"

            Write-Success "Added to PATH ($pathScope)"
            return $true
        }
        return $false
    }
    catch {
        Write-Warn "Could not update PATH: $_"
        Write-Host ""
        Write-Host "Manually add this to your PATH:"
        Write-Host "  $Directory" -ForegroundColor Yellow
        return $false
    }
}

# Install the binary
function Install-Binary {
    param(
        [string]$BinaryPath,
        [string]$InstallDirectory
    )

    # Create install directory
    if (-not (Test-Path $InstallDirectory)) {
        try {
            New-Item -ItemType Directory -Path $InstallDirectory -Force | Out-Null
        }
        catch {
            throw "Failed to create directory $InstallDirectory : $_"
        }
    }

    $destination = Join-Path $InstallDirectory "$Script:BinaryName.exe"

    Write-Info "Installing to $destination..."

    try {
        Copy-Item -Path $BinaryPath -Destination $destination -Force
        Write-Success "Installed successfully!"
    }
    catch {
        throw "Failed to install: $_"
    }
}

# Main installation function
function Install-DataQL {
    Write-Banner

    # Determine install directory
    $targetDir = $InstallDir
    if (-not $targetDir) {
        if ($UserInstall) {
            $targetDir = Join-Path $env:USERPROFILE ".dataql\bin"
        }
        else {
            $targetDir = Join-Path $env:ProgramFiles "dataql"
        }
    }

    # Check for admin rights if system install
    if (-not $UserInstall -and -not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        Write-Warn "Administrator rights required for system-wide installation"
        Write-Host ""
        Write-Host "Options:" -ForegroundColor Yellow
        Write-Host "  1. Run PowerShell as Administrator"
        Write-Host "  2. Use user installation: `$env:DATAQL_USER_INSTALL=`"true`"; irm ... | iex"
        Write-Host ""

        $response = Read-Host "Continue with user installation? (Y/n)"
        if ($response -ne "n" -and $response -ne "N") {
            $Script:UserInstall = $true
            $targetDir = Join-Path $env:USERPROFILE ".dataql\bin"
        }
        else {
            throw "Installation cancelled"
        }
    }

    # Get version
    $targetVersion = $Version
    if (-not $targetVersion) {
        Write-Info "Fetching latest version..."
        $targetVersion = Get-LatestVersion
    }

    Write-Info "Version: $targetVersion"
    Write-Info "Install directory: $targetDir"

    # Check if already installed
    $existingBinary = Join-Path $targetDir "$Script:BinaryName.exe"
    if ((Test-Path $existingBinary) -and -not $Force) {
        try {
            $existingVersion = & $existingBinary --version 2>$null | Select-Object -First 1
            Write-Warn "$Script:BinaryName is already installed: $existingVersion"
            Write-Host "Use -Force or `$env:DATAQL_FORCE=`"true`" to reinstall"
            return
        }
        catch {
            # Continue with installation if version check fails
        }
    }

    # Create temp directory
    $tempDir = Join-Path $env:TEMP "dataql-install-$(Get-Random)"
    New-Item -ItemType Directory -Path $tempDir -Force | Out-Null

    try {
        # Download
        $binaryPath = Get-Binary -Version $targetVersion -TempDir $tempDir

        # Install
        Install-Binary -BinaryPath $binaryPath -InstallDirectory $targetDir

        # Add to PATH
        $pathAdded = Add-ToPath -Directory $targetDir

        # Verify installation
        $installedBinary = Join-Path $targetDir "$Script:BinaryName.exe"
        if (Test-Path $installedBinary) {
            Write-Host ""
            Write-Success "$Script:BinaryName $targetVersion installed successfully!"
            Write-Host ""

            if (-not $pathAdded) {
                Write-Host "Note: You may need to restart your terminal for PATH changes to take effect" -ForegroundColor Yellow
                Write-Host ""
            }

            Write-Host "Get started:"
            Write-Host "  $Script:BinaryName --help" -ForegroundColor Cyan
            Write-Host "  $Script:BinaryName run -f data.csv -q `"SELECT * FROM data`"" -ForegroundColor Cyan
        }
        else {
            throw "Installation verification failed"
        }
    }
    finally {
        # Cleanup
        if (Test-Path $tempDir) {
            Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}

# Run installation
try {
    Install-DataQL
}
catch {
    Write-Error $_
    exit 1
}
