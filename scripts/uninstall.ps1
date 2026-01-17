#Requires -Version 5.1
<#
.SYNOPSIS
    DataQL Uninstallation Script for Windows

.DESCRIPTION
    Removes DataQL from your system.

.EXAMPLE
    # Uninstall
    irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/uninstall.ps1 | iex
#>

[CmdletBinding()]
param()

# Configuration
$Script:BinaryName = "dataql"
$Script:SystemInstallDir = Join-Path $env:ProgramFiles "dataql"
$Script:UserInstallDir = Join-Path $env:USERPROFILE ".dataql\bin"

# Colors and formatting
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

# Remove from PATH
function Remove-FromPath {
    param([string]$Directory)

    foreach ($scope in @("User", "Machine")) {
        try {
            $currentPath = [Environment]::GetEnvironmentVariable("Path", $scope)
            if ($currentPath -like "*$Directory*") {
                $newPath = ($currentPath -split ';' | Where-Object { $_ -ne $Directory }) -join ';'

                if ($scope -eq "Machine") {
                    if (([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
                        [Environment]::SetEnvironmentVariable("Path", $newPath, $scope)
                        Write-Info "Removed from system PATH"
                    }
                }
                else {
                    [Environment]::SetEnvironmentVariable("Path", $newPath, $scope)
                    Write-Info "Removed from user PATH"
                }
            }
        }
        catch {
            # Ignore PATH errors
        }
    }
}

# Main uninstall function
function Uninstall-DataQL {
    Write-Host ""
    Write-Host "DataQL Uninstaller" -ForegroundColor Cyan
    Write-Host "==================" -ForegroundColor Cyan
    Write-Host ""

    $found = $false

    # Check system installation
    $systemBinary = Join-Path $Script:SystemInstallDir "$Script:BinaryName.exe"
    if (Test-Path $systemBinary) {
        Write-Info "Found $Script:BinaryName in $Script:SystemInstallDir"

        try {
            if (([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
                Remove-Item -Path $systemBinary -Force

                # Remove directory if empty
                $remaining = Get-ChildItem -Path $Script:SystemInstallDir -ErrorAction SilentlyContinue
                if (-not $remaining) {
                    Remove-Item -Path $Script:SystemInstallDir -Force -ErrorAction SilentlyContinue
                }

                Remove-FromPath -Directory $Script:SystemInstallDir
                Write-Success "Removed $systemBinary"
                $found = $true
            }
            else {
                Write-Warn "Administrator rights required to remove system installation"
                Write-Host "Run PowerShell as Administrator to uninstall from $Script:SystemInstallDir"
            }
        }
        catch {
            Write-Error "Failed to remove system installation: $_"
        }
    }

    # Check user installation
    $userBinary = Join-Path $Script:UserInstallDir "$Script:BinaryName.exe"
    if (Test-Path $userBinary) {
        Write-Info "Found $Script:BinaryName in $Script:UserInstallDir"

        try {
            Remove-Item -Path $userBinary -Force

            # Remove directory if empty
            $remaining = Get-ChildItem -Path $Script:UserInstallDir -ErrorAction SilentlyContinue
            if (-not $remaining) {
                Remove-Item -Path $Script:UserInstallDir -Force -ErrorAction SilentlyContinue

                # Also remove parent .dataql directory if empty
                $parentDir = Split-Path $Script:UserInstallDir -Parent
                $parentRemaining = Get-ChildItem -Path $parentDir -ErrorAction SilentlyContinue
                if (-not $parentRemaining) {
                    Remove-Item -Path $parentDir -Force -ErrorAction SilentlyContinue
                }
            }

            Remove-FromPath -Directory $Script:UserInstallDir
            Write-Success "Removed $userBinary"
            $found = $true
        }
        catch {
            Write-Error "Failed to remove user installation: $_"
        }
    }

    # Check if it's somewhere else in PATH
    $otherLocation = Get-Command $Script:BinaryName -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source
    if ($otherLocation -and $otherLocation -ne $systemBinary -and $otherLocation -ne $userBinary) {
        Write-Warn "Found additional installation at: $otherLocation"
        Write-Host "You may want to remove it manually"
    }

    if (-not $found) {
        Write-Warn "$Script:BinaryName is not installed in standard locations"
        Write-Host "Checked: $Script:SystemInstallDir, $Script:UserInstallDir"
    }
    else {
        Write-Host ""
        Write-Success "$Script:BinaryName has been uninstalled"
    }
}

# Run uninstaller
try {
    Uninstall-DataQL
}
catch {
    Write-Error $_
    exit 1
}
