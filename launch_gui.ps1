# PowerShell script to launch the Cosmos DB Migration Tool GUI
# Provides better error handling and user feedback than batch files

param(
    [switch]$InstallDeps,
    [switch]$CreateVenv,
    [switch]$Help
)

function Show-Help {
    Write-Host "Azure Cosmos DB MongoDB Migration Tool - GUI Launcher" -ForegroundColor Cyan
    Write-Host "================================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Usage:"
    Write-Host "  .\launch_gui.ps1                  # Launch GUI (default)"
    Write-Host "  .\launch_gui.ps1 -InstallDeps     # Install dependencies first"
    Write-Host "  .\launch_gui.ps1 -CreateVenv      # Create virtual environment"
    Write-Host "  .\launch_gui.ps1 -Help            # Show this help"
    Write-Host ""
    Write-Host "Requirements:"
    Write-Host "  - Python 3.8 or later"
    Write-Host "  - pip package manager"
    Write-Host ""
}

function Test-PythonInstallation {
    try {
        $pyVersion = python --version 2>$null
        if ($pyVersion -match "Python (\d+)\.(\d+)") {
            $major = [int]$matches[1]
            $minor = [int]$matches[2]
            if ($major -ge 3 -and $minor -ge 8) {
                Write-Host "✓ Python $pyVersion detected" -ForegroundColor Green
                return $true
            } else {
                Write-Host "✗ Python $pyVersion is too old. Please install Python 3.8 or later." -ForegroundColor Red
                return $false
            }
        }
    } catch {
        Write-Host "✗ Python is not installed or not in PATH" -ForegroundColor Red
        Write-Host "  Please install Python from https://python.org" -ForegroundColor Yellow
        return $false
    }
    return $false
}

function Install-Dependencies {
    Write-Host "Installing/updating dependencies..." -ForegroundColor Yellow
    try {
        python -m pip install --upgrade pip
        python -m pip install -r requirements.txt
        Write-Host "✓ Dependencies installed successfully" -ForegroundColor Green
        return $true
    } catch {
        Write-Host "✗ Failed to install dependencies: $_" -ForegroundColor Red
        return $false
    }
}

function Create-VirtualEnvironment {
    Write-Host "Creating virtual environment..." -ForegroundColor Yellow
    try {
        python -m venv venv
        Write-Host "✓ Virtual environment created" -ForegroundColor Green
        
        Write-Host "Activating virtual environment..." -ForegroundColor Yellow
        & ".\venv\Scripts\Activate.ps1"
        Write-Host "✓ Virtual environment activated" -ForegroundColor Green
        
        return $true
    } catch {
        Write-Host "✗ Failed to create virtual environment: $_" -ForegroundColor Red
        return $false
    }
}

function Start-GUI {
    Write-Host "Starting Azure Cosmos DB Migration Tool GUI..." -ForegroundColor Cyan
    try {
        # Check if virtual environment exists and activate it
        if (Test-Path "venv\Scripts\Activate.ps1") {
            Write-Host "Activating virtual environment..." -ForegroundColor Yellow
            & ".\venv\Scripts\Activate.ps1"
        }
        
        # Launch the GUI
        python gui_launcher.py
        
    } catch {
        Write-Host "✗ Failed to start GUI: $_" -ForegroundColor Red
        Write-Host "Try running with -InstallDeps flag to install dependencies" -ForegroundColor Yellow
        return $false
    }
}

# Main script execution
if ($Help) {
    Show-Help
    exit 0
}

Write-Host "Azure Cosmos DB MongoDB Migration Tool" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

# Check Python installation
if (-not (Test-PythonInstallation)) {
    Write-Host ""
    Write-Host "Please install Python 3.8+ and try again." -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

# Create virtual environment if requested
if ($CreateVenv) {
    if (-not (Create-VirtualEnvironment)) {
        Read-Host "Press Enter to exit"
        exit 1
    }
}

# Install dependencies if requested or if requirements.txt is newer than last install
if ($InstallDeps -or -not (Test-Path "venv\pyvenv.cfg")) {
    if (-not (Install-Dependencies)) {
        Read-Host "Press Enter to exit"
        exit 1
    }
}

# Check if .env file exists, if not, suggest creating one
if (-not (Test-Path ".env")) {
    Write-Host ""
    Write-Host "⚠ Configuration file (.env) not found" -ForegroundColor Yellow
    Write-Host "  You can create one from the template: .env.template" -ForegroundColor Yellow
    Write-Host "  Or configure connections through the GUI" -ForegroundColor Yellow
    Write-Host ""
}

# Start the GUI
Start-GUI

Write-Host ""
Write-Host "GUI session ended." -ForegroundColor Green
Read-Host "Press Enter to exit"
