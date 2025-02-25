# PowerShell script to run the Final Family Planning Database Scraper

# Get command line arguments
param(
    [string]$start = "2017-01",
    [string]$end = "2025-01",
    [string]$resume = "",
    [int]$workers = 4,
    [int]$retries = 5,
    [string]$warehouse = "",  # Default empty to process ALL warehouses
    [switch]$resetProgress,
    [switch]$skipTableCreation
)

# Load environment variables from .env file
if (Test-Path .\.env) {
    Get-Content .\.env | ForEach-Object {
        if ($_ -match "^\s*([^#][^=]+)=(.*)$") {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim(" `"'")
            Set-Item -Path env:$name -Value $value
        }
    }
    Write-Host "Environment variables loaded from .env file" -ForegroundColor Green
} else {
    Write-Host "Warning: .env file not found" -ForegroundColor Yellow
}

# Get computer name for progress tracking
$machineName = $env:COMPUTERNAME
Write-Host "Running on machine: $machineName" -ForegroundColor Cyan

# Install required packages
Write-Host "Installing requirements..." -ForegroundColor Green
pip install -r requirements-db.txt
pip install pyodbc python-dotenv

# Create database table if it doesn't exist and if not skipped
if (-not $skipTableCreation) {
    Write-Host "Checking for sqlcmd utility..." -ForegroundColor Yellow
    $sqlcmdExists = $null -ne (Get-Command "sqlcmd" -ErrorAction SilentlyContinue)
    
    if ($sqlcmdExists) {
        Write-Host "Creating database table using sqlcmd..." -ForegroundColor Green
        sqlcmd -S $env:SERVER -d $env:BANGLADESH_DATABASE -U $env:UID -P $env:PID -i create_form_f2_table_fixed.sql
    } else {
        Write-Host "sqlcmd utility not found. Will create table directly from Python script." -ForegroundColor Yellow
        Write-Host "Table creation will be handled by the Python script on first run." -ForegroundColor Yellow
        
        # Create a flag file to indicate table creation is needed
        "Table creation needed" | Out-File -FilePath "create_table_flag.txt"
    }
}

# Start the scraper
Write-Host "Starting final database scraper..." -ForegroundColor Green
$arguments = "--start $start --end $end --workers $workers --retries $retries"

if ($warehouse) {
    $arguments += " --warehouse `"$warehouse`""
}

if ($resume) {
    $arguments += " --resume $resume"
}

if ($resetProgress) {
    $arguments += " --reset-progress"
}

# Pass flag for table creation if needed
if ((-not $sqlcmdExists) -and (-not $skipTableCreation)) {
    $arguments += " --create-table"
}

Write-Host "Running: python final_db_scraper.py $arguments" -ForegroundColor Cyan
Write-Host "Processing ALL warehouses from Jan 2017 to Jan 2025" -ForegroundColor Yellow
Write-Host "Progress will be saved to scraper_progress_$machineName.pkl" -ForegroundColor Yellow
Write-Host "You can stop this script at any time and resume later from any server" -ForegroundColor Yellow
Invoke-Expression "python final_db_scraper.py $arguments"

# Clean up flag file if it exists
if (Test-Path "create_table_flag.txt") {
    Remove-Item "create_table_flag.txt"
}

Write-Host "Done!" -ForegroundColor Green
Read-Host -Prompt "Press Enter to exit"
