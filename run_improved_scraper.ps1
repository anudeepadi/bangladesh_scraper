# PowerShell script to run the Improved Family Planning Database Scraper

# Get command line arguments
param(
    [string]$start = "2020-01",
    [string]$end = "2022-12",
    [string]$resume = "",
    [int]$workers = 4,
    [int]$retries = 5,
    [string]$warehouse = "Dhaka CWH",
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

# Install required packages
Write-Host "Installing requirements..." -ForegroundColor Green
pip install -r requirements-db.txt

# Create database table if it doesn't exist and if not skipped
if (-not $skipTableCreation) {
    Write-Host "Checking for sqlcmd utility..." -ForegroundColor Yellow
    $sqlcmdExists = $null -ne (Get-Command "sqlcmd" -ErrorAction SilentlyContinue)
    
    if ($sqlcmdExists) {
        Write-Host "Creating database table using sqlcmd..." -ForegroundColor Green
        sqlcmd -S $env:SERVER -d $env:BANGLADESH_DATABASE -U $env:UID -P $env:PID -i create_form_f2_table_improved.sql
    } else {
        Write-Host "sqlcmd utility not found. Will create table directly from Python script." -ForegroundColor Yellow
        Write-Host "Table creation will be handled by the Python script on first run." -ForegroundColor Yellow
        
        # Create a flag file to indicate table creation is needed
        "Table creation needed" | Out-File -FilePath "create_table_flag.txt"
    }
}

# Start the scraper
Write-Host "Starting improved database scraper..." -ForegroundColor Green
$arguments = "--start $start --end $end --workers $workers --retries $retries --warehouse `"$warehouse`""

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

Write-Host "Running: python optimized_db_scraper_improved.py $arguments" -ForegroundColor Cyan
Invoke-Expression "python optimized_db_scraper_improved.py $arguments"

# Clean up flag file if it exists
if (Test-Path "create_table_flag.txt") {
    Remove-Item "create_table_flag.txt"
}

Write-Host "Done!" -ForegroundColor Green
Read-Host -Prompt "Press Enter to exit"
