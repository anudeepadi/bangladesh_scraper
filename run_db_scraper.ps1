# PowerShell script to run the Family Planning Database Scraper

# Get command line arguments
param(
    [string]$start = "2020-01",
    [string]$end = "2022-12",
    [string]$resume = "",
    [int]$workers = 4,
    [int]$retries = 5,
    [string]$warehouse = "Dhaka CWH"
)

Write-Host "Installing requirements..." -ForegroundColor Green
pip install -r requirements-db.txt

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

Write-Host "Creating database table if it doesn't exist..." -ForegroundColor Green
sqlcmd -S $env:SERVER -d $env:BANGLADESH_DATABASE -U $env:UID -P $env:PID -i create_form_f3_table.sql

Write-Host "Starting database scraper..." -ForegroundColor Green
$arguments = "--start $start --end $end --workers $workers --retries $retries --warehouse `"$warehouse`""

if ($resume) {
    $arguments += " --resume $resume"
}

Write-Host "Running: python db_scraper.py $arguments" -ForegroundColor Cyan
Invoke-Expression "python db_scraper.py $arguments"

Write-Host "Done!" -ForegroundColor Green
Read-Host -Prompt "Press Enter to exit"
