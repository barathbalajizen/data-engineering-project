param(
    [Parameter(Position = 0)]
    [ValidateSet(
        "help",
        "sync",
        "sync-dev",
        "lock",
        "test",
        "test-cov",
        "flow",
        "cloud-status",
        "cloud-login",
        "cloud-use",
        "flow-cloud",
        "service-start",
        "service-stop",
        "service-status",
        "ingest",
        "sentiment",
        "silver",
        "gold",
        "lint",
        "format",
        "typecheck"
    )]
    [string]$Task = "help"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Step {
    param([string]$Command)
    Write-Host "> $Command" -ForegroundColor Cyan
    Invoke-Expression $Command
}

switch ($Task) {
    "help" {
        Write-Host "Available tasks:" -ForegroundColor Green
        Write-Host "  .\scripts\tasks.ps1 sync       - Install runtime dependencies from uv.lock"
        Write-Host "  .\scripts\tasks.ps1 sync-dev   - Install runtime + dev dependencies"
        Write-Host "  .\scripts\tasks.ps1 lock       - Refresh uv.lock from pyproject.toml"
        Write-Host "  .\scripts\tasks.ps1 test       - Run unit tests"
        Write-Host "  .\scripts\tasks.ps1 test-cov   - Run tests with coverage report"
        Write-Host "  .\scripts\tasks.ps1 flow       - Run full Prefect flow"
        Write-Host "  .\scripts\tasks.ps1 cloud-status - Show Prefect profile and API configuration"
        Write-Host "  .\scripts\tasks.ps1 cloud-login  - Login to Prefect Cloud"
        Write-Host "  .\scripts\tasks.ps1 cloud-use    - Switch to PREFECT_PROFILE (default: ecommerce-cloud)"
        Write-Host "  .\scripts\tasks.ps1 flow-cloud   - Use cloud profile, then run full Prefect flow"
        Write-Host "  .\scripts\tasks.ps1 service-start - Start local Prefect server in background"
        Write-Host "  .\scripts\tasks.ps1 service-stop  - Stop local Prefect server background process"
        Write-Host "  .\scripts\tasks.ps1 service-status - Show local Prefect server process status"
        Write-Host "  .\scripts\tasks.ps1 ingest     - Run ingestion scripts"
        Write-Host "  .\scripts\tasks.ps1 sentiment  - Run sentiment enrichment"
        Write-Host "  .\scripts\tasks.ps1 silver     - Run Bronze to Silver pipeline"
        Write-Host "  .\scripts\tasks.ps1 gold       - Run Silver to Gold pipeline"
        Write-Host "  .\scripts\tasks.ps1 lint       - Run flake8"
        Write-Host "  .\scripts\tasks.ps1 format     - Run black + isort"
        Write-Host "  .\scripts\tasks.ps1 typecheck  - Run mypy"
    }
    "sync" { Invoke-Step "uv sync" }
    "sync-dev" { Invoke-Step "uv sync --extra dev" }
    "lock" { Invoke-Step "uv lock" }
    "test" { Invoke-Step "uv run pytest tests/unit -q" }
    "test-cov" { Invoke-Step "uv run pytest tests --cov=src --cov-report=html" }
    "flow" { Invoke-Step "uv run python src/flows/prefect_flow.py" }
    "cloud-status" {
        Invoke-Step "prefect profile ls"
        Invoke-Step "prefect config view"
    }
    "cloud-login" { Invoke-Step "prefect cloud login" }
    "cloud-use" {
        $profileName = if ($env:PREFECT_PROFILE) { $env:PREFECT_PROFILE } else { "ecommerce-cloud" }
        Invoke-Step "prefect profile use $profileName"
    }
    "flow-cloud" {
        $profileName = if ($env:PREFECT_PROFILE) { $env:PREFECT_PROFILE } else { "ecommerce-cloud" }
        Invoke-Step "prefect profile use $profileName"
        Invoke-Step "uv run python src/flows/prefect_flow.py"
    }
    "service-start" {
        $existing = Get-CimInstance Win32_Process | Where-Object {
            $_.Name -match "python|prefect" -and $_.CommandLine -match "prefect\s+server\s+start"
        }

        if ($existing) {
            Write-Host "Prefect local server is already running." -ForegroundColor Yellow
            $existing | ForEach-Object {
                Write-Host ("  PID: {0}" -f $_.ProcessId)
            }
            break
        }

        Start-Process -FilePath "prefect" -ArgumentList @("server", "start") -WindowStyle Normal
        Write-Host "Started Prefect local server in a new terminal window." -ForegroundColor Green
        Write-Host "Local UI: http://127.0.0.1:4200" -ForegroundColor Green
    }
    "service-stop" {
        $targets = Get-CimInstance Win32_Process | Where-Object {
            $_.Name -match "python|prefect" -and $_.CommandLine -match "prefect\s+server\s+start"
        }

        if (-not $targets) {
            Write-Host "No running Prefect local server process found." -ForegroundColor Yellow
            break
        }

        $targets | ForEach-Object {
            Stop-Process -Id $_.ProcessId -Force
            Write-Host ("Stopped Prefect server PID {0}" -f $_.ProcessId) -ForegroundColor Green
        }
    }
    "service-status" {
        $targets = Get-CimInstance Win32_Process | Where-Object {
            $_.Name -match "python|prefect" -and $_.CommandLine -match "prefect\s+server\s+start"
        }

        if ($targets) {
            Write-Host "Prefect local server is running:" -ForegroundColor Green
            $targets | ForEach-Object {
                Write-Host ("  PID: {0}" -f $_.ProcessId)
            }
            Write-Host "Local UI: http://127.0.0.1:4200" -ForegroundColor Green
        }
        else {
            Write-Host "Prefect local server is not running." -ForegroundColor Yellow
        }
    }
    "ingest" {
        Invoke-Step "uv run python src/ingestion/orders_loader.py"
        Invoke-Step "uv run python src/ingestion/reviews_loader.py"
        Invoke-Step "uv run python src/ingestion/product_api_client.py"
    }
    "sentiment" { Invoke-Step "uv run python src/ingestion/sentiment_enrichment.py" }
    "silver" { Invoke-Step "uv run python src/transform/silver_layer_loader.py" }
    "gold" { Invoke-Step "uv run python src/transform/gold_layer_loader.py" }
    "lint" { Invoke-Step "uv run flake8 src tests" }
    "format" {
        Invoke-Step "uv run isort src tests"
        Invoke-Step "uv run black src tests"
    }
    "typecheck" { Invoke-Step "uv run mypy src" }
}
