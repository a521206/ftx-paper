[CmdletBinding()]
param(
    [int]$Port = 8501,
    [string]$HostAddress = "127.0.0.1",
    [string]$ConfigPath = ""
)

$ErrorActionPreference = "Stop"
$packageRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPython = Join-Path (Split-Path -Parent $packageRoot) ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $venvPython)) { throw "Python virtual environment not found: $venvPython" }

function Stop-ExistingApiProcess {
    param(
        [Parameter(Mandatory)]
        [string]$PythonPath
    )

    # Match only this project's API entry point. The UI process and unrelated
    # Python processes are intentionally left running.
    $apiProcesses = @(
        Get-CimInstance -ClassName Win32_Process |
            Where-Object {
                $_.ExecutablePath -and
                $_.ExecutablePath -ieq $PythonPath -and
                $_.CommandLine -and
                (
                    $_.CommandLine -like "*from ftx_paper.cli import api_main*" -or
                    $_.CommandLine -like "*ftx_paper.cli:api_main*"
                )
            }
    )

    foreach ($apiProcess in $apiProcesses) {
        Write-Host "Stopping existing FTX API process (PID $($apiProcess.ProcessId))..."
        Stop-Process -Id ([int]$apiProcess.ProcessId) -Force
    }

    if ($apiProcesses.Count -gt 0) {
        Start-Sleep -Milliseconds 250
    }
}

Stop-ExistingApiProcess -PythonPath $venvPython

$env:PYTHONPATH = Join-Path $packageRoot "src"
$env:FTX_WEB_HOST = $HostAddress
$env:FTX_WEB_PORT = [string]$Port
$env:FTX_PAPER_RUNTIME_DIR = Join-Path $packageRoot "data\runtime\ftx-paper"
$env:FTX_PAPER_AUTH_DB = Join-Path $packageRoot "data\runtime\zerodha\auth.sqlite3"
if ($ConfigPath) { $env:FTX_PAPER_CONFIG_PATH = $ConfigPath }

& $venvPython -c "from ftx_paper.cli import api_main; api_main()"
exit $LASTEXITCODE
