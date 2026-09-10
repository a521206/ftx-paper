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

$env:PYTHONPATH = Join-Path $packageRoot "src"
$env:FTX_WEB_HOST = $HostAddress
$env:FTX_WEB_PORT = [string]$Port
$env:FTX_PAPER_AUTH_DB = Join-Path $env:USERPROFILE ".ftx-paper\auth\auth.sqlite3"
if ($ConfigPath) { $env:FTX_PAPER_CONFIG_PATH = $ConfigPath }

& $venvPython -c "from ftx_paper.cli import api_main; api_main()"
exit $LASTEXITCODE
