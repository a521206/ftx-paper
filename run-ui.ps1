[CmdletBinding()]
param(
    [int]$Port = 8502,
    [string]$ApiBaseUrl = "http://127.0.0.1:8501"
)

$ErrorActionPreference = "Stop"
$packageRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPython = Join-Path (Split-Path -Parent $packageRoot) ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $venvPython)) {
    throw "Python virtual environment not found: $venvPython"
}

$env:PYTHONPATH = Join-Path $packageRoot "src"
$env:FTX_API_BASE_URL = $ApiBaseUrl
$env:FTX_UI_PORT = [string]$Port

& $venvPython -c "from ftx_paper.cli import ui_main; ui_main()"
exit $LASTEXITCODE
