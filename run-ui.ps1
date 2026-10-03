[CmdletBinding()]
param(
    [int]$Port = 8502,
    [string]$ApiBaseUrl = "http://127.0.0.1:8501"
)

$ErrorActionPreference = "Stop"
$packageRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPython = Join-Path $packageRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $venvPython)) {
    throw "Python virtual environment not found: $venvPython"
}

function Stop-ExistingUiProcess {
    param(
        [Parameter(Mandatory)]
        [string]$PythonPath
    )

    # Match only this project's UI entry point. In particular, do not stop the
    # API process or unrelated Python processes that may be using another port.
    $uiProcesses = @(
        Get-CimInstance -ClassName Win32_Process |
            Where-Object {
                $_.ExecutablePath -and
                $_.ExecutablePath -ieq $PythonPath -and
                $_.CommandLine -and
                (
                    $_.CommandLine -like "*from ftx_paper.cli import ui_main*" -or
                    $_.CommandLine -like "*ftx_paper.cli:ui_main*"
                )
            }
    )

    foreach ($uiProcess in $uiProcesses) {
        Write-Host "Stopping existing FTX UI process (PID $($uiProcess.ProcessId))..."
        Stop-Process -Id ([int]$uiProcess.ProcessId) -Force
    }

    if ($uiProcesses.Count -gt 0) {
        Start-Sleep -Milliseconds 250
    }
}

Stop-ExistingUiProcess -PythonPath $venvPython

$env:PYTHONPATH = Join-Path $packageRoot "src"
$paperHome = if ($env:FTX_PAPER_HOME) { $env:FTX_PAPER_HOME } else { Join-Path $env:LOCALAPPDATA "ftx-paper" }
$env:FTX_PAPER_HOME = $paperHome
$env:FTX_API_BASE_URL = $ApiBaseUrl
$env:FTX_UI_PORT = [string]$Port
$env:FTX_PAPER_RUNTIME_DIR = Join-Path $paperHome "runtime"

& $venvPython -c "from ftx_paper.cli import ui_main; ui_main()"
exit $LASTEXITCODE
