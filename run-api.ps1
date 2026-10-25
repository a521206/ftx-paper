[CmdletBinding()]
param(
    [int]$Port = 8501,
    [string]$HostAddress = "127.0.0.1",
    [string]$ConfigPath = "",
    [string]$Broker = ""
)

$ErrorActionPreference = "Stop"
$packageRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPython = Join-Path $packageRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $venvPython)) { throw "Python virtual environment not found: $venvPython" }

$envFile = Join-Path $packageRoot ".env"
if (Test-Path -LiteralPath $envFile) {
    Get-Content -LiteralPath $envFile | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#") -or $line.IndexOf("=") -lt 1) { return }

        $separator = $line.IndexOf("=")
        $name = $line.Substring(0, $separator).Trim()
        $value = $line.Substring($separator + 1).Trim()
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        if (-not [Environment]::GetEnvironmentVariable($name)) {
            Set-Item -Path "Env:$name" -Value $value
        }
    }
}

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
$paperHome = if ($env:FTX_PAPER_HOME) { $env:FTX_PAPER_HOME } else { Join-Path $env:LOCALAPPDATA "ftx-paper" }
$env:FTX_PAPER_HOME = $paperHome
$env:FTX_WEB_HOST = $HostAddress
$env:FTX_WEB_PORT = [string]$Port
$env:FTX_PAPER_RUNTIME_DIR = Join-Path $paperHome "runtime"
$env:FTX_PAPER_AUTH_DB = Join-Path $paperHome "auth\auth.sqlite3"
if ($ConfigPath) { $env:FTX_PAPER_CONFIG_PATH = $ConfigPath }
if ($Broker) { $env:FTX_BROKER = $Broker }

& $venvPython -c "from ftx_paper.cli import api_main; api_main()"
exit $LASTEXITCODE
