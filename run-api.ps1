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

$paperHome = if ($env:FTX_PAPER_HOME) { $env:FTX_PAPER_HOME } else { Join-Path $env:LOCALAPPDATA "ftx-paper" }
$runtimeDatabase = Join-Path $paperHome "runtime\runtime.sqlite3"

function Stop-ExistingApiProcess {
    param(
        [Parameter(Mandatory)] [string]$PythonPath,
        [Parameter(Mandatory)] [string]$RuntimeDatabase
    )

    $leasePid = $null
    if (Test-Path -LiteralPath $RuntimeDatabase) {
        $leaseQuery = 'import sqlite3, sys; c=sqlite3.connect(sys.argv[1]); row=c.execute("SELECT pid FROM process_leases WHERE service = ''api''").fetchone(); print(row[0] if row else "")'
        $leaseValue = (& $PythonPath -c $leaseQuery $RuntimeDatabase | Select-Object -Last 1)
        if ($null -ne $leaseValue -and $leaseValue.ToString().Trim() -match '^\d+$') {
            $leasePid = [int]$leaseValue.ToString().Trim()
        }
    }

    # The lease PID is authoritative because Windows virtualenv launchers can
    # expose a child server with a normalized command line lacking $PythonPath.
    # Command-line discovery remains a fallback for processes that started
    # before the lease was written or have a stale/missing lease.
    $apiProcesses = @(
        Get-CimInstance -ClassName Win32_Process |
            Where-Object {
                ($leasePid -and $_.ProcessId -eq $leasePid) -or
                (
                    $_.CommandLine -and
                    (
                        $_.CommandLine -like "*$PythonPath*" -and
                        (
                            $_.CommandLine -like "*from ftx_paper.cli import api_main*" -or
                            $_.CommandLine -like "*ftx_paper.cli:api_main*" -or
                            $_.CommandLine -like "*-m ftx_paper.cli api*"
                        )
                    )
                )
            }
    )

    foreach ($apiProcess in $apiProcesses) {
        Write-Host "Stopping existing FTX API process (PID $($apiProcess.ProcessId))..."
        Stop-Process -Id ([int]$apiProcess.ProcessId) -Force
        Wait-Process -Id ([int]$apiProcess.ProcessId) -Timeout 10 -ErrorAction SilentlyContinue
    }

    if ($apiProcesses.Count -gt 0) {
        Start-Sleep -Milliseconds 500
    }
}

Stop-ExistingApiProcess -PythonPath $venvPython -RuntimeDatabase $runtimeDatabase

$env:PYTHONPATH = Join-Path $packageRoot "src"
$env:FTX_PAPER_HOME = $paperHome
$env:FTX_WEB_HOST = $HostAddress
$env:FTX_WEB_PORT = [string]$Port
$env:FTX_PAPER_RUNTIME_DIR = Join-Path $paperHome "runtime"
$env:FTX_PAPER_AUTH_DB = Join-Path $paperHome "auth\auth.sqlite3"
if ($ConfigPath) { $env:FTX_PAPER_CONFIG_PATH = $ConfigPath }
if ($Broker) { $env:FTX_BROKER = $Broker }

& $venvPython -c "from ftx_paper.cli import api_main; api_main()"
exit $LASTEXITCODE
