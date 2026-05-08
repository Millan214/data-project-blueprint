# Bootstrap Python (via winget if missing), then run setup_env.py to install deps.
$ErrorActionPreference = 'Stop'

function Get-PythonCommand {
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        & py -3 --version *> $null
        if ($LASTEXITCODE -eq 0) { return 'py -3' }
    }
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        & python --version *> $null
        if ($LASTEXITCODE -eq 0) { return 'python' }
    }
    return $null
}

$pythonCmd = Get-PythonCommand
if (-not $pythonCmd) {
    Write-Host 'Python not found - installing via winget...'
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if (-not $winget) {
        Write-Error 'winget is not available on this machine. Install Python 3 manually from https://www.python.org/downloads/ and re-run.'
        exit 1
    }
    & winget install --id Python.Python.3.12 --silent --accept-package-agreements --accept-source-agreements
    if ($LASTEXITCODE -ne 0) {
        Write-Error "winget install failed (exit $LASTEXITCODE)."
        exit $LASTEXITCODE
    }
    # Refresh PATH for the current session so the new python is visible without restarting the shell.
    $machinePath = [System.Environment]::GetEnvironmentVariable('Path', 'Machine')
    $userPath = [System.Environment]::GetEnvironmentVariable('Path', 'User')
    $env:Path = "$machinePath;$userPath"
    $pythonCmd = Get-PythonCommand
    if (-not $pythonCmd) {
        Write-Error 'Python still not found after install. Open a new terminal and re-run this config.'
        exit 1
    }
}

Write-Host "Using Python: $pythonCmd"
$script = Join-Path $PSScriptRoot 'setup_env.py'
if ($pythonCmd -eq 'py -3') {
    & py -3 $script
} else {
    & python $script
}
exit $LASTEXITCODE
