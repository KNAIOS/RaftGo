param([string]$Go = 'D:\go_sdk\go1.27.1\bin\go.exe')
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
if (-not (Test-Path -LiteralPath $Go)) { $Go = (Get-Command go -ErrorAction Stop).Source }
$keys = @('GOMODCACHE','GOCACHE','GOOS','GOARCH','CGO_ENABLED')
$saved = @{}
foreach ($key in $keys) { $saved[$key] = [Environment]::GetEnvironmentVariable($key, 'Process') }
try {
    $env:GOMODCACHE = Join-Path (Get-Location) '.cache\mod'
    $env:GOCACHE = Join-Path (Get-Location) '.cache\build'
    $env:GOOS = 'linux'; $env:GOARCH = 'amd64'; $env:CGO_ENABLED = '0'
    New-Item -ItemType Directory -Path bin -Force | Out-Null
    & $Go build -tags nomsgpack -trimpath -ldflags '-s -w' -o bin/raftkv-linux-amd64 .
    if ($LASTEXITCODE -ne 0) { throw 'Go build failed' }
    docker build -f Dockerfile.local -t raftkv:local .
    if ($LASTEXITCODE -ne 0) { throw 'Docker build failed' }
} finally {
    foreach ($key in $keys) { [Environment]::SetEnvironmentVariable($key, $saved[$key], 'Process') }
}
