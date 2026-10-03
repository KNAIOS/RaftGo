param([switch]$LocalBuild, [string]$Go = 'D:\go_sdk\go1.27.1\bin\go.exe')
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
foreach ($node in @('node1','node2','node3')) {
    New-Item -ItemType Directory -Path (Join-Path 'data' $node) -Force | Out-Null
}
if ($LocalBuild) {
    & "$PSScriptRoot/build-local.ps1" -Go $Go
    docker compose -f compose.yaml -f compose.local.yaml up -d --no-build --wait
} else {
    docker compose up -d --build --wait
}
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose failed' }
docker compose ps
