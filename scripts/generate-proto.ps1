param(
    [string]$Protoc = '',
    [string]$PluginDir = ''
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
if (-not $Protoc) { $Protoc = Join-Path (Get-Location) '.cache/tools/protoc/bin/protoc.exe' }
if (-not $PluginDir) { $PluginDir = Join-Path (Get-Location) '.cache/tools/bin' }
$savedPath = $env:PATH
try {
    $env:PATH = "$PluginDir;$savedPath"
    & $Protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/kv/v1/kv.proto
    if ($LASTEXITCODE -ne 0) { throw 'Protobuf generation failed' }
} finally { $env:PATH = $savedPath }
