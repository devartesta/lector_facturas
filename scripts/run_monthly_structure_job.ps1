param(
    [string]$Root = ""
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if ([string]::IsNullOrWhiteSpace($Root)) {
    $userProfile = [Environment]::GetFolderPath("UserProfile")
    $Root = Join-Path $userProfile "OneDrive - Artesta\ARTESTA - 6. Finances"
}

python scripts/create_next_month_structure.py --root $Root
