param(
    [datetime]$TargetDate = (Get-Date),
    [switch]$UseYesterday,
    [switch]$SyncCatalog,
    [int]$CatalogSyncRetries = 3,
    [int]$CatalogSyncTimeoutSeconds = 45,
    [int]$SleepSeconds = 2,
    [string]$PythonExe = "python",
    [string]$AppDataDir = "data"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$env:PYTHONPATH = "src"
$env:APP_DATA_DIR = $AppDataDir
$env:ANA_MODE = "live"

if ($UseYesterday) {
    $TargetDate = (Get-Date).AddDays(-1)
}
$targetIso = $TargetDate.Date.ToString("yyyy-MM-dd")

Write-Host ("[daily] target_date={0}" -f $targetIso)

if ($SyncCatalog) {
    $syncOk = $false
    for ($attempt = 1; $attempt -le $CatalogSyncRetries; $attempt++) {
        Write-Host ("[catalog] sync attempt {0}/{1}..." -f $attempt, $CatalogSyncRetries)
        & $PythonExe -m app.ana.catalog sync --json --timeout-s $CatalogSyncTimeoutSeconds
        if ($LASTEXITCODE -eq 0) {
            $syncOk = $true
            break
        }
        if ($attempt -lt $CatalogSyncRetries) {
            Start-Sleep -Seconds (5 * $attempt)
        }
    }
    if (-not $syncOk) {
        throw "[catalog] failed to sync after retries."
    }
}

Write-Host "[daily] loading reservoir catalog from DB..."
$catalogOutput = & $PythonExe -m app.ana.catalog list --json --limit 10000 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "[daily] failed to list reservoir catalog."
}

$catalogText = ($catalogOutput | ForEach-Object { "$_" }) -join [Environment]::NewLine
try {
    $catalogRows = @($catalogText | ConvertFrom-Json)
}
catch {
    throw "[daily] failed to parse catalog json output."
}

if ($catalogRows.Count -eq 0) {
    throw "[daily] catalog is empty. Run: python -m app.ana.catalog sync"
}

$logDir = Join-Path $AppDataDir "out/backfill"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$logPath = Join-Path $logDir "daily_update_$stamp.csv"

$summary = [ordered]@{
    target_date  = $targetIso
    total        = 0
    ok_with_data = 0
    ok_zero      = 0
    errors       = 0
}

function Write-LogRow {
    param(
        [object]$ReservoirRow,
        [string]$Outcome,
        [object]$ResultObj,
        [string]$RawOutput,
        [string]$ErrorMessage
    )

    $row = [pscustomobject]@{
        timestamp_utc   = (Get-Date).ToUniversalTime().ToString("o")
        target_date     = $targetIso
        reservatorio_id = $ReservoirRow.reservatorio_id
        reservatorio    = $ReservoirRow.reservatorio
        uf              = $ReservoirRow.uf
        subsistema      = $ReservoirRow.subsistema
        outcome         = $Outcome
        status          = if ($null -ne $ResultObj) { $ResultObj.status } else { $null }
        processed       = if ($null -ne $ResultObj) { $ResultObj.processed } else { $null }
        inserted        = if ($null -ne $ResultObj) { $ResultObj.inserted } else { $null }
        existing        = if ($null -ne $ResultObj) { $ResultObj.existing } else { $null }
        source          = if ($null -ne $ResultObj) { $ResultObj.source } else { $null }
        run_id          = if ($null -ne $ResultObj) { $ResultObj.run_id } else { $null }
        error           = $ErrorMessage
        raw_output      = $RawOutput
    }

    $append = Test-Path $logPath
    $row | Export-Csv -Path $logPath -NoTypeInformation -Encoding UTF8 -Append:$append
}

foreach ($row in ($catalogRows | Sort-Object { [int]$_.reservatorio_id })) {
    $rid = [int]$row.reservatorio_id
    $name = [string]$row.reservatorio
    $summary.total++

    $env:ANA_RESERVATORIO = "$rid"
    Write-Host ("[daily] {0} - {1} -> {2}" -f $rid, $name, $targetIso)

    $output = & $PythonExe -m app.jobs.extract_job --since $targetIso --until $targetIso --force --log-level INFO 2>&1
    $exitCode = $LASTEXITCODE
    $rawOutput = ($output | ForEach-Object { "$_" }) -join " || "

    if ($exitCode -ne 0) {
        $summary.errors++
        Write-Host ("  error exit={0}" -f $exitCode)
        Write-LogRow -ReservoirRow $row -Outcome "error" -ResultObj $null -RawOutput $rawOutput -ErrorMessage ("python exit code {0}" -f $exitCode)
        Start-Sleep -Seconds $SleepSeconds
        continue
    }

    $jsonLine = $output | Where-Object { $_ -match '^\s*\{.*\}\s*$' } | Select-Object -Last 1
    if (-not $jsonLine) {
        $summary.errors++
        Write-Host "  error json_not_found"
        Write-LogRow -ReservoirRow $row -Outcome "error" -ResultObj $null -RawOutput $rawOutput -ErrorMessage "json output not found"
        Start-Sleep -Seconds $SleepSeconds
        continue
    }

    try {
        $result = $jsonLine | ConvertFrom-Json
    }
    catch {
        $summary.errors++
        Write-Host "  error json_parse_failed"
        Write-LogRow -ReservoirRow $row -Outcome "error" -ResultObj $null -RawOutput $rawOutput -ErrorMessage "json parse failed"
        Start-Sleep -Seconds $SleepSeconds
        continue
    }

    if ([int]$result.processed -gt 0) {
        $summary.ok_with_data++
        Write-Host ("  ok processed={0} inserted={1} existing={2}" -f $result.processed, $result.inserted, $result.existing)
        Write-LogRow -ReservoirRow $row -Outcome "ok_with_data" -ResultObj $result -RawOutput $rawOutput -ErrorMessage ""
    }
    else {
        $summary.ok_zero++
        Write-Host ("  ok_zero processed={0}" -f $result.processed)
        Write-LogRow -ReservoirRow $row -Outcome "ok_zero" -ResultObj $result -RawOutput $rawOutput -ErrorMessage ""
    }

    Start-Sleep -Seconds $SleepSeconds
}

Write-Host ""
Write-Host "=== Daily Update Summary ==="
Write-Host ("target_date  : {0}" -f $summary.target_date)
Write-Host ("total        : {0}" -f $summary.total)
Write-Host ("ok_with_data : {0}" -f $summary.ok_with_data)
Write-Host ("ok_zero      : {0}" -f $summary.ok_zero)
Write-Host ("errors       : {0}" -f $summary.errors)
Write-Host ("log_csv      : {0}" -f $logPath)
