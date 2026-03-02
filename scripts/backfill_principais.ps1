param(
    [datetime]$StartDate = [datetime]'2024-01-01',
    [datetime]$EndDate = [datetime]'2024-12-31',
    [int]$WindowMonths = 1,
    [int]$SleepSuccessSeconds = 3,
    [int]$SleepErrorSeconds = 10,
    [string]$PythonExe = "python",
    [string]$AppDataDir = "data",
    [switch]$SyncCatalog,
    [int]$CatalogSyncRetries = 3,
    [int]$CatalogSyncTimeoutSeconds = 45,
    [switch]$FailOnCatalogSyncError
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($WindowMonths -lt 1) {
    throw "WindowMonths must be >= 1."
}
if ($StartDate -gt $EndDate) {
    throw "StartDate must be <= EndDate."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$env:PYTHONPATH = "src"
$env:APP_DATA_DIR = $AppDataDir
$env:ANA_MODE = "live"

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
            $sleepRetry = [Math]::Min(30, 5 * $attempt)
            Write-Host ("[catalog] sync failed; retrying in {0}s..." -f $sleepRetry)
            Start-Sleep -Seconds $sleepRetry
        }
    }

    if (-not $syncOk) {
        $msg = "[catalog] sync failed after retries; continuing backfill with existing catalog."
        if ($FailOnCatalogSyncError) {
            throw $msg
        }
        Write-Warning $msg
    }
}

$reservoirs = @(
    @{ id = 19004; name = "FURNAS" },          # SE/CO
    @{ id = 19014; name = "MARIMBONDO" },
    @{ id = 19017; name = "EMBORCACAO" },
    @{ id = 19025; name = "ITUMBIARA" },
    @{ id = 19018; name = "NOVA PONTE" },
    @{ id = 19119; name = "TRES MARIAS" },
    @{ id = 19128; name = "SERRA DA MESA" },
    @{ id = 19027; name = "SAO SIMAO" },
    @{ id = 19034; name = "ILHA SOLTEIRA" },
    @{ id = 19046; name = "JUPIA" },
    @{ id = 19134; name = "TUCURUI" },         # Norte
    @{ id = 19152; name = "BELO MONTE" },
    @{ id = 19121; name = "SOBRADINHO" },      # Nordeste
    @{ id = 19122; name = "ITAPARICA (LUIZ GONZAGA)" },
    @{ id = 19126; name = "XINGO" },
    @{ id = 19059; name = "FOZ DO AREIA (G B MUNHOZ)" }, # Sul
    @{ id = 19064; name = "SALTO SANTIAGO" },
    @{ id = 19058; name = "ITAIPU" }
)

$logDir = Join-Path $AppDataDir "out/backfill"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$logPath = Join-Path $logDir "backfill_principais_$stamp.csv"

$summary = [ordered]@{
    windows_total = 0
    ok_with_data = 0
    ok_zero = 0
    errors = 0
}

function Write-LogRow {
    param(
        [string]$Outcome,
        [hashtable]$Reservoir,
        [datetime]$WindowStart,
        [datetime]$WindowEnd,
        [object]$ResultObj,
        [string]$RawOutput,
        [string]$ErrorMessage
    )

    $row = [pscustomobject]@{
        timestamp_utc   = (Get-Date).ToUniversalTime().ToString("o")
        reservatorio_id = $Reservoir.id
        reservatorio    = $Reservoir.name
        data_inicial    = $WindowStart.ToString("yyyy-MM-dd")
        data_final      = $WindowEnd.ToString("yyyy-MM-dd")
        outcome         = $Outcome
        status          = if ($null -ne $ResultObj) { $ResultObj.status } else { $null }
        processed       = if ($null -ne $ResultObj) { $ResultObj.processed } else { $null }
        inserted        = if ($null -ne $ResultObj) { $ResultObj.inserted } else { $null }
        existing        = if ($null -ne $ResultObj) { $ResultObj.existing } else { $null }
        source          = if ($null -ne $ResultObj) { $ResultObj.source } else { $null }
        error           = $ErrorMessage
        raw_output      = $RawOutput
    }

    $append = Test-Path $logPath
    $row | Export-Csv -Path $logPath -NoTypeInformation -Encoding UTF8 -Append:$append
}

$pyCommand = "import json; from app.jobs.extract_job import run_once; print(json.dumps(run_once(), ensure_ascii=False))"

foreach ($res in $reservoirs) {
    Write-Host ""
    Write-Host "=== Reservatorio $($res.id) - $($res.name) ==="

    $cursor = $StartDate
    while ($cursor -le $EndDate) {
        $windowEnd = $cursor.AddMonths($WindowMonths).AddDays(-1)
        if ($windowEnd -gt $EndDate) {
            $windowEnd = $EndDate
        }

        $summary.windows_total++

        $env:ANA_RESERVATORIO = "$($res.id)"
        $env:ANA_DATA_INICIAL = $cursor.ToString("yyyy-MM-dd")
        $env:ANA_DATA_FINAL = $windowEnd.ToString("yyyy-MM-dd")

        Write-Host ("  -> {0} a {1}" -f $env:ANA_DATA_INICIAL, $env:ANA_DATA_FINAL)

        $output = & $PythonExe -c $pyCommand 2>&1
        $exitCode = $LASTEXITCODE
        $rawOutput = ($output | ForEach-Object { "$_" }) -join " || "

        if ($exitCode -ne 0) {
            $summary.errors++
            Write-Host "     error (exit=$exitCode)"
            Write-LogRow -Outcome "error" -Reservoir $res -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $null -RawOutput $rawOutput -ErrorMessage "python exit code $exitCode"
            Start-Sleep -Seconds $SleepErrorSeconds
            $cursor = $windowEnd.AddDays(1)
            continue
        }

        $jsonLine = $output | Where-Object { $_ -match '^\s*\{.*\}\s*$' } | Select-Object -Last 1
        if (-not $jsonLine) {
            $summary.errors++
            Write-Host "     error (json not found)"
            Write-LogRow -Outcome "error" -Reservoir $res -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $null -RawOutput $rawOutput -ErrorMessage "json output not found"
            Start-Sleep -Seconds $SleepErrorSeconds
            $cursor = $windowEnd.AddDays(1)
            continue
        }

        try {
            $result = $jsonLine | ConvertFrom-Json
        }
        catch {
            $summary.errors++
            Write-Host "     error (json parse)"
            Write-LogRow -Outcome "error" -Reservoir $res -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $null -RawOutput $rawOutput -ErrorMessage "json parse failed"
            Start-Sleep -Seconds $SleepErrorSeconds
            $cursor = $windowEnd.AddDays(1)
            continue
        }

        if ([int]$result.processed -gt 0) {
            $summary.ok_with_data++
            Write-Host ("     ok processed={0} inserted={1} existing={2}" -f $result.processed, $result.inserted, $result.existing)
            Write-LogRow -Outcome "ok_with_data" -Reservoir $res -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $result -RawOutput $rawOutput -ErrorMessage ""
            Start-Sleep -Seconds $SleepSuccessSeconds
        }
        else {
            $summary.ok_zero++
            Write-Host ("     ok_zero processed={0}" -f $result.processed)
            Write-LogRow -Outcome "ok_zero" -Reservoir $res -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $result -RawOutput $rawOutput -ErrorMessage ""
            Start-Sleep -Seconds $SleepErrorSeconds
        }

        $cursor = $windowEnd.AddDays(1)
    }
}

Write-Host ""
Write-Host "=== Backfill Summary ==="
Write-Host ("windows_total : {0}" -f $summary.windows_total)
Write-Host ("ok_with_data  : {0}" -f $summary.ok_with_data)
Write-Host ("ok_zero       : {0}" -f $summary.ok_zero)
Write-Host ("errors        : {0}" -f $summary.errors)
Write-Host ("log_csv       : {0}" -f $logPath)
