<#
.SYNOPSIS
Executa backfill historico para todos os reservatorios do catalogo.

.DESCRIPTION
Carrega o catalogo da tabela ana_reservatorios, percorre janelas de datas por
reservatorio e chama o extract_job em modo live. Opcionalmente sincroniza o
catalogo antes do backfill e gera CSV consolidado com status de cada janela
processada.

.PARAMETER StartDate
Data inicial do backfill.

.PARAMETER EndDate
Data final do backfill.

.PARAMETER WindowMonths
Quantidade de meses por janela de execucao.

.PARAMETER SleepSuccessSeconds
Espera entre janelas com sucesso.

.PARAMETER SleepErrorSeconds
Espera entre janelas com erro ou sem parse valido.

.PARAMETER PythonExe
Executavel Python utilizado para chamadas do pipeline.

.PARAMETER AppDataDir
Diretorio base de dados/artifacts (equivalente a APP_DATA_DIR).

.PARAMETER SyncCatalog
Sincroniza catalogo antes de iniciar o backfill.

.PARAMETER CatalogSyncRetries
Quantidade de tentativas de sincronizacao do catalogo.

.PARAMETER CatalogSyncTimeoutSeconds
Timeout em segundos para cada tentativa de sync.

.PARAMETER FailOnCatalogSyncError
Interrompe o script se a sincronizacao do catalogo falhar.

.PARAMETER CatalogListLimit
Limite maximo de reservatorios retornados pela listagem do catalogo.

.EXAMPLE
.\scripts\backfill_reservatorios_catalogo.ps1 -StartDate '2026-03-08' -EndDate '2026-03-26' -SyncCatalog

.EXAMPLE
.\scripts\backfill_reservatorios_catalogo.ps1 -StartDate '2025-01-01' -EndDate '2025-03-31' -WindowMonths 2
#>

param(
    [Parameter(Mandatory = $true)]
    [datetime]$StartDate,
    [Parameter(Mandatory = $true)]
    [datetime]$EndDate,
    [int]$WindowMonths = 1,
    [int]$SleepSuccessSeconds = 3,
    [int]$SleepErrorSeconds = 10,
    [string]$PythonExe = "python",
    [string]$AppDataDir = "data",
    [switch]$SyncCatalog,
    [int]$CatalogSyncRetries = 3,
    [int]$CatalogSyncTimeoutSeconds = 45,
    [switch]$FailOnCatalogSyncError,
    [int]$CatalogListLimit = 10000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$rangeStart = $StartDate.Date
$rangeEnd = $EndDate.Date

if ($WindowMonths -lt 1) {
    throw "WindowMonths must be >= 1."
}
if ($rangeStart -gt $rangeEnd) {
    throw "StartDate must be <= EndDate."
}
if ($CatalogListLimit -lt 1) {
    throw "CatalogListLimit must be >= 1."
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

Write-Host "[catalog] loading reservoir catalog from DB..."
$catalogOutput = & $PythonExe -m app.ana.catalog list --json --limit $CatalogListLimit 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "[catalog] failed to list reservoir catalog."
}

$catalogText = ($catalogOutput | ForEach-Object { "$_" }) -join [Environment]::NewLine
try {
    $catalogRows = @($catalogText | ConvertFrom-Json)
}
catch {
    throw "[catalog] failed to parse catalog json output."
}

if ($catalogRows.Count -eq 0) {
    throw "[catalog] catalog is empty. Run: python -m app.ana.catalog sync"
}

$logDir = Join-Path $AppDataDir "out/backfill"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$logPath = Join-Path $logDir "backfill_reservatorios_catalogo_$stamp.csv"

$summary = [ordered]@{
    start_date      = $rangeStart.ToString("yyyy-MM-dd")
    end_date        = $rangeEnd.ToString("yyyy-MM-dd")
    reservatorios   = $catalogRows.Count
    windows_total   = 0
    ok_with_data    = 0
    ok_zero         = 0
    errors          = 0
}

function Write-LogRow {
    param(
        [object]$ReservoirRow,
        [string]$Outcome,
        [datetime]$WindowStart,
        [datetime]$WindowEnd,
        [object]$ResultObj,
        [string]$RawOutput,
        [string]$ErrorMessage
    )

    $row = [pscustomobject]@{
        timestamp_utc   = (Get-Date).ToUniversalTime().ToString("o")
        reservatorio_id = $ReservoirRow.reservatorio_id
        reservatorio    = $ReservoirRow.reservatorio
        uf              = $ReservoirRow.uf
        subsistema      = $ReservoirRow.subsistema
        data_inicial    = $WindowStart.ToString("yyyy-MM-dd")
        data_final      = $WindowEnd.ToString("yyyy-MM-dd")
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

    Write-Host ""
    Write-Host "=== Reservatorio $rid - $name ==="

    $cursor = $rangeStart
    while ($cursor -le $rangeEnd) {
        $windowEnd = $cursor.AddMonths($WindowMonths).AddDays(-1)
        if ($windowEnd -gt $rangeEnd) {
            $windowEnd = $rangeEnd
        }

        $summary.windows_total++

        $windowStartIso = $cursor.ToString("yyyy-MM-dd")
        $windowEndIso = $windowEnd.ToString("yyyy-MM-dd")
        $env:ANA_RESERVATORIO = "$rid"

        Write-Host ("  -> {0} a {1}" -f $windowStartIso, $windowEndIso)

        $output = & $PythonExe -m app.jobs.extract_job --since $windowStartIso --until $windowEndIso --force --log-level INFO 2>&1
        $exitCode = $LASTEXITCODE
        $rawOutput = ($output | ForEach-Object { "$_" }) -join " || "

        if ($exitCode -ne 0) {
            $summary.errors++
            Write-Host ("     error (exit={0})" -f $exitCode)
            Write-LogRow -ReservoirRow $row -Outcome "error" -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $null -RawOutput $rawOutput -ErrorMessage ("python exit code {0}" -f $exitCode)
            Start-Sleep -Seconds $SleepErrorSeconds
            $cursor = $windowEnd.AddDays(1)
            continue
        }

        $jsonLine = $output | Where-Object { $_ -match '^\s*\{.*\}\s*$' } | Select-Object -Last 1
        if (-not $jsonLine) {
            $summary.errors++
            Write-Host "     error (json not found)"
            Write-LogRow -ReservoirRow $row -Outcome "error" -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $null -RawOutput $rawOutput -ErrorMessage "json output not found"
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
            Write-LogRow -ReservoirRow $row -Outcome "error" -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $null -RawOutput $rawOutput -ErrorMessage "json parse failed"
            Start-Sleep -Seconds $SleepErrorSeconds
            $cursor = $windowEnd.AddDays(1)
            continue
        }

        if ([int]$result.processed -gt 0) {
            $summary.ok_with_data++
            Write-Host ("     ok processed={0} inserted={1} existing={2}" -f $result.processed, $result.inserted, $result.existing)
            Write-LogRow -ReservoirRow $row -Outcome "ok_with_data" -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $result -RawOutput $rawOutput -ErrorMessage ""
            Start-Sleep -Seconds $SleepSuccessSeconds
        }
        else {
            $summary.ok_zero++
            Write-Host ("     ok_zero processed={0}" -f $result.processed)
            Write-LogRow -ReservoirRow $row -Outcome "ok_zero" -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $result -RawOutput $rawOutput -ErrorMessage ""
            Start-Sleep -Seconds $SleepErrorSeconds
        }

        $cursor = $windowEnd.AddDays(1)
    }
}

Write-Host ""
Write-Host "=== Backfill Catalog Summary ==="
Write-Host ("start_date    : {0}" -f $summary.start_date)
Write-Host ("end_date      : {0}" -f $summary.end_date)
Write-Host ("reservatorios : {0}" -f $summary.reservatorios)
Write-Host ("windows_total : {0}" -f $summary.windows_total)
Write-Host ("ok_with_data  : {0}" -f $summary.ok_with_data)
Write-Host ("ok_zero       : {0}" -f $summary.ok_zero)
Write-Host ("errors        : {0}" -f $summary.errors)
Write-Host ("log_csv       : {0}" -f $logPath)
