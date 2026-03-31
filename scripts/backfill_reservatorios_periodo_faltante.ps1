<#
.SYNOPSIS
Preenche apenas o periodo historico faltante no inicio da serie de cada reservatorio.

.DESCRIPTION
Carrega o catalogo da tabela ana_reservatorios, inspeciona a cobertura atual da
tabela ana_medicoes e executa backfill apenas para o intervalo anterior a
primeira medicao ja existente de cada reservatorio. Isso ajuda a uniformizar o
periodo inicial dos reservatorios sem refazer toda a carga historica.

.PARAMETER TargetStartDate
Data inicial desejada para todos os reservatorios.

.PARAMETER FallbackEndDate
Data final usada apenas para reservatorios sem nenhuma medicao no banco. Se nao
for informada, o script usa a maior data ja presente no banco; se o banco estiver
vazio, usa ontem.

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
.\scripts\backfill_reservatorios_periodo_faltante.ps1 -TargetStartDate '2018-01-01' -SyncCatalog

.EXAMPLE
.\scripts\backfill_reservatorios_periodo_faltante.ps1 -TargetStartDate '2024-01-01' -WindowMonths 2
#>

param(
    [Parameter(Mandatory = $true)]
    [datetime]$TargetStartDate,
    [datetime]$FallbackEndDate,
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

$rangeStart = $TargetStartDate.Date

if ($WindowMonths -lt 1) {
    throw "WindowMonths must be >= 1."
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

$coveragePy = @"
import json
import sqlite3
import sys
from pathlib import Path

db_path = Path(sys.argv[1]) / "out" / "ana.db"
con = sqlite3.connect(db_path)
con.row_factory = sqlite3.Row
coverage_rows = con.execute(
    '''
    SELECT
        reservatorio_id,
        MIN(data_medicao) AS min_data,
        MAX(data_medicao) AS max_data,
        COUNT(*) AS total
    FROM ana_medicoes
    GROUP BY reservatorio_id
    '''
).fetchall()
db_row = con.execute(
    '''
    SELECT
        MIN(data_medicao) AS db_min,
        MAX(data_medicao) AS db_max
    FROM ana_medicoes
    '''
).fetchone()
print(
    json.dumps(
        {
            "coverage": [dict(row) for row in coverage_rows],
            "db_min": db_row["db_min"],
            "db_max": db_row["db_max"],
        },
        ensure_ascii=False,
    )
)
con.close()
"@

Write-Host "[coverage] loading measurement coverage from DB..."
$coverageOutput = & $PythonExe -c $coveragePy $AppDataDir 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "[coverage] failed to inspect measurement coverage."
}

$coverageText = ($coverageOutput | ForEach-Object { "$_" }) -join [Environment]::NewLine
try {
    $coveragePayload = $coverageText | ConvertFrom-Json
}
catch {
    throw "[coverage] failed to parse coverage json output."
}

$coverageMap = @{}
foreach ($row in @($coveragePayload.coverage)) {
    $coverageMap[[int]$row.reservatorio_id] = $row
}

if ($PSBoundParameters.ContainsKey("FallbackEndDate")) {
    $fallbackEnd = $FallbackEndDate.Date
}
elseif ($null -ne $coveragePayload.db_max -and [string]$coveragePayload.db_max -ne "") {
    $fallbackEnd = [datetime]::ParseExact(
        [string]$coveragePayload.db_max,
        "yyyy-MM-dd",
        [System.Globalization.CultureInfo]::InvariantCulture
    ).Date
}
else {
    $fallbackEnd = (Get-Date).AddDays(-1).Date
}

if ($rangeStart -gt $fallbackEnd) {
    throw "TargetStartDate must be <= effective fallback end date."
}

$logDir = Join-Path $AppDataDir "out/backfill"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$logPath = Join-Path $logDir "backfill_reservatorios_periodo_faltante_$stamp.csv"

$summary = [ordered]@{
    target_start              = $rangeStart.ToString("yyyy-MM-dd")
    fallback_end              = $fallbackEnd.ToString("yyyy-MM-dd")
    reservatorios_total       = $catalogRows.Count
    reservatorios_com_gap     = 0
    reservatorios_sem_gap     = 0
    reservatorios_sem_dados   = 0
    windows_total             = 0
    ok_with_data              = 0
    ok_zero                   = 0
    errors                    = 0
}

function Write-LogRow {
    param(
        [object]$ReservoirRow,
        [string]$CoverageStart,
        [string]$GapStart,
        [string]$GapEnd,
        [string]$Outcome,
        $WindowStart,
        $WindowEnd,
        [object]$ResultObj,
        [string]$RawOutput,
        [string]$ErrorMessage
    )

    $row = [pscustomobject]@{
        timestamp_utc        = (Get-Date).ToUniversalTime().ToString("o")
        reservatorio_id      = $ReservoirRow.reservatorio_id
        reservatorio         = $ReservoirRow.reservatorio
        uf                   = $ReservoirRow.uf
        subsistema           = $ReservoirRow.subsistema
        cobertura_inicial    = $CoverageStart
        gap_inicial          = $GapStart
        gap_final            = $GapEnd
        data_inicial         = if ($null -ne $WindowStart) { $WindowStart.ToString("yyyy-MM-dd") } else { $null }
        data_final           = if ($null -ne $WindowEnd) { $WindowEnd.ToString("yyyy-MM-dd") } else { $null }
        outcome              = $Outcome
        status               = if ($null -ne $ResultObj) { $ResultObj.status } else { $null }
        processed            = if ($null -ne $ResultObj) { $ResultObj.processed } else { $null }
        inserted             = if ($null -ne $ResultObj) { $ResultObj.inserted } else { $null }
        existing             = if ($null -ne $ResultObj) { $ResultObj.existing } else { $null }
        source               = if ($null -ne $ResultObj) { $ResultObj.source } else { $null }
        run_id               = if ($null -ne $ResultObj) { $ResultObj.run_id } else { $null }
        error                = $ErrorMessage
        raw_output           = $RawOutput
    }

    $append = Test-Path $logPath
    $row | Export-Csv -Path $logPath -NoTypeInformation -Encoding UTF8 -Append:$append
}

foreach ($row in ($catalogRows | Sort-Object { [int]$_.reservatorio_id })) {
    $rid = [int]$row.reservatorio_id
    $name = [string]$row.reservatorio

    $coverage = $coverageMap[$rid]
    $coverageStart = $null
    if ($null -ne $coverage -and $null -ne $coverage.min_data -and [string]$coverage.min_data -ne "") {
        $coverageStart = [string]$coverage.min_data
    }

    $gapStart = $rangeStart
    if ($null -ne $coverageStart) {
        $gapEnd = [datetime]::ParseExact(
            $coverageStart,
            "yyyy-MM-dd",
            [System.Globalization.CultureInfo]::InvariantCulture
        ).AddDays(-1).Date
    }
    else {
        $gapEnd = $fallbackEnd
        $summary.reservatorios_sem_dados++
    }

    if ($gapStart -gt $gapEnd) {
        $summary.reservatorios_sem_gap++
        Write-Host ("[skip] {0} - {1} coverage_start={2}" -f $rid, $name, $(if ($coverageStart) { $coverageStart } else { "sem_dados" }))
        Write-LogRow -ReservoirRow $row -CoverageStart $coverageStart -GapStart $gapStart.ToString("yyyy-MM-dd") -GapEnd $gapEnd.ToString("yyyy-MM-dd") -Outcome "skip_sem_gap" -WindowStart $null -WindowEnd $null -ResultObj $null -RawOutput "" -ErrorMessage ""
        continue
    }

    $summary.reservatorios_com_gap++
    $gapStartIso = $gapStart.ToString("yyyy-MM-dd")
    $gapEndIso = $gapEnd.ToString("yyyy-MM-dd")

    Write-Host ""
    Write-Host ("=== Reservatorio {0} - {1} ===" -f $rid, $name)
    Write-Host ("  cobertura_inicial={0}" -f $(if ($coverageStart) { $coverageStart } else { "sem_dados" }))
    Write-Host ("  backfill_gap={0} a {1}" -f $gapStartIso, $gapEndIso)

    $cursor = $gapStart
    while ($cursor -le $gapEnd) {
        $windowEnd = $cursor.AddMonths($WindowMonths).AddDays(-1)
        if ($windowEnd -gt $gapEnd) {
            $windowEnd = $gapEnd
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
            Write-LogRow -ReservoirRow $row -CoverageStart $coverageStart -GapStart $gapStartIso -GapEnd $gapEndIso -Outcome "error" -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $null -RawOutput $rawOutput -ErrorMessage ("python exit code {0}" -f $exitCode)
            Start-Sleep -Seconds $SleepErrorSeconds
            $cursor = $windowEnd.AddDays(1)
            continue
        }

        $jsonLine = $output | Where-Object { $_ -match '^\s*\{.*\}\s*$' } | Select-Object -Last 1
        if (-not $jsonLine) {
            $summary.errors++
            Write-Host "     error (json not found)"
            Write-LogRow -ReservoirRow $row -CoverageStart $coverageStart -GapStart $gapStartIso -GapEnd $gapEndIso -Outcome "error" -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $null -RawOutput $rawOutput -ErrorMessage "json output not found"
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
            Write-LogRow -ReservoirRow $row -CoverageStart $coverageStart -GapStart $gapStartIso -GapEnd $gapEndIso -Outcome "error" -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $null -RawOutput $rawOutput -ErrorMessage "json parse failed"
            Start-Sleep -Seconds $SleepErrorSeconds
            $cursor = $windowEnd.AddDays(1)
            continue
        }

        if ([int]$result.processed -gt 0) {
            $summary.ok_with_data++
            Write-Host ("     ok processed={0} inserted={1} existing={2}" -f $result.processed, $result.inserted, $result.existing)
            Write-LogRow -ReservoirRow $row -CoverageStart $coverageStart -GapStart $gapStartIso -GapEnd $gapEndIso -Outcome "ok_with_data" -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $result -RawOutput $rawOutput -ErrorMessage ""
            Start-Sleep -Seconds $SleepSuccessSeconds
        }
        else {
            $summary.ok_zero++
            Write-Host ("     ok_zero processed={0}" -f $result.processed)
            Write-LogRow -ReservoirRow $row -CoverageStart $coverageStart -GapStart $gapStartIso -GapEnd $gapEndIso -Outcome "ok_zero" -WindowStart $cursor -WindowEnd $windowEnd -ResultObj $result -RawOutput $rawOutput -ErrorMessage ""
            Start-Sleep -Seconds $SleepErrorSeconds
        }

        $cursor = $windowEnd.AddDays(1)
    }
}

Write-Host ""
Write-Host "=== Backfill Missing Coverage Summary ==="
Write-Host ("target_start            : {0}" -f $summary.target_start)
Write-Host ("fallback_end            : {0}" -f $summary.fallback_end)
Write-Host ("reservatorios_total     : {0}" -f $summary.reservatorios_total)
Write-Host ("reservatorios_com_gap   : {0}" -f $summary.reservatorios_com_gap)
Write-Host ("reservatorios_sem_gap   : {0}" -f $summary.reservatorios_sem_gap)
Write-Host ("reservatorios_sem_dados : {0}" -f $summary.reservatorios_sem_dados)
Write-Host ("windows_total           : {0}" -f $summary.windows_total)
Write-Host ("ok_with_data            : {0}" -f $summary.ok_with_data)
Write-Host ("ok_zero                 : {0}" -f $summary.ok_zero)
Write-Host ("errors                  : {0}" -f $summary.errors)
Write-Host ("log_csv                 : {0}" -f $logPath)
