param(
    [int]$LookbackDays = 7,
    [int]$InventoryDays = 14,
    [string[]]$Buckets = @("HomeAssistant"),
    [string[]]$ProfileEntities = @(),
    [string[]]$SummaryEntities = @(),
    [string[]]$BinaryEntities = @(),
    [string]$BalanceStartUtc = "",
    [string]$BalanceStopUtc = "",
    [string]$SnapshotStartUtc = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$envPath = Join-Path $repoRoot ".env_agent"
if (-not (Test-Path -LiteralPath $envPath)) { throw ".env_agent was not found." }

# Read only the explicitly authorized agent credential file. Never print its values.
$settings = @{}
foreach ($line in Get-Content -LiteralPath $envPath) {
    if ($line -match '^\s*(INFLUX_URL|INFLUX_TOKEN|INFLUX_ORG)\s*=\s*(.*?)\s*$') {
        $value = $Matches[2].Trim()
        if ($value.Length -ge 2 -and (($value[0] -eq '"' -and $value[-1] -eq '"') -or ($value[0] -eq "'" -and $value[-1] -eq "'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        $settings[$Matches[1]] = $value
    }
}
foreach ($key in @("INFLUX_URL", "INFLUX_TOKEN", "INFLUX_ORG")) {
    if (-not $settings[$key]) { throw "Required key $key is missing in .env_agent." }
}

$baseUrl = $settings["INFLUX_URL"].TrimEnd('/')
$orgQuery = [uri]::EscapeDataString($settings["INFLUX_ORG"])
$queryUri = "$baseUrl/api/v2/query?org=$orgQuery"
Add-Type -AssemblyName System.Net.Http
$httpClient = [System.Net.Http.HttpClient]::new()
$httpClient.Timeout = [TimeSpan]::FromMinutes(3)
$httpClient.DefaultRequestHeaders.Authorization = [System.Net.Http.Headers.AuthenticationHeaderValue]::new("Token", $settings["INFLUX_TOKEN"])
$httpClient.DefaultRequestHeaders.Accept.ParseAdd("text/csv")
$candidateRegex = '(?i)(pv|solar|grid|fems|ess|battery|vitocal|kompressor|verbrauch|consum|power|energy|strom|soc|feed|export|import|load|inverter|fronius|house|mt_stall)'

function Invoke-FluxCsv([string]$Flux) {
    try {
        $content = [System.Net.Http.StringContent]::new($Flux, [System.Text.Encoding]::UTF8, "application/vnd.flux")
        $response = $httpClient.PostAsync($queryUri, $content).GetAwaiter().GetResult()
        $responseText = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
        if (-not $response.IsSuccessStatusCode) {
            throw "HTTP $([int]$response.StatusCode): $responseText"
        }
        $lines = @(($responseText -split "`r?`n") | Where-Object { $_ -and -not $_.StartsWith('#') } | ForEach-Object { if ($_.StartsWith(',')) { $_.Substring(1) } else { $_ } })
        if ($lines.Count -lt 2) { return @() }
        return @($lines | ConvertFrom-Csv)
    }
    catch {
        # Do not include request headers or token in errors/output.
        $errRecord = $_
        $detail = [string]$errRecord.Exception.Message
        if ($settings["INFLUX_TOKEN"]) { $detail = $detail.Replace($settings["INFLUX_TOKEN"], "[REDACTED]") }
        throw "InfluxDB Flux query failed. Check endpoint, permissions, bucket, and query syntax. Details=$detail"
    }
}

function Escape-FluxString([string]$Value) {
    return $Value.Replace('\\', '\\\\').Replace('"', '\\"')
}

function Get-SeriesKey($Row) {
    return "$($Row._measurement)|$($Row.entity_id)|$($Row._field)|$($Row.unit)"
}

Write-Output "Read-only InfluxDB inventory; inventory=$InventoryDays days; targeted profile=$LookbackDays days. Credential values are suppressed."
foreach ($bucket in $Buckets) {
    $bucketFlux = Escape-FluxString $bucket
    $inventoryFlux = @"
from(bucket: "$bucketFlux")
  |> range(start: -${InventoryDays}d)
  |> filter(fn: (r) => exists r["entity_id"] and (r["entity_id"] =~ /$candidateRegex/ or r["_measurement"] =~ /$candidateRegex/))
  |> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "state")
  |> group(columns: ["_measurement", "entity_id", "_field", "unit"])
  |> count(column: "_value")
  |> group()
  |> sort(columns: ["_measurement", "entity_id", "_field"])
"@
    $seriesList = Invoke-FluxCsv $inventoryFlux
    Write-Output "`nBUCKET: $bucket  | matching series with data in last $InventoryDays days: $($seriesList.Count)"
    if ($seriesList.Count -eq 0) { continue }
    $seriesList | Select-Object _measurement, entity_id, _field, unit, _value | Format-Table -AutoSize | Out-String -Width 200 | Write-Output
    if ($ProfileEntities.Count -eq 0) {
        Write-Output "No time-profile entities requested."
    }
    $profileSeries = @($seriesList | Where-Object { $ProfileEntities -contains $_.entity_id -and $_._field -eq "value" })
    if ($ProfileEntities.Count -gt 0) {
        Write-Output "Targeted time profile: $($profileSeries.Count) value series for explicitly requested entities over $LookbackDays days."
    }
    if ($ProfileEntities.Count -gt 0 -and $profileSeries.Count -gt 0) {
    $targetFilters = @($ProfileEntities | ForEach-Object { 'r["entity_id"] == "' + (Escape-FluxString $_) + '"' }) -join ' or '

    $firstFlux = @"
from(bucket: "$bucketFlux")
  |> range(start: -${LookbackDays}d)
  |> filter(fn: (r) => ($targetFilters) and r["_field"] == "value")
  |> group(columns: ["_measurement", "entity_id", "_field", "unit"])
  |> first()
  |> keep(columns: ["_measurement", "entity_id", "_field", "unit", "_time"])
  |> group()
"@
    $lastFlux = $firstFlux.Replace("|> first()", "|> last()")
    $elapsedFlux = @"
from(bucket: "$bucketFlux")
  |> range(start: -${LookbackDays}d)
  |> filter(fn: (r) => ($targetFilters) and r["_field"] == "value")
  |> group(columns: ["_measurement", "entity_id", "_field", "unit"])
  |> sort(columns: ["_time"])
  |> elapsed(unit: 1s, columnName: "elapsed_s")
  |> filter(fn: (r) => r["elapsed_s"] >= 0)
  |> keep(columns: ["_measurement", "entity_id", "_field", "unit", "elapsed_s"])
  |> group(columns: ["_measurement", "entity_id", "_field", "unit"])
"@
    $firstBySeries = @{}
    foreach ($row in (Invoke-FluxCsv $firstFlux)) { $firstBySeries[(Get-SeriesKey $row)] = $row._time }
    $lastBySeries = @{}
    foreach ($row in (Invoke-FluxCsv $lastFlux)) { $lastBySeries[(Get-SeriesKey $row)] = $row._time }
    $medianBySeries = @{}
    foreach ($row in (Invoke-FluxCsv ($elapsedFlux + '  |> quantile(column: "elapsed_s", q: 0.5) |> group()'))) { $medianBySeries[(Get-SeriesKey $row)] = $row.elapsed_s }
    $maxBySeries = @{}
    foreach ($row in (Invoke-FluxCsv ($elapsedFlux + '  |> max(column: "elapsed_s") |> group()'))) { $maxBySeries[(Get-SeriesKey $row)] = $row.elapsed_s }

    $summaries = foreach ($series in $profileSeries) {
        $key = Get-SeriesKey $series
        [pscustomobject]@{
            measurement = $series._measurement
            entity_id = $series.entity_id
            field = $series._field
            unit_tag = $series.unit
            points_in_inventory_window = $series._value
            first_time = $firstBySeries[$key]
            last_time = $lastBySeries[$key]
            median_gap_seconds = $medianBySeries[$key]
            max_gap_seconds_in_lookback = $maxBySeries[$key]
        }
    }
        Write-Output "Series summaries: $(@($summaries).Count)"
        $summaries | Format-Table -AutoSize | Out-String -Width 240 | Write-Output
    }

    if ($SummaryEntities.Count -gt 0) {
        $summaryFilters = @($SummaryEntities | ForEach-Object { 'r["entity_id"] == "' + (Escape-FluxString $_) + '"' }) -join ' or '
        $valueBase = @"
from(bucket: "$bucketFlux")
  |> range(start: -${LookbackDays}d)
  |> filter(fn: (r) => ($summaryFilters) and r["_field"] == "value")
  |> group(columns: ["_measurement", "entity_id", "_field", "unit"])
"@
        $minimum = Invoke-FluxCsv ($valueBase + '  |> min() |> group()')
        $maximum = Invoke-FluxCsv ($valueBase + '  |> max() |> group()')
        $average = Invoke-FluxCsv ($valueBase + '  |> mean() |> group()')
        $minBySeries = @{}
        foreach ($row in $minimum) { $minBySeries[(Get-SeriesKey $row)] = $row._value }
        $maxBySeries = @{}
        foreach ($row in $maximum) { $maxBySeries[(Get-SeriesKey $row)] = $row._value }
        $meanBySeries = @{}
        foreach ($row in $average) { $meanBySeries[(Get-SeriesKey $row)] = $row._value }
        $valueSummaries = foreach ($series in ($seriesList | Where-Object { $SummaryEntities -contains $_.entity_id -and $_._field -eq "value" })) {
            $key = Get-SeriesKey $series
            [pscustomobject]@{
                measurement = $series._measurement
                entity_id = $series.entity_id
                field = $series._field
                min_value_lookback = $minBySeries[$key]
                mean_value_lookback = $meanBySeries[$key]
                max_value_lookback = $maxBySeries[$key]
            }
        }
        Write-Output "Numeric value summaries over $LookbackDays days (no raw points):"
        $valueSummaries | Format-Table -AutoSize | Out-String -Width 200 | Write-Output
    }

    if ($BinaryEntities.Count -gt 0) {
        $binaryFilters = @($BinaryEntities | ForEach-Object { 'r["entity_id"] == "' + (Escape-FluxString $_) + '"' }) -join ' or '
        $binaryFlux = @"
from(bucket: "$bucketFlux")
  |> range(start: -${LookbackDays}d)
  |> filter(fn: (r) => ($binaryFilters) and r["_field"] == "value")
  |> keep(columns: ["_measurement", "entity_id", "_value"])
  |> group(columns: ["_measurement", "entity_id"])
  |> distinct(column: "_value")
  |> group()
"@
        Write-Output "Distinct recent value states for selected binary sensors:"
        Invoke-FluxCsv $binaryFlux | Select-Object _measurement, entity_id, _value | Format-Table -AutoSize | Out-String -Width 180 | Write-Output
        $priorBinaryFlux = @"
from(bucket: "$bucketFlux")
  |> range(start: -${InventoryDays}d, stop: -${LookbackDays}d)
  |> filter(fn: (r) => ($binaryFilters) and r["_field"] == "value")
  |> group(columns: ["_measurement", "entity_id"])
  |> last()
  |> group()
"@
        $profileStartUtc = [datetime]::UtcNow.AddDays(-$LookbackDays)
        $priorStates = foreach ($row in (Invoke-FluxCsv $priorBinaryFlux)) {
            $eventTime = [datetime]::Parse($row._time).ToUniversalTime()
            [pscustomobject]@{
                measurement = $row._measurement
                entity_id = $row.entity_id
                last_value_before_profile = $row._value
                event_time_utc = $row._time
                age_at_profile_start_hours = [math]::Round(($profileStartUtc - $eventTime).TotalHours, 2)
            }
        }
        Write-Output "Last known binary state before the $LookbackDays-day profile (needed to initialize change-only state):"
        $priorStates | Format-Table -AutoSize | Out-String -Width 200 | Write-Output
    }

    if ($BalanceStartUtc -and $BalanceStopUtc) {
        $balanceEntities = @("elektrischer_verbrauch", "fems_gridactivepower", "fems_productiondcactualpower", "fems_essdischargepower", "fems_esssoc")
        $balanceFilters = @($balanceEntities | ForEach-Object { 'r["entity_id"] == "' + (Escape-FluxString $_) + '"' }) -join ' or '
        $startUtc = [datetime]::Parse($BalanceStartUtc).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        $stopUtc = [datetime]::Parse($BalanceStopUtc).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        $balanceFlux = @"
from(bucket: "$bucketFlux")
  |> range(start: $startUtc, stop: $stopUtc)
  |> filter(fn: (r) => ($balanceFilters) and r["_field"] == "value")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "entity_id", "_value"])
  |> group()
  |> pivot(rowKey: ["_time"], columnKey: ["entity_id"], valueColumn: "_value")
  |> sort(columns: ["_time"])
"@
        Write-Output "Hourly mean power sample (W) for $startUtc to ${stopUtc}:"
        Invoke-FluxCsv $balanceFlux | Select-Object _time, elektrischer_verbrauch, fems_gridactivepower, fems_productiondcactualpower, fems_essdischargepower, fems_esssoc | Format-Table -AutoSize | Out-String -Width 240 | Write-Output
    }

    if ($SnapshotStartUtc) {
        $snapshotEntities = @("elektrischer_verbrauch", "fems_gridactivepower", "fems_productiondcactualpower", "fems_essdischargepower", "fems_esssoc")
        $snapshotFilters = @($snapshotEntities | ForEach-Object { 'r["entity_id"] == "' + (Escape-FluxString $_) + '"' }) -join ' or '
        $snapshotStart = [datetime]::Parse($SnapshotStartUtc).ToUniversalTime()
        $snapshotStop = $snapshotStart.AddMinutes(1)
        $snapshotFlux = @"
from(bucket: "$bucketFlux")
  |> range(start: $($snapshotStart.ToString("yyyy-MM-ddTHH:mm:ssZ")), stop: $($snapshotStop.ToString("yyyy-MM-ddTHH:mm:ssZ")))
  |> filter(fn: (r) => ($snapshotFilters) and r["_field"] == "value")
  |> group(columns: ["_measurement", "entity_id"])
  |> last()
  |> keep(columns: ["_time", "_measurement", "entity_id", "_value"])
  |> group()
  |> sort(columns: ["entity_id"])
"@
        Write-Output "Last observed values in the minute after $SnapshotStartUtc (timestamp kept to assess alignment):"
        Invoke-FluxCsv $snapshotFlux | Select-Object _time, _measurement, entity_id, _value | Format-Table -AutoSize | Out-String -Width 200 | Write-Output
    }
}
