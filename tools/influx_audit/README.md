# Read-only InfluxDB audit helper

`inventory.ps1` is a small, read-only helper for the staged data review. It reads only `INFLUX_URL`, `INFLUX_TOKEN` and `INFLUX_ORG` from the repository-root `.env_agent`; it does not read `.env`, `.env copy` or any other env file. It never prints credential values and does not write query results to disk.

## Stage 1: discover candidate signals

The default invocation scans a 14-day window in the raw `HomeAssistant` bucket and prints matching measurement/entity/field/unit metadata plus point counts. It filters to `value` and `state`, does not return sample values, and does not calculate time profiles:

```powershell
.\tools\influx_audit\inventory.ps1
```

Change the period or bucket explicitly only when needed:

```powershell
.\tools\influx_audit\inventory.ps1 -InventoryDays 7 -Buckets HomeAssistant
```

## Stage 2: profile selected entities

After reviewing Stage 1, pass only exact entities that need timing analysis. The script queries first/last timestamps and median/maximum event gaps within the default seven-day window. Counts in the listing remain the 14-day inventory counts.

```powershell
.\tools\influx_audit\inventory.ps1 -ProfileEntities fems_gridactivepower,e3_vitocal_kompressor
```

For selected numeric series, the following option adds min/mean/max summaries over seven days without returning sample rows:

```powershell
.\tools\influx_audit\inventory.ps1 -SummaryEntities fems_gridactivepower,fems_productiondcactualpower
```

For change-only binary sensors, `-BinaryEntities` reports distinct values and the last state before the profiling window. `-BalanceStartUtc` and `-BalanceStopUtc` produce hourly mean summaries for the named power signals over that bounded interval. `-SnapshotStartUtc` returns the last observed values within one minute for a small fixed set of signals; use it sparingly to verify event alignment or polarity.

For counters, compressors and FENECON signals, use exact entity IDs shown by Stage 1. The Home-Assistant Influx integration in this project stores common units in `_measurement` (e.g. `W` or `kWh`); the `unit` tag was empty on the inspected rows. Confirm each signal's semantics/unit against Home Assistant entity metadata or device documentation before implementation.

All queries use InfluxDB Flux read operations. The helper does not write, delete, downsample or alter retention/configuration.
