# Home Assistant InfluxDB Processing

This repository processes Home Assistant time-series data stored in InfluxDB 2.x.
It repairs heat-pump counter data, derives heat-pump statistics and corrected
house consumption, and can run configurable battery-storage simulations.
Production runs also publish retained run status to Home Assistant over MQTT.

The application is intentionally configuration-driven. The Python code contains
the processing algorithms; stage-specific JSON files define buckets, source
series, output series, model versions, and enabled scenarios.

## What the application does

The application processes complete local calendar days in `Europe/Berlin`. The
current day is never processed. Processing is incremental: every processor
looks up its last output day for its configured version and continues through
yesterday. Timestamps are queried and written as UTC-aware timestamps, while
day boundaries are calculated in `Europe/Berlin`, including 23-hour and
25-hour DST days.

### Processing pipeline

1. Load `.env`, the selected stage configuration, logging, and optional MQTT.
2. Connect to InfluxDB and find the first available day in the input bucket.
3. Run the configured Home Assistant processors in order.
4. Run battery scenarios when the `scenarios` section is present.
5. Write a run summary, publish the final MQTT status, and close connections.

### Heat-pump counter repair

The `fix_waermepumpe_stromverbrauch` processor repairs daily heat-pump
consumption counters, including missing or shifted resets, and writes a
monotonic, normalized daily curve. The configured production output is:

- Bucket: `HomeAssistant_processed`
- Measurement: `fix_waermepumpe_stromverbrauch`
- Field: `value`
- Tags: `entity_id`, `version`, `unit`

### Daily aggregates

The `daily_aggregate` processor calculates daily counter totals for the
configured heat-pump entities and writes them to:

- Measurement: `Waermepumpe_statistik`
- Entity: configured by `output_entity_id` (`Gesamt` in the supplied stages)
- Field: `daily_sum`
- Unit: `kWh`

### Heat-pump statistics

`Waermepumpe_statistik` distributes positive counter deltas over time. Compressor
activity and the configured post-run period are used as weights. Positive
`fems_gridactivepower` is treated as one shared grid budget and is not counted
twice for two heat pumps. The calculated PV share is therefore a derived
remainder, not a directly measured PV flow.

The processor reads repaired counters from
`fix_waermepumpe_stromverbrauch` and writes interval and daily values to the
configured `Waermepumpe_statistik` measurement.

The statistic fields are:

| Field | Unit | Meaning |
| --- | --- | --- |
| `pv_contribution` | `kWh` | Derived PV contribution for an interval |
| `grid_import` | `kWh` | Shared grid allocation for an interval |
| `daily_pv` | `kWh` | Daily PV contribution |
| `daily_grid_import` | `kWh` | Daily shared grid allocation |
| `daily_sum` | `kWh` | Daily aggregate output from the daily-aggregate processor |

The entity ID is the configured heat-pump entity for per-pump values and
`Gesamt` for the configured total.

### Corrected house consumption

`corrected_house_consumption` treats configured source measurements as
change-only power values. A value remains effective until the next change; a
value immediately before the day is loaded when available. Values are
time-weighted over the configured interval and written at the interval start.
Negative results are clamped to zero.

The supplied configuration uses a 90-second interval:

- Measurement: `Hausverbrauch_korrigiert`
- Entity: `Hausverbrauch_korrigiert`
- Field: `value`
- Unit: `W`
- Version: `v1`

The source is `W / elektrischer_verbrauch / value` in the `HomeAssistant`
bucket. The processor does not add `mt_stall_neu` a second time when the input
already represents the corrected house load.

### Battery scenarios

The scenario runner uses sample-and-hold input series and an idealized battery
model:

1. PV supplies the house load.
2. Remaining PV charges the battery up to its limits.
3. Remaining PV is exported.
4. A load deficit discharges the battery up to its limits.
5. The remaining deficit is imported from the grid.

Scenario simulation is not a calibrated forecast and is not a complete AC
energy balance. It is a reproducible comparison model. The configured version
is the reprocessing boundary: changing historical inputs or model assumptions
requires a new scenario version so that the series are recalculated
chronologically.

The scenario output uses the configured scenario bucket and the
`batterie_szenarien` measurement. Every point has these tags:

| Tag | Meaning |
| --- | --- |
| `entity_id` | Signal name |
| `scenario` | Battery definition |
| `pv_mode` | PV source selection |
| `version` | Scenario model version |
| `unit` | `%`, `W`, or `kWh` |

Interval fields:

| Entity IDs | Field | Unit |
| --- | --- | --- |
| `soc_pct`, `stored_energy` | `actual` | `%`, `kWh` |
| `house_load`, `pv_generation`, `pv_to_load` | `actual` | `W` |
| `pv_to_battery`, `battery_to_load`, `grid_import`, `grid_export` | `actual` | `W` |

Daily energy entities use `daily_sum` in `kWh`. Daily SOC uses `start` and
`end` fields in percent and also stores start/end energy in kWh.

The supplied PV modes are:

- `without_old_pv`: local PV (`fems_pv` and `mt_stall_neu`)
- `with_old_pv`: local PV plus `mt_stall_alt_1` and `mt_stall_alt_2`

The supplied battery definitions are `current_battery`, `without_battery`,
`16_modules_2_towers`, `15_modules_1_tower`, `22_modules_2_towers`, and
`30_modules_2_towers`. Only definitions with `"enabled": true` are processed.
`without_battery` is the zero-capacity reference scenario.

Optional quality analysis compares the current battery with measured SOC and
grid power. Daily quality points use `quality` and `signed_error` fields;
time-series comparison points use the `error` field. Signed errors always mean
simulation minus reality. Quality analysis does not alter the simulation.
Optional efficiency analysis is also diagnostic and does not change model
results.

## InfluxDB data contract

All source series are matched case-sensitively by bucket, measurement,
`entity_id`, field, and, where configured, version. The default source shape is:

```text
measurement: <configured measurement>
tags:        entity_id, version, scenario, pv_mode, unit
field:       value (or a processor-specific output field)
timestamp:   UTC
```

Input and output bucket names are configured; no bucket is created implicitly.
The InfluxDB token must have read access to every input bucket and write access
to every output bucket.

## Configuration

The stage is selected with `--stage` and maps to `config/<stage>.json`:

| Stage | MQTT | Processing output | Scenario output | Supplied scenario state |
| --- | --- | --- | --- | --- |
| `dev` | Disabled | `HomeAssistant_processed` | `testing` | quality and efficiency enabled |
| `prod` | Enabled | `HomeAssistant_processed` | `Scenarios` | quality and efficiency disabled |
| `test` | Requires `config/test.json` | Defined by that file | Defined by that file | Defined by that file |

The most important configuration sections are:

- `processing.input_bucket` and `processing.output_bucket`
- `processing.entities_to_process`
- `scenarios.buckets`, `setup`, `sources`, `pv_sources`, and `pv_modes`
- `scenarios.definitions.<name>.enabled`
- `scenarios.quality.enabled` and `scenarios.efficiency.enabled`
- `mqtt.enabled`, broker settings, topic prefixes, and timeouts

Keep credentials out of JSON files and Git. The application loads `.env` from
the repository root. Use `.env_example` as a template:

```dotenv
INFLUX_URL=http://influxdb:8086
INFLUX_TOKEN=replace_with_a_token
INFLUX_ORG=replace_with_an_org
MQTT_USERNAME=replace_if_required
MQTT_PASSWORD=replace_if_required
```

`MQTT_USERNAME` and `MQTT_PASSWORD` are only used when MQTT is enabled and can
be renamed through `mqtt.username_env` and `mqtt.password_env`.
`INFLUX_TIMEOUT_MS` is optional and defaults to `100000`.

When changing historical inputs or simulation assumptions, increment the
relevant processor or scenario `version`. Versions are used to find the last
completed day and prevent incompatible output series from being mixed.

## Requirements

- Python 3.10 or newer
- InfluxDB 2.x
- An InfluxDB token with the permissions described above
- Optional: an MQTT 3.1.1 broker for Home Assistant status reporting
- Linux `cron`, `flock`, `git`, `logrotate`, and `sha256sum` for the supplied
  production deployment

Python dependencies are pinned in `requirements.txt`; development and test
dependencies are in `requirements_dev.txt`.

## Local installation and execution

### Windows PowerShell

```powershell
py -m venv venv_dev
.\venv_dev\Scripts\Activate.ps1
python -m pip install -r requirements_dev.txt
Copy-Item .env_example .env
# Edit .env with real credentials.
python src/main.py --stage dev
```

### Linux

```bash
python3 -m venv venv
. venv/bin/activate
python -m pip install -r requirements.txt
cp .env_example .env
# Edit .env with real credentials.
python src/main.py --stage dev
```

The command-line interface is:

```text
python src/main.py [--stage {dev,test,prod}]
                  [--log-level {DEBUG,INFO,WARNING,ERROR}]
                  [--log-file PATH]
```

The default stage is `dev`. A production invocation writes to the buckets and
measurements configured in `config/prod.json`; review that file before running
it.

## Production deployment on Debian/Ubuntu

The repository contains a complete example deployment for a Linux host. The
following paths are the defaults used by the scripts:

```text
/opt/Python-Processing-InfluxDB
/opt/Python-Processing-InfluxDB/venv
/usr/local/sbin/python-auswertung-launcher
/var/log/Python_Auswertung/
```

If the checkout uses another path, update `REPO_DIR` and related paths in
`run_script.sh`, `deploy/launcher/python-auswertung-launcher`, and
`cron.d_influx_job`.

### One-time installation

Run from the repository checkout as root or with `sudo`:

```bash
sudo install -o root -g root -m 0755 \
  deploy/launcher/python-auswertung-launcher \
  /usr/local/sbin/python-auswertung-launcher
sudo install -o root -g root -m 0644 \
  deploy/logrotate/python-auswertung \
  /etc/logrotate.d/python-auswertung
sudo install -o root -g root -m 0644 \
  cron.d_influx_job \
  /etc/cron.d/influx_job
sudo chmod 0755 run_script.sh
```

Create the production virtual environment and install dependencies:

```bash
sudo python3 -m venv /opt/Python-Processing-InfluxDB/venv
sudo /opt/Python-Processing-InfluxDB/venv/bin/python \
  -m pip install -r /opt/Python-Processing-InfluxDB/requirements.txt
```

Place `.env` in the repository root and make it readable by the account that
executes the job. The example launcher runs as `root`, performs
`git pull --ff-only origin master` before each run, validates logrotate, and
then calls `run_script.sh`. Ensure the checkout has the required Git remote,
branch, ownership, and credentials before enabling this behavior.

### Cron schedule

`cron.d_influx_job` runs the launcher every day at 04:00 local host time:

```cron
0 4 * * * root /usr/bin/flock -n /tmp/influx_job.lock /usr/local/sbin/python-auswertung-launcher
```

Files in `/etc/cron.d` are read automatically; a cron restart is normally not
required. On Debian and Ubuntu the service is named `cron`, not `crond`:

```bash
sudo systemctl enable --now cron
sudo systemctl status cron --no-pager
sudo ls -l /etc/cron.d/influx_job
sudo cat /etc/cron.d/influx_job
```

The file must be owned by `root:root` and normally have mode `0644`. `flock`
prevents overlapping runs.

### Manual production test

Do not wait for 04:00 to validate a deployment:

```bash
sudo /usr/local/sbin/python-auswertung-launcher
sudo tail -n 100 /var/log/Python_Auswertung/runs.log
```

The launcher writes early deployment failures to `runs.log`. The application
writes detailed timestamped logs and a compact run summary. `logrotate` rotates
the summary at 10 MB and keeps ten compressed archives.

## MQTT status for Home Assistant

When enabled, the application publishes retained status values and Home
Assistant MQTT Discovery configuration. Topics are based on:

```text
<topic_prefix>/<stage>/state
<topic_prefix>/<stage>/availability
<discovery_prefix>/...
```

The default topic prefix is `python-processing`; the default Discovery prefix
is `homeassistant`. The published status includes result, run state, last
successful run, runtime, last processed date, and a sanitized diagnostic.
Results are `SUCCESS`, `SUCCESS_WITH_WARNINGS`, and `ERROR`.

The MQTT Last Will marks an unexpectedly terminated run as failed. A clean
disconnect leaves the retained result unchanged. MQTT reporting is best-effort
and must not prevent the InfluxDB processing result from being logged.

The optional dashboard definition is in `docs/home_assistant_dashboard.yaml`.

## Logging and troubleshooting

Development and test logs default to `logs/`. Production logs and summaries
default to `/var/log/Python_Auswertung/`. A run summary contains status, number
of processed days, duration, processing step, warning count, and error details.

Common checks:

```bash
# Verify credentials are present without printing their values.
test -n "$INFLUX_URL" && test -n "$INFLUX_TOKEN" && test -n "$INFLUX_ORG"

# Verify the production service and schedule.
systemctl status cron --no-pager
cat /etc/cron.d/influx_job

# Run the Python entry point directly.
cd /opt/Python-Processing-InfluxDB
/opt/Python-Processing-InfluxDB/venv/bin/python src/main.py --stage prod

# Inspect the latest summary.
tail -n 100 /var/log/Python_Auswertung/runs.log
```

An HTTP 403 normally means that the token lacks access to the requested
bucket. An HTTP 404 normally means that the bucket or organization is wrong.
Missing source data, invalid configuration, or an incomplete sample-and-hold
day is reported in the application log; no synthetic success value is written.

## Tests

Tests use fakes for InfluxDB and MQTT and do not replace a test against the
real buckets and broker:

```bash
python -m pytest
```

On Windows, use the project interpreter explicitly when needed:

```powershell
.\venv_dev\Scripts\python.exe -m pytest
```

The test suite covers processors, DST boundaries, scenario calculations,
configuration validation, logging, MQTT status, and InfluxDB handler behavior.

## Repository layout

```text
config/                 Stage-specific JSON configuration
deploy/launcher/        Production launcher
deploy/logrotate/       Production logrotate rule
docs/                   Home Assistant dashboard and reference documents
src/main.py             Application entry point
src/moduls/processing/  Heat-pump and house-consumption processors
src/moduls/szenarios/   Battery model, validation, and scenario runner
tests/                  Unit tests with service fakes
cron.d_influx_job       Cron definition for the production host
run_script.sh           Production virtualenv/dependency runner
requirements.txt        Runtime dependencies
requirements_dev.txt    Development and test dependencies
```

## Safety and operational notes

- Never commit `.env`, API tokens, MQTT passwords, or other credentials.
- Treat `config/prod.json` as a write configuration: it controls real output
  buckets and enabled simulations.
- Create a new version before reprocessing historical data with changed logic
  or assumptions.
- Test configuration and permissions with `dev` or `test` before enabling
  production.
- The battery model is an analysis tool, not a charge controller or safety
  system.
- Review source measurements and units before changing entity mappings.

## License and project status

No license file is currently included. Add an explicit license before
redistributing the repository.

The heat-pump processing, scenario engine, incremental day handling, logging,
tests, and MQTT run-status integration are implemented. Whether individual
production scenarios should be enabled remains a domain decision controlled by
the `enabled` flags in `config/prod.json`.
