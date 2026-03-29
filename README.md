# Status: Work in progress (WIP)

# Python InfluxDB Data Processing

Python tooling that reads Home Assistant metrics from InfluxDB, cleans and aggregates daily data, and writes results to a processed bucket. Runs as a scheduled task (cron) or containerized (e.g., Proxmox LXC). Designed to cope with larger datasets via day-by-day processing and explicit versioning of processed data.

## What this does (solution overview)
- **Data source**: Raw Home Assistant measurements in InfluxDB (input bucket).
- **Processing pipeline** (in `src/moduls/processing`):
	- `FixWaermepumpeStromverbrauchProcessor`: repairs heat-pump energy curves by detecting/resetting daily counters, enforcing monotonicity, and writing cleaned series to the output bucket with version tags.
	- `DailyAggregateProcessor`: sums per-entity daily consumption into per-day aggregates plus a combined entity.
	- `WaermepumpeStatistikProcessor`: splits consumption into PV vs. grid import per interval and writes interval + daily totals.
- **Orchestration**: `HomeAssistantProcessor` wires processors based on `config/<stage>.json` and the first available data day. `main.py` handles CLI, logging, env loading, and lifecycle (connect, process, disconnect).
- **Logging**: stage-aware console + file logging with rotation/cleanup (30 days) via `moduls.logger_setup`.
- **Scheduling**: `get_days_to_process` selects unprocessed days up to yesterday to keep memory bounded and to retry missed days safely.

## Features
- Retrieve raw data for previous/unprocessed days and process incrementally
- Normalize time series (timezone-aware, daily boundaries, monotonic fixes)
- Write cleaned data into a separate processed bucket with versioning
- Daily aggregation and PV/grid split statistics
- Container/cron friendly; stage-based configuration and .env secrets

## Setup

### Prerequisites

- Python 3.9+
- InfluxDB instance
- Home Assistant with InfluxDB integration

### Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Create requirements.txt

```bash
pip freeze > requirements.txt
```

## Configuration

Create a `.env` (copy `.env_example` if present) and fill in required variables, e.g.:

```
INFLUX_URL=http://influxdb:8086
INFLUX_TOKEN=your-token
INFLUX_ORG=your-org
```

Stage config lives in `config/<stage>.json` (e.g., `dev.json`, `prod.json`) and defines:
- `processing.input_bucket` / `processing.output_bucket`
- `processing.entities_to_process` with processor names, versions, entity IDs, and measurements
- `scenarios` (future use for storage simulations)

## Usage

### CLI arguments

`python -m src.main` or `python src/main.py` accepts:

- `--stage {dev,test,prod}` (default: `dev`) — selects config file `config/<stage>.json` and adjusts default log level (DEBUG for dev/test, INFO for prod).
- `--log-level {DEBUG,INFO,WARNING,ERROR}` — override the stage-based level.
- `--log-file <path>` — custom log file; defaults to `logs/<timestamp>.log`.

Example:

```bash
python src/main.py --stage prod --log-level INFO --log-file logs/app_prod.log
```

### Run tests

```bash
pytest
```

## Logging
- Console + file logging via `moduls.logger_setup`.
- Default log dir: `logs/` (dev/test) or `/var/log/Python_Auswertung` (prod). Older than 30 days are removed.

## Proxmox LXC deployment

Steps to run this on a Proxmox LXC (Debian/Ubuntu base):

1) Create the container
- Create an unprivileged LXC with enough RAM/CPU for your data volume.
- Install basics inside the container:
	```bash
	apt-get update
	apt-get install -y git python3 python3-venv cron
	```

2) Get the code under /opt by cloning the repo
- ```bash
	mkdir -p /opt
	cd /opt
	git clone https://github.com/dirkniemann/Python-Processing-InfluxDB
	cd /opt/Python-Processing-InfluxDB
	```

3) Configure environment
- Create `.env` in the repo root with your InfluxDB settings (see Configuration section).
```bash
cp .env_example .env
nano .env
```

4) Create virtualenv and install deps
- ```bash
	python3 -m venv /opt/Python-Processing-InfluxDB/venv
	source /opt/Python-Processing-InfluxDB/venv/bin/activate
	pip install -r requirements.txt
	```

5) Cron setup
- Make sure [run_script.sh](run_script.sh) is executable:
	```bash
	chmod +x /opt/Python-Processing-InfluxDB/run_script.sh
	```
- Use the provided cron snippet [cron.d_influx_job](cron.d_influx_job):
	```bash
	cp cron.d_influx_job /etc/cron.d/influx_job
	chmod 644 /etc/cron.d/influx_job
	service cron reload
	```
	It runs daily at 04:00 and uses flock to avoid overlap:
	```
	0 4 * * * root /usr/bin/flock -n /tmp/influx_job.lock /bin/bash -lc '/opt/Python-Processing-InfluxDB/run_script.sh'
	```

6) Logs
- The script writes to `/var/log/influx_job.log` via `tee`; ensure the cron user can write there (e.g., `sudo touch /var/log/influx_job.log && sudo chown $(whoami):$(whoami) /var/log/influx_job.log`).

7) Manual run/test
- ```bash
	cd /opt/Python-Processing-InfluxDB
	./run_script.sh
	```

## How it is implemented (internals)
- **Entry point**: `src/main.py` parses CLI, loads `.env`, selects stage config, sets up logging, and orchestrates connection/processing.
- **Connection layer**: `moduls.influxdb_handler.InfluxDBHandler` wraps connect/disconnect, queries, writes, and date handling (Berlin tz -> UTC). Includes helpers for last/first day detection and version lookup.
- **Processors** (configured via `entities_to_process`):
  - `FixWaermepumpeStromverbrauchProcessor`: detects daily counter resets, inserts missing resets, clamps non-monotonic drops, writes cleaned series with version tag.
  - `DailyAggregateProcessor`: pulls the latest cleaned version, sums daily values per entity and combined output, writes `daily_sum` fields.
  - `WaermepumpeStatistikProcessor`: correlates consumption with grid power, splits kWh into PV vs. grid import, writes interval and daily totals.
- **Scheduling**: `get_days_to_process` selects unprocessed days up to yesterday to keep memory usage bounded.
- **Logging**: root logger configured once; console + file handler, cleanup of old logs.

## Tests implemented
- `tests/test_main.py`: fakes Influx handler/processor to ensure CLI wiring and exit code success.
- `tests/test_influxdb_handler.py`: timezone conversion round-trip, connection using fakes, last datapoint retrieval, write path, UTC conversion in queries, and None handling when no data exists.
- `tests/test_logger_setup.py`: logger creation writes a file and cleans up old logs.
- `tests/test_homeassistant_processing.py`: processor wiring from config, config validation, required output entity enforcement, and date-based day selection.
- `tests/test_config_prod.py`: structural checks for `config/prod.json` (processing buckets, entities, scenarios base data).
- `tests/test_intentional_failures.py`: marked xfail to illustrate failure reporting (kept for CI visibility).

## Roadmap / TODO
- Publish MQTT status for each run and include logs on errors
- Add scenarios for different battery sizes
- Add more tests (integration/processing flow)
- Integrate Fronius inverter data into FENECON flow
- Cross-check processed data against Home Assistant for consistency
- Scenario plan:
	- Use `fems_gridactivepower` as grid signal
	- Use `fems_esssoc` for current battery SoC
	- Use `fems_essdischargepower` (negative for charge power)
	- Iterate each step: compute current battery energy (kWh) and add to virtual battery
	- When exporting to grid, check if `essdischargepower` is already high; compute remaining headroom and add to virtual battery
	- Per day: start with SoC 0 or carry over last day’s SoC
	- When importing from grid, draw from battery first; respect current battery constraints
	- If battery empty, draw from grid only
	- Adjust `gridactivepower` for scenarios and persist
