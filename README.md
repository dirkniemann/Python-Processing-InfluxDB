
# Python InfluxDB Data Processing

A Python script that extracts data from InfluxDB (populated via Home Assistant), processes daily records, and writes results to a new bucket. Designed to run as a containerized application in Proxmox. Supports scenarios with large memory requirements for processing extensive datasets.


## Features

- Retrieve data from InfluxDB for the previous day
- Process and transform time-series data
- Write processed data to a new InfluxDB bucket
- Container-ready for Proxmox deployment

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

## Usage

### CLI arguments

`python -m src.main` or `python src/main.py` accepts:

- `--stage {dev,test,prod}` (default: `dev`) — selects config file `config/<stage>.json` and adjusts default log level (DEBUG for dev/test, WARNING for prod).
- `--log-level {DEBUG,INFO,WARNING,ERROR}` — overrides the stage-based level.
- `--log-file <path>` — custom log file location; defaults to `logs/<timestamp>.log`.

Example:

```bash
python src/main.py --stage prod --log-level INFO --log-file logs/app_prod.log
```

### Run tests

```bash
pytest
```

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


Todo:
- MQTT versenden über Status des Skripts, log bei error mitschicken