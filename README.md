
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

Configurae the .env. Dafür gibt es eine .env_example, die kopiert werden kann, zu .env umgenannt und mit den passenden Feldern befüllt werden müss

## Usage

```bash
python main.py
```