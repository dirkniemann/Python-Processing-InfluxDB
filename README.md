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
MQTT_USERNAME=your-mqtt-username
MQTT_PASSWORD=your-mqtt-password
```

`MQTT_USERNAME` und `MQTT_PASSWORD` werden nur benötigt, wenn der konfigurierte MQTT-Broker eine Authentifizierung verlangt. Die Werte gehören ausschließlich in die lokale `.env` und niemals ins Repository.

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
- Console + detailed file logging via `moduls.logger_setup`.
- Production runs append exactly one compact status line to `/var/log/Python_Auswertung/runs.log`.
- The summary is deliberately human-readable, with date/time, status, processing result, and duration:
	```
	2026-09-27 04:00:01 | SUCCESS | 3 Tage verarbeitet | Dauer: 42 s
	2026-09-28 04:00:01 | SUCCESS | Keine neuen Tage verarbeitet | Dauer: 4 s
	2026-09-29 04:00:01 | ERROR | Fehler in data processing (RuntimeError): Verbindung zu InfluxDB fehlgeschlagen | Dauer: 8 s
	```
- Failed lines contain the processing step, exception type and a sanitized error message. Credentials and raw measurements are never written to the summary.
- The detailed timestamp-based logs remain available for diagnosis. The summary is rotated by Linux `logrotate` at 10 MB and keeps ten compressed backups.

## Installation auf einem neuen Zielsystem

Steps for a new Proxmox LXC or Debian/Ubuntu system:

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
- Install the stable launcher outside the repository and install the logrotate rule:
	```bash
	install -m 0755 /opt/Python-Processing-InfluxDB/deploy/launcher/python-auswertung-launcher /usr/local/sbin/python-auswertung-launcher
	install -D -m 0644 /opt/Python-Processing-InfluxDB/deploy/logrotate/python-auswertung /etc/logrotate.d/python-auswertung
	logrotate -d /etc/logrotate.conf
	```
- Then install the provided cron snippet [cron.d_influx_job](cron.d_influx_job):
	```bash
	cp cron.d_influx_job /etc/cron.d/influx_job
	chmod 644 /etc/cron.d/influx_job
	service cron reload
	```
	It runs daily at 04:00 and uses `flock` to avoid overlap. The launcher updates `master`, validates logrotate, and starts the current repository version:
	```
	0 4 * * * root /usr/bin/flock -n /tmp/influx_job.lock /usr/local/sbin/python-auswertung-launcher
	```

6) Logs
- The application writes detailed logs and `runs.log` below `/var/log/Python_Auswertung`; the cron user (`root` in the example) must be able to write there.

7) Manual run/test
- ```bash
	/usr/local/sbin/python-auswertung-launcher
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
- `tests/test_main.py`: fakes Influx handler/processor to verify successful runs, interrupts, and connection failures with correct summary steps.
- `tests/test_influxdb_handler.py` and `tests/test_influxdb_handler_additional.py`: timezone conversion, connection and write paths, UTC query conversion, missing data, credentials, version selection, and sorted records.
- `tests/test_logger_setup.py`: logger creation writes a file and cleans up old logs.
- `tests/test_homeassistant_processing.py`: processor wiring from config, config validation, required output entity enforcement, and date-based day selection.
- `tests/test_daily_aggregate_processor.py`: daily entity/total sums, missing source data, and pending-day processing.
- `tests/test_fix_waermepumpe_stromverbrauch_processor.py`: monotonicity repairs, zero backfill, and a realistic missing-reset day.
- `tests/test_waermepumpe_statistik_processor.py`: shared grid budget, counter resets, change-only compressor states, and complete daily PV/grid totals.
- `tests/test_config_prod.py`: structural checks for `config/prod.json` (processing buckets, entities, scenarios base data).

## Roadmap / TODO
- Batterieszenario-Handling implementieren

## Aktueller Stand und To Do

Dieser Abschnitt dokumentiert die umgesetzte v2-Auswertung und die verbleibenden fachlichen Grenzen. Read-only InfluxDB-Abfragen über `.env_agent` wurden auf kurze, gezielte Zeiträume begrenzt; die folgenden Einheiten, Punktzahlen und Zeitabstände sind Bestandsindikatoren, keine vollständige Validierung der Gerätesemantik.

### Umgesetzt

- `fix_waermepumpe_stromverbrauch` bereinigt die Tageszählerstände beider Wärmepumpen und schreibt sie als `value` in kWh.
- `Waermepumpe_statistik` verarbeitet beide bereinigten Zähler und `fems_gridactivepower` mit Version `v2`.
- Sensorrollen sind in `config/dev.json` und `config/prod.json` explizit benannt; die Reihenfolge von Listen hat keine Bedeutung.
- `fems_gridactivepower` wird als Watt interpretiert. Nur positive Werte werden als gemeinsames Netzbudget verwendet; negative Werte zählen nicht als Netzbezug.
- Das gemeinsame Netzbudget wird höchstens einmal auf beide Pumpen verteilt. Bei knappem Budget erfolgt die Verteilung proportional zur geschätzten Intervallenergie.
- Die beiden Kompressor-Binary-Sensoren werden als Change-Only-Signale fortgeschrieben. Bekannte Aktivitätsfenster und ein konfigurierbarer Nachlauf von 15 Minuten gewichten die zeitliche Zählerverteilung.
- `pv_contribution` ist weiterhin ein rechnerischer Rest nach Abzug des Netzanteils und kein direkt gemessener PV-Stromfluss.

In InfluxDB sind laut Nutzer folgende Change-Only-Sensoren vorhanden:

| Rolle | Measurement | Entity | Feld / Werte |
| --- | --- | --- | --- |
| Kompressor 1 | `binary_sensor.e3_vitocal_kompressor` | `e3_vitocal_kompressor` | `value`: 0 = aus, 1 = an |
| Kompressor 2 | `binary_sensor.e3_vitocal_kompressor_2` | `e3_vitocal_kompressor_2` | `value`: 0 = aus, 1 = an |

Measurement-Namen sind in InfluxDB case-sensitive. Vor einer Änderung gegen die tatsächliche Schreibweise im Bucket prüfen. Die Sensoren schreiben nur Zustandsänderungen; ein Zustand gilt bis zum nächsten Ereignis. Ein erster Wert `1` innerhalb eines Abfragefensters beweist nicht, dass der Kompressor genau zu diesem Zeitpunkt eingeschaltet wurde.

### InfluxDB-Bestandsprüfung

Am 2026-09-26 wurden read-only Abfragen mit dem ausdrücklich dafür vorgesehenen `.env_agent`-Zugang ausgeführt. Die normale `.env` wurde nicht gelesen oder verwendet. Auf Nutzerwunsch beschränkt sich die erste Inventur auf **14 Tage** und gibt nur Metadaten/Punktzahlen aus. Zeitprofile und Wertzusammenfassungen wurden anschließend gezielt auf relevante Entities und sieben Tage begrenzt; eine einzelne Plausibilitätsprobe verwendete einen Tag mit Stundenmitteln sowie einen einminütigen Snapshot.

Die Home-Assistant-Reihen verwenden hier überwiegend die **Einheit als Measurement** und nicht als Unit-Tag: Leistungsreihen liegen im Measurement `W`, die Wärmepumpenzähler im Measurement `kWh`. Das Influx-Tag `unit` war bei diesen Reihen leer. Das SOC-Measurement wurde wörtlich als `„%“` ausgegeben und sollte vor Konfigurationsfestlegung noch auf exakte Schreibweise geprüft werden.

| Signal | Measurement | Entity | Field | Beobachtung im 14-Tage-Fenster |
| --- | --- | --- | --- | ---: |
| Hausverbrauchsleistung (bekannt fehlerhaft, nicht als Bilanzquelle verwenden) | `W` | `elektrischer_verbrauch` | `value` | 129.5k Punkte |
| Netzleistung | `W` | `fems_gridactivepower` | `value` | 119.4k Punkte |
| PV-Leistung | `W` | `fems_productiondcactualpower` | `value` | 70.7k Punkte |
| PV-Leistung, neue Anlage | `W` | `mt_stall_neu_leistung_ac_fixed` | `value` | 10.0k Punkte |
| PV-Leistung, alte Anlage 1 | `W` | `mt_stall_alt_1_leistung_ac_fixed` | `value` | 10.0k Punkte |
| PV-Leistung, alte Anlage 2 | `W` | `mt_stall_alt_2_leistung_ac_fixed` | `value` | 10.2k Punkte |
| Batterie-Leistung (bidirektional) | `W` | `fems_essdischargepower` | `value` | 84.0k Punkte |
| Batterie-SOC | `„%“` | `fems_esssoc` | `value` | 2.7k Punkte |
| Wärmepumpe 1 Tageszähler | `kWh` | `e3_vitocal_heizung_stromverbrauch_heute` | `value` | 895 Punkte |
| Wärmepumpe 2 Tageszähler | `kWh` | `e3_vitocal_heizung_stromverbrauch_heute_2` | `value` | 1.14k Punkte |
| Kompressor 1 | `binary_sensor.e3_vitocal_kompressor` | `e3_vitocal_kompressor` | `value`, zusätzlich `state` | je 176 Punkte |
| Kompressor 2 | `binary_sensor.e3_vitocal_kompressor_2` | `e3_vitocal_kompressor_2` | `value`, zusätzlich `state` | je 243 Punkte |

Damit sind die in der Nutzerangabe genannten Kompressoren vorhanden, aber die tatsächliche zweite Measurement-Schreibweise ist **kleingeschrieben** `binary_sensor.e3_vitocal_kompressor_2` (nicht `Binary_Sensor_E3_Vitocal_Kompressor_2`). Für die numerische 0/1-Zeitreihe ist `value` vorhanden; `state` ist ebenfalls gespeichert, wurde aber nicht als Zahl ausgewertet. Die `value`-Felder beider Kompressoren enthalten im betrachteten Fenster beide Zustände 0 und 1.

Weitere gefundene passende Reihen waren drei `climate.e3_vitocal_heizung*`-Entities (Field `state`), ein Außentemperatursensor in `°C` sowie zwei Gas-Tageszähler in `kWh`. Im kurzen Inventar wurde kein separat benannter Batterie-Ladesensor, kein eigener Einspeisezähler und kein weiterer expliziter Netzbezugssensor gefunden. `fems_gridactivepower` ist vorzeichenbehaftet und kann vermutlich Bezug/Einspeisung abbilden; diese Annahme sollte gegen Geräte-/Entity-Metadaten bestätigt werden.

#### Zeitabstände und Lückenindikatoren

Die Median-/Maximalabstände unten wurden aus Ereigniszeitstempeln über sieben Tage berechnet. Maximalabstand bedeutet zunächst nur „Abstand zwischen gespeicherten Punkten“; bei Change-Only-Reihen ist er keine automatische Datenlücke.

| Signal | Medianabstand | Größter Abstand | Einordnung |
| --- | ---: | ---: | --- |
| `elektrischer_verbrauch` | 10 s | 30 min 40 s | gewöhnlich häufig, einzelne lange Stille auffällig |
| `fems_gridactivepower` | 10 s | 30 min 40 s | gewöhnlich häufig, lange Lücke für Zuordnung ungültig behandeln, bis Semantik geprüft ist |
| `fems_productiondcactualpower` | 10 s | 2 h 48 min 13 s | Eventabstand; Nutzer bestätigt Change-only-Verhalten/Null bei Nacht, Zustand fortschreiben |
| `fems_essdischargepower` | 5 s | 13 h 28 min 13 s | Eventabstand; Nutzer bestätigt unveränderte Leistung (u. a. 0 W) bleibt bis Änderung gültig |
| `fems_esssoc` | 2 min | 14 h 23 min 35 s | Eventabstand; unveränderter SOC bleibt gültig; vor Fensterbeginn letzten SOC laden |
| WP1-Zähler | 6 min 29 s | 15 h 31 min 2 s | Zählerdifferenz über solche langen Intervalle zeitlich nicht präzise aufteilbar |
| WP2-Zähler | 6 min 30 s | 14 h 18 min 1 s | wie WP1 |
| Kompressor 1 Change-Only | 39 min | 18 h 9 min 2 s | lange Abstände sind bei unverändertem Zustand plausibel |
| Kompressor 2 Change-Only | 45 min 29 s | 22 h 38 min 32 s | lange Abstände sind bei unverändertem Zustand plausibel |

Vor dem 7-Tage-Profil lag der letzte Kompressor-1-Wert `0` 13.07 Stunden und der letzte Kompressor-2-Wert `1` 8.82 Stunden zurück. Die Zustände müssen daher über den Profilbeginn hinweg fortgeschrieben werden. Bei diesen ausdrücklich Change-Only-Sensoren darf ein langer Abstand allein den Zustand **nicht** auf `unknown` setzen; `unknown` gilt vor dem ersten bekannten Zustand bzw. bei nachgewiesenem Recorder-/Retention-Verlust.

#### Vorzeichen und Plausibilitätscheck

Im Stundenmittel des 25.09.2026 (UTC) war `fems_gridactivepower` nachts positiv (z. B. etwa +5.8 kW bei rund 36 W PV) und bei hoher PV-Leistung negativ (z. B. etwa −17.3 kW bei rund 20.6 kW PV). Das stützt die Konvention **positiv = Netzbezug, negativ = Einspeisung**. Die Extremwerte der letzten sieben Tage waren −25.501 kW bis +13.864 kW.

`fems_essdischargepower` lag zwischen −17.041 kW und +15.108 kW. Am selben Tag stieg der SOC bei negativer Batterieleistung von etwa 12.5 % auf 96.6 % und fiel bei positiver Leistung wieder bis etwa 14 %. Das stützt **negativ = Laden, positiv = Entladen**. Ein separater Ladesensor ist im kurzen Kandidateninventar nicht sichtbar. Laut ergänzender Nutzerinformation ist `elektrischer_verbrauch` bereits an der Quelle fehlerhaft und wird teilweise negativ. Diese Reihe darf daher nicht als Eingangsgröße für die Lastbilanz oder Wärmepumpenzuordnung dienen; sie kann allenfalls als Diagnose-/Vergleichssignal erhalten bleiben.

Der einminütige Snapshot um 10:00 UTC zeigte innerhalb derselben Minute: das inzwischen als fehlerhaft bekannte Signal `elektrischer_verbrauch` `12.349 kW`, Grid `−14.156 kW`, PV `21.730 kW`, Batterieleistung `−3.584 kW` (Batteriemessung lag rund 31 s früher) und SOC `56 %`. Wegen des Quellfehlers ist der Verbrauchswert für die Bilanz nicht belastbar; außerdem schließt die einfache Bilanz mit diesen nicht exakt synchronisierten Signalen nicht. Die kurze Influx-Inventur bestätigt jetzt auch alle drei `mt_stall_*_leistung_ac_fixed`-Entities im Measurement `W`. Laut Nutzerinformation speisen die beiden `mt_stall_alt_*`-Anlagen derzeit vollständig ein und laden die Batterie nicht. Vor Batterie-Simulation und PV-/Netz-Zuordnung müssen daher die Systemgrenze, DC-/AC-Bezug von `fems_productiondcactualpower`, Zeitsynchronität und das Verhalten der alten PV-Anlagen ausdrücklich modelliert werden.

Für eine spätere Energiebilanz muss zwischen **gesamter Erzeugung** und **lokal nutzbarer Erzeugung** unterschieden werden. Sofern alle Signale dieselbe AC-Systemgrenze abdecken und zeitlich ausgerichtet sind, lautet die zu prüfende Brutto-Lastbilanz `Hauslast = vorzeichenbehaftete Netzleistung + vorzeichenbehaftete Batterieleistung + gesamte PV-Erzeugung hinter dem Netz-Zähler`. Vorläufig gilt `Grid > 0` = Bezug, `Grid < 0` = Einspeisung sowie Batterie `> 0` = Entladung, `< 0` = Laden. Die alten PV-Anlagen gehören gegebenenfalls zur Gesamtproduktion für diese Bilanz, dürfen laut aktuellem Anlagenbetrieb aber **nicht** als lokal verfügbare PV oder Batterie-Ladequelle für Szenarien gezählt werden; ihre gemessene Leistung ist als feste Einspeisung zu behandeln. Ob diese Abgrenzung und Bilanzgleichung für die konkreten Messpunkte gilt, muss anhand der Anlagenmetadaten bestätigt werden. PV-Leistungen dürfen weder blind addiert noch doppelt gezählt werden.

Die Messwerte (`W`, `kWh`, SOC und Binärzustände) sind Influx-Eingaben. Ein aus Zählerständen abgeleiteter Intervallverbrauch und seine Netz-/PV-Aufteilung bleiben abgeleitete bzw. geschätzte Werte. Das gilt besonders, wenn ein Zähler bis zu 15 Stunden zwischen Punkten und ein Kompressor bis zu 23 Stunden zwischen Zustandswechseln hat.

**Für die v2-WP-Auswertung festgelegt:** (1) `fems_gridactivepower` wird in W verarbeitet, positiv bedeutet Netzbezug; (2) Grid-Werte werden bis zum nächsten Messpunkt fortgeschrieben; (3) die WP-Zuordnung verwendet ausschließlich Zähler, Grid und Kompressoren, keine PV- oder Batteriesignale; (4) Change-Only-Kompressoren werden über bekannte Ereignisse fortgeschrieben, mit 15 Minuten konfigurierbarem Nachlauf; (5) bei fehlender Aktivität wird die Zählerenergie zeitproportional verteilt und nicht verworfen. `elektrischer_verbrauch` ist laut Nutzer an der Quelle fehlerhaft und wird nicht zur Last- oder WP-Berechnung verwendet. Die physikalische Systemgrenze der PV-/Batteriesignale bleibt für die spätere Batteriesimulation offen.

Die read-only Hilfsabfrage und ihre gestufte Bedienung sind unter [`tools/influx_audit`](tools/influx_audit/README.md) beschrieben. Das Skript liest ausschließlich `INFLUX_URL`, `INFLUX_TOKEN` und `INFLUX_ORG` aus `.env_agent`, gibt keine Credential-Werte aus und speichert keine Rohmessdaten.

Die folgenden Punkte sind umgesetzt:

- Die Wärmepumpen-Auswertung verarbeitet beide Zähler mit expliziten Sensorrollen, Tagesgrenzen und Reset-Behandlung.
- Das gemeinsame Netzbudget wird einmalig, gedeckelt und bei mehreren Pumpen proportional verteilt.
- Change-Only-Kompressorsignale werden mit konfigurierbarem Nachlauf fortgeschrieben.
- Datenbasis, Zeitzonen, Vorzeichenannahmen und relevante InfluxDB-Sensoren sind dokumentiert.
- Die Verarbeitung ist versioniert und nutzt für Tests das separate `testing`-Bucket aus `config/dev.json`.
- Logging, Laufzusammenfassung, Fehlerstatus, Logrotation und der externe Launcher sind umgesetzt und getestet.
- Die PR-CI führt die vollständige Testsuite mit mindestens 70 % Coverage aus.

## Batterieszenario-Simulation – TODO

**Status dieser Änderung: technische V1-Implementierung mit inkrementellem Betrieb.** Die korrigierte Hauslast, die konfigurationsgetriebene Batterie-Engine, der restartfähige Szenario-Runner, Qualitätsmarker, Realbatterie-Diagnostik und die Timeseries-/Daily-Measurements sind implementiert und durch automatisierte Tests abgesichert. Es wurden keine produktiven Daten oder bestehende Wärmepumpenlogik geändert. MQTT-Status, Grafana und Wirtschaftlichkeitsberechnung bleiben ausdrücklich offen; empirische Effizienzwerte werden nur diagnostisch berichtet und nicht automatisch in V1 verwendet.

### Bestandsmodell und Projektanschlüsse

- Python-Einstieg ist `src/main.py`. Er lädt `config/<stage>.json`, verbindet zu InfluxDB und startet `HomeAssistantProcessor`.
- `src/moduls/processing/HomeAssistant_processing.py` registriert den korrigierten Hausverbrauch; `src/main.py` startet den Szenario-Runner erst danach. Szenario-IDs und PV-Modi werden ausschließlich aus JSON geladen.
- `src/moduls/influxdb_handler.py` stellt zusätzlich zu den bestehenden APIs einen chronologischen Latest-Query, einen Mehrfeld-Writer und pivotierte Daily-Abfragen für Neustart-/Rebuild-Entscheidungen bereit. Naive Zeitstempel werden als Europe/Berlin interpretiert und nach UTC umgerechnet.
- Der Szenario-Runner ermittelt pro Szenario/PV-Modus den verfügbaren vollständigen Datenhorizont, setzt nach dem letzten vollständigen Daily-Punkt fort und rekonstruiert den SOC aus `stored_energy_end_kwh`. Historische Input-Fingerprint-Änderungen oder Konfigurations-/Modelländerungen erzeugen eine neue `run_version` und rechnen konsistent ab dem Anfang neu.
- `config/{dev,prod}.json` enthält `22.4 kWh` Base-DC-Kapazität, `17.92 kW` Base-Lade-/Entladeleistung, SOC 5–100 %, Initial-SOC 5 %, explizite Change-only-Quellen und optionale Realbatterie-Validierungsquellen. Das Szenarioziel ist `testing` in dev und `Szenario` in prod.
- Die Tests decken Engine, mehrere Tage, Neustart, inkrementelles Fortsetzen, historische Rebuilds, unvollständige Eingaben, DST (24/23/25 Stunden), Szenariokonfiguration und Realbatterie-Diagnostik ab.

### Eigenes Processing-Ergebnis: korrigierter Hausverbrauch

Der korrigierte Hausverbrauch soll vor der Batteriesimulation als eigenständige versionierte Zeitreihe im Processing-Zielbucket entstehen: `HomeAssistant_processed` in prod und `testing` in dev.

```text
Hausverbrauch_korrigiert [W]
  = elektrischer_verbrauch [W]
  + mt_stall_neu_leistung_ac_fixed [W]
```

`mt_stall_alt_1/2` gehören ausdrücklich **nicht** in diese Korrektur. Die Alt-PV liegt messtechnisch außerhalb des hierfür relevanten FEMS-Pfads; sie bleibt ausschließlich eine getrennte PV-/Betriebsmodus-Eingangsgröße der späteren Szenarien. Diese Messgrenzenfestlegung stammt aus der Nutzerangabe und passt zum getrennten Entity-Bestand. Die read-only Stichprobe vom 2026-09-25 erfasste FEMS-Verbrauch und `MT-Stall neu` separat; sie stützt, dass die neue Anlage als eigener Zusatzfluss vorhanden ist, ersetzt aber keinen Mehrzeitraum-Bilanznachweis.

Die bestehende Struktur kann das aufnehmen, benötigt aber einen neuen konfigurierten Prozessor. `HomeAssistantProcessor` erkennt heute nur feste Schlüssel (`fix_waermepumpe_stromverbrauch`, `daily_aggregate`, `Waermepumpe_statistik`); ein beliebiger neuer JSON-Schlüssel wird noch nicht automatisch ausgeführt. Empfohlener Eintrag in `processing.entities_to_process` in dev/prod:

```json
"corrected_house_consumption": {
  "version": "v1",
  "output_measurement": "Hausverbrauch_korrigiert",
  "output_entity_id": "Hausverbrauch_korrigiert",
  "sensors": {
    "fems_house_consumption": {
      "measurement": "W",
      "entity_id": "elektrischer_verbrauch",
      "field": "value",
      "unit": "W"
    },
    "mt_stall_neu_power": {
      "measurement": "W",
      "entity_id": "mt_stall_neu_leistung_ac_fixed",
      "field": "value",
      "unit": "W"
    }
  }
}
```

Ein dedizierter `CorrectedHouseConsumptionProcessor` kann in die vorhandene Prozessorliste registriert werden und `get_data`/`write_datapoint` wiederverwenden. Er schreibt Feld `value`, Einheit `W`, Output-Measurement und Entity wie oben sowie Version und Berliner Tageszeitstempel/UTC-Instant. Die Config beschreibt ausschließlich die beiden addierten Quellen; keine Alt-PV und keine Batterieleistung.

#### Zeitliche Zuordnung

Keine zeilenweise Join-Addition nur bei identischem Timestamp. Die fachlich passende Erststrategie ist **Zero-Order-Hold (letzten bekannten Wert halten)** für diese Leistungszustände, nicht lineare Interpolation: `MT-Stall neu` ist Change-only; bei `elektrischer_verbrauch` zeigen die Daten einen Medianabstand um 10 s und Eventwerte, daher den letzten Wert bis zum nächsten Messereignis halten, solange keine Geräteinformation eine Intervallmittel-Semantik nachweist. Vorläufige Implementierungsannahme explizit als Sample-and-Hold dokumentieren.

Pro lokalem Tag eine sortierte Vereinigung der Zeitstempel beider Signale plus Tagesanfang/-ende bilden. Vor der ersten Änderung des Tages je Quelle den chronologisch letzten Punkt **vor** Tagesbeginn laden. Auf jedem Teilintervall gilt der letzte FEMS-Wert plus der letzte MT-Neu-Wert; bei einem Event einer der Quellen den korrigierten Summenwert neu schreiben. Damit bleiben Änderung und Mitternachtsübergang erhalten, ohne erfundene Zwischenwerte. Für kWh-Leistungsintegrale den Summenwert über die jeweilige tatsächliche Intervalllänge integrieren; Tagesfenster wegen DST in Europe/Berlin schneiden, Zeitpunkte intern UTC.

**Bestandsverhalten von `InfluxDBHandler.get_last_datapoint()`:** Der Name ist missverständlich: Die Funktion filtert einen Zeitraum und nutzt Flux `max()` auf `_value`; sie liefert also den numerisch größten Wert im Zeitfenster, nicht das chronologisch letzte Event. Im aktuellen Quellcode wird sie nur von `DailyAggregateProcessor` auf dem reparierten Tages-Energiezähler (`fix_waermepumpe_stromverbrauch`, Feld `value`) verwendet. Dort ist das Maximum wegen Counter-/Tagesreset ausdrücklich die gewünschte Referenz. Die bestehende Semantik bleibt unverändert. Für Change-only-Leistungs-/Statussignale, deren letzter Zustand vor Tagesbeginn benötigt wird, ist eine getrennte Funktion wie `get_latest_datapoint_by_time()` mit exakter Serienfilterung und chronologischer `last()`-Semantik vorgesehen.

### InfluxDB-Datenbasis (read-only geprüft)

Der gezielte Audit vom 2026-09-27 nutzte ausschließlich das dafür vorgesehene `.env_agent` und las keine Rohdaten in Dateien. Die Inventur deckte 14 Tage ab, Profil und Min/Mean/Max-Zusammenfassungen sieben Tage. Die vorherige README-Bestandsaufnahme vom 2026-09-26 bleibt für die zusätzlichen Einzelbeobachtungen maßgeblich. Punktzahlen und Min/Mean/Max sind Bestandsindikatoren, keine vollständige Datenqualitäts- oder Gerätespezifikationsprüfung; der Mittelwert unten ist ein Ereignismittel, kein zeitgewichteter Leistungs-Mittelwert.

| Rolle | Influx Measurement / Entity / Field | Beobachtung und Status |
| --- | --- | --- |
| PV FEMS | `W` / `fems_productiondcactualpower` / `value` | Einheit `W`; Medianabstand 10 s, größter beobachteter Ereignisabstand im 7-Tage-Profil 2:48:13; Wertebereich 27 bis 26.547 W. Entity nennt DC-Leistung; für eine AC-Bilanz ist Umrechnung/Systemgrenze zu bestätigen. Laut Nutzer kennt FEMS seine eigene PV. |
| PV neu | `W` / `mt_stall_neu_leistung_ac_fixed` / `value` | Einheit `W`; Medianabstand 59 s, größter Abstand 12:30:05; Wertebereich 0 bis 12.476 W. AC-Semantik im Entity-Namen, Zähler-/Wechselrichtergrenze und Verknüpfung zu FEMS noch bestätigen. |
| PV alt, Wechselrichter 1 | `W` / `mt_stall_alt_1_leistung_ac_fixed` / `value` | Einheit `W`; Medianabstand 60 s, größter Abstand 12:20:00; Wertebereich 0 bis 10.792 W. Laut Nutzer derzeit vollständig eingespeist. |
| PV alt, Wechselrichter 2 | `W` / `mt_stall_alt_2_leistung_ac_fixed` / `value` | Einheit `W`; Medianabstand 60 s, größter Abstand 12:13:00; Wertebereich 0 bis 12.146 W. Laut Nutzer derzeit vollständig eingespeist. |
| Netzleistung | `W` / `fems_gridactivepower` / `value` | Einheit `W`; Medianabstand 10 s, größter Abstand 30:40; Bereich −25.501 bis +13.864 W. Stundenmittel am 2026-09-25 stützen vorläufig positiv = Bezug und negativ = Einspeisung. Geräte-/Zählerdefinition noch bestätigen. |
| Batterie-Leistung | `W` / `fems_essdischargepower` / `value` | Laut Nutzer DC-seitig und bidirektional; Medianabstand 5 s, größter beobachteter Ereignisabstand 13:28:13; Werte −17.041 bis +15.108 W. SOC-Verlauf stützt negativ = Laden, positiv = Entladen. Kein separates Ladeleistungssignal gefunden. |
| Batterie-SOC | `„%“` / `fems_esssoc` / `value` | Measurement-Schreibweise ist wörtlich ungewöhnlich und sollte vor Query-Konfiguration exakt übernommen werden. Medianabstand 2 min, größter Ereignisabstand 14:23:35; Wertebereich 4 bis 100. Nutzer legt Simulationsgrenzen 5–100 % fest; reale SOC-Einheit/Genauigkeit bleibt gegen Entity-Metadaten zu bestätigen. |
| FEMS-Hausverbrauch | `W` / `elektrischer_verbrauch` / `value` | Laut Nutzer durch FEMS-Messgrenze fehlerhaft: FEMS kennt `MT-Stall neu` nicht; wenn deren Einspeisung den Netzfluss senkt, kann der berechnete Verbrauch zu klein oder negativ werden. Nicht unverändert als Simulationslast nutzen. |

#### Change-only-Signale und Zeitfortschreibung

Die jüngste 7-Tage-Auswertung liefert Abstände **zwischen gespeicherten Ereignissen**, nicht den Beweis einer Messlücke. Der Nutzer bestätigt als Anlagenverhalten, dass Batterie-Leistung und SOC bei unverändertem Wert sowie PV-Leistung bei dauerhaft 0 W teils keine neuen Punkte schreiben. Das gilt auch für die PV-Sensoren `MT-Stall neu` und `MT-Stall alt`. Insbesondere ist eine nächtliche Nullleistung ohne neue Punkte normal. Daher sind die beobachteten maximalen Abstände bis 14:23:35 nicht pauschal Datenqualitätsfehler.

Für bestätigte Change-only-Reihen gilt: den letzten bekannten Zustand bis zum nächsten Änderungsereignis fortschreiben, auch über Mitternacht/Tagesgrenzen; vor Beginn eines Abfragefensters den letzten Punkt davor als Startzustand laden. Batterie-Leistung 0 W und SOC bleiben bis zur Änderung gültig; PV 0 W bleibt nachts gültig. Ein fehlender neuer Punkt ist weder automatisch Null noch ungültig. Ein echter Recorder-/Kommunikationsverlust muss über unabhängige Verfügbarkeitsinformation oder anderweitige Qualitätsindikatoren erkannt werden; ein frei gewähltes maximales Eventintervall darf gültige Change-only-Werte nicht verwerfen. Bei später nachgewiesenem Ausfall muss die betroffene Zeitspanne allerdings als unbekannt markiert werden. **Datenbasis:** Eventabstände im 7-Tage-Profil plus Nutzerbeschreibung der Sensoren. **Annahme:** aufgeführte Leistungs-/SOC-Reihen speichern Änderungen, während der Zustand bis zum nächsten Event gilt. **Auswirkung:** Nullwerte und SOC können ohne ständige Wiederholungsmessung zeitlich fortgeschrieben werden; initialer Vorzustand und echte Ausfälle sind getrennt zu behandeln.

`fems_gridactivepower` hat im gemessenen 7-Tage-Profil einen Medianabstand von 10 s und einen maximalen Ereignisabstand von 30:40; seine genaue Schreibsemantik und Ausfallindikatoren sind gesondert zu bestätigen. Die Influx-Integration speichert die Einheit überwiegend als Measurement; das `unit`-Tag war leer. Die alte PV wird heute laut Nutzer vollständig verkauft/eingespeist.

**Vorzeichen:** Beobachtete Muster stützen `Grid > 0` = Import, `Grid < 0` = Export sowie Batterie-DC `P > 0` = Entladung aus dem Speicher und `P < 0` = Laden. Nutzerangabe: FEMS kennt die eigene PV, Batterie und Grid, aber **nicht** `MT-Stall neu`. `MT-Stall alt 1/2` liegt außerhalb des für die FEMS-Hauslastkorrektur relevanten Pfads und wird dort ausdrücklich nicht addiert.

#### FEMS-Hausverbrauch korrigieren: Bilanz und verbleibender Nachweis

Die Korrekturformel und die für sie geltende Messgrenze sind nun **fachlich festgelegt**:

```text
Hauslast_korrigiert = FEMS_Hausverbrauch + MT_Stall_neu_AC-Leistung
```

Laut bestätigter Anlagen-/Messgrenzenbeschreibung saldiert FEMS seine eigenen Flüsse, sieht `MT-Stall neu` nicht, und genau deren AC-Leistung ist der fehlende Beitrag. Deshalb wird `MT-Stall neu` addiert; `MT-Stall alt 1/2` werden **nicht** addiert und beeinflussen diesen Korrekturpfad nicht. Das ist die festgelegte Modellgrundlage, kein offener Architekturentscheid. Mehrzeitraum-Bilanzprüfungen bleiben sinnvoll als Qualitätsprüfung des neuen Prozessors, ändern die fachliche Zuordnung aber nicht ohne gegenteilige Messung.

Die unabhängige physikalische AC-Gesamtbilanz ist eine **separate Validierung** der festgelegten Korrekturformel und darf nicht mit ihr vermischt werden:

```text
Hauslast_AC = FEMS_PV_AC + MT_Stall_neu_AC + Grid_AC + Batterie_AC
```

Dabei ist Grid positiv bei Bezug, Batterie-Entladung positiv. Die verfügbaren FEMS-PV- und Batterie-Leistungsreihen sind laut Entity/Nutzer DC-seitig; für diese AC-Bilanz sind daher DC→AC-Umwandlung und Verluste einzubeziehen. Nicht einfach rohe DC-Werte mit AC-Grid/PV addieren. Die Formel für den korrigierten Hausverbrauch ist fachlich festgelegt und wird durch diese separate Bilanz nicht ersetzt. Für eine belastbare Plausibilisierung müssen FEMS-interne Berechnung und die AC/DC-Seiten trotzdem nachvollzogen werden; der bisherige Snapshot kann das wegen unterschiedlicher Zeitstempel und DC/AC-Mischung nicht leisten.

Eine read-only Stichprobe um 2026-09-25 10:00 UTC fand `elektrischer_verbrauch` 12.349 W um 10:00:57 und `mt_stall_neu_leistung_ac_fixed` 8.349 W um 10:00:27; die korrigierte Last läge bei rund 20.698 W. Die Zeitstempel liegen bis zu 31 s auseinander. Die Stichprobe bestätigt die getrennten Reihen und zeigt, warum timestamp-genaues Zeilen-Joining falsch wäre; sie ist keine unabhängige Lastmessung. Die Werte `FEMS-PV 21.730 W` und `Grid −14.156 W` lagen ungefähr um 10:00:57, Batterie-DC war `−3.584 W` um 10:00:26. Die rohe Summe FEMS-PV + Grid + Batterie-DC ergäbe rund 3.990 W und ist wegen DC/AC-Mischung kein zulässiger Gegenbeweis zur FEMS-Lastkorrektur. Die korrigierte Zeitreihe ist daher als eigenes, versioniertes Processing-Ergebnis zu schreiben und auf nichtnegative/plausible Verläufe zu prüfen.

**Datenbasis:** separate Influx-Reihen für FEMS-Hausverbrauch und MT-Neu-Leistung, ein einminütiges read-only Snapshot und Nutzerbestätigung der Pfadgrenze. **Festlegung:** `FEMS-Hausverbrauch + MT-Stall-neu-AC`; Alt-PV nicht addieren. **Auswirkung:** die Korrektur kann als eigener Processor geplant werden; zeitliches Alignment und Plausibilitätschecks müssen darin umgesetzt werden.

**Datenbasis:** jüngste read-only Inventur, dokumentierte 7-Tage-Ereignisprofile, Vorzeichenprobe und Nutzerklärung der Change-only-Signale. **Annahme:** Power-Werte beschreiben Momentanleistung in W, mit den oben genannten Vorzeichen; bestätigte Change-only-Werte gelten bis zum nächsten Event. **Auswirkung:** Lange Eventabstände sind hier grundsätzlich erwartbar, dennoch sind Hauslastkorrektur, Messpunktgrenzen, DC/AC-Pfade und Zeitsynchronität vor belastbaren Simulationsergebnissen zu verifizieren.

### Fachliche Empfehlung: Referenz, Basis und Erweiterung

Reale und simulierte Batterie sind getrennte Vergleichsrollen:

1. **Reale Batterie:** historische Messgrundlage und Validierung. Gemessene DC-Leistung und SOC helfen Vorzeichen, nutzbare Kapazität, Verluste und Reglerverhalten zu untersuchen. Aus DC-Batterieleistung plus SOC-Änderung kann eine effektive DC-seitige Energiebeziehung abgeschätzt werden; vollständige AC↔DC-/Round-trip-Verluste benötigen zusätzlich passende AC-Energie am Lade-/Entladepfad. Keine Wirkungsgrade erfinden. Die gemessene Leistung ist kein Ladeprofil für hypothetische Szenarien.
2. **Simulierte aktuelle Batterie (`current_battery`):** Vergleichsbasis mit 22,4 kWh gesamter DC-Kapazität, 21,28 kWh zwischen 5–100 % SOC nutzbar und symmetrisch 17,92 kW DC Base Lade-/Entladeleistung. Gleiche Eingangszeitreihe, Verlustannahme und Strategie wie Erweiterungen. Szenariodifferenzen können damit bereits in V1 relativ ausgewertet werden; parallel wird die reale Batterie als unabhängige Plausibilitätsreferenz berichtet, ohne eine scheinpräzise Übereinstimmung zu verlangen.
3. **Erweiterungsszenarien:** dieselbe Engine und derselbe Last-/PV-/Netz-Zeitverlauf; nur deklarierte Parameter und Betriebsmodus ändern sich. Szenarien sollen aus einer iterierbaren Konfigurationsliste geladen werden. Es darf keine Szenarionamen-/Anzahl-Verzweigung im Prozessor geben.

Damit wird die Nutzeridee fachlich umgesetzt: Simulationen unter identischen Bedingungen fair vergleichen und reale Werte als unabhängige Referenz führen. Eine Abweichung ist erwartbar, falls Regelstrategie, DC/AC-Wandlung, Verluste oder externe Steuerung nicht exakt bekannt sind. Ein späteres Akzeptanzkriterium kann nach V1 mit einem geeigneten Referenzzeitraum und einer realistischen Modellunsicherheit festgelegt werden.

#### Empirische Wirkungsgrad-/Verlustprüfung

Vorhandene Größen im Rohbucket: bidirektionale Batterie-DC-Leistung `fems_essdischargepower` in W, SOC `fems_esssoc` in Prozent; ergänzend FEMS-PV (als DC bezeichnet), Grid-Leistung (AC, vorzeichenbehaftet), MT-Neu-PV (AC) und der hier korrigierte FEMS-Hausverbrauch. Die reale Batterie liefert somit Daten, um **effektive richtungsabhängige DC-seitige Werte** zu schätzen: DC-Eingangsenergie bei Laden gegen SOC-Energiezunahme, und SOC-Energieabnahme gegen DC-Ausgangsenergie bei Entladen. Ansatz bei bestätigter linearer SOC-Energiebeziehung: `ΔE_SOC = 22.4 kWh × ΔSOC / 100`; die Leistung wird als Change-Event-Stufenfunktion zeitintegriert.

Eine gezielte read-only 14-Tage-Diagnose integrierte Batterie-DC-Leistung zwischen SOC-Ereignissen und verglich SOC-Paare bis 30 Minuten Abstand, bei höchstens 5 % Energie in Gegenrichtung. Unter der Annahme 22,4 kWh linearer SOC ergab sich für Laden: 255.84 kWh DC-Eingang zu 288.96 kWh SOC-Energiezunahme, Verhältnis **112.94 %**. Als Gegenprüfung über zusammenhängende reine Ladesitzungen von mindestens 30 Minuten und 1 kWh SOC-Änderung ergaben 27 Sitzungen **113.4 %** gepoolt (Einzelsitzungen 104.8–118.8 %). Für Entladen ergaben die kurzen SOC-Intervalle 281.34 kWh SOC-Abnahme zu 224.70 kWh DC-Ausgang (**79.87 %**); 18 längere Sitzungen ergaben **81.1 %** gepoolt (75.1–86.4 %). Ein Ladeverhältnis über 100 % ist physikalisch kein Wirkungsgrad. Diese Zahlen sind **diagnostische Residuen, keine gültigen Effizienzwerte**; längere Sitzungen lösen den Widerspruch nicht. SOC-Auflösung/Kalibrierung, Power-Sensorgrenze/Skalierung, Kapazitätsbezug und eventuelle Nichtlinearität der SOC-Energiebeziehung müssen erst geklärt werden. Die grobe Kombination der beiden Richtungsquotienten zu einem einzelnen Round-trip-Wert würde den Widerspruch verdecken und ist deshalb ebenfalls nicht als Wirkungsgrad zu übernehmen.

Mit bestehenden Daten sind getrennte Lade-/Entlade-**DC-seitige** Schätzungen prinzipiell möglich, aber die 14-Tage-Probe ist dafür noch nicht plausibel genug. Alle 2,792 SOC-Punkte der untersuchten 14 Tage waren ganzzahlig; ein 1-%-Schritt entspricht 0.224 kWh bei 22.4 kWh und macht kurze Zyklen empfindlich gegenüber Quantisierung/Timing. Die einzelne reale SOC-Messung von 4 % liegt unter der festgelegten Simulationsreserve und wird nur als mögliche Messabweichung/Rundung/Ausnahme dokumentiert, nicht als zulässiger Simulationszustand. Für den vollständigen AC↔AC-Round-trip wurde in der begrenzten Inventur keine separate AC-Batterie-Energie-Reihe gefunden; Konverterverluste könnten über einen geprüften AC-Flussabgleich geschätzt werden, sobald korrigierte Last und PV-AC-Grenzen feststehen. Ein einzelner Gesamtwirkungsgrad wäre erst dann brauchbar, wenn er aus einem physikalisch geschlossenen AC-in/AC-out-Zyklus stammt und seine Anwendbarkeit über Leistungsbereiche geklärt ist; diese Datenbasis liegt aktuell nicht vor. Noch keinen festen Wirkungsgrad festschreiben. 98 % Maximal- und 97.5 % Euro-PEI-Datenblattwerte werden nicht als Ersatz für Empirie verwendet.

### PV-Betriebsmodi und Verfügbarkeit

Physische PV-Erzeugung und für Haus/Batterie **wirtschaftlich bzw. technisch verfügbare** PV müssen getrennt bleiben.

- **Zwei technische PV-Varianten:** `without_old_pv` berücksichtigt `mt_stall_alt_1/2` nicht als lokal verfügbare Erzeugung; `with_old_pv` behandelt sie wie die übrige verfügbare PV: erst Hauslast decken, Überschuss in den Speicher und verbleibenden Überschuss ins Netz. Beide Varianten laufen für dieselbe Batterie-Konfiguration und denselben historischen Last-/PV-Zeitraum.
- Diese Varianten bilden ausdrücklich keine aktuelle Tarif- oder Vertragsabrechnung nach. Die reale Alt-PV wird gegenwärtig laut Nutzer vollständig verkauft/eingespeist; diese aktuelle Situation ist nicht die Definition des technischen `without_old_pv`-Falls. Die Simulation ändert historische Messdaten nicht.
- Alle nicht gespeicherten PV-Überschüsse werden in beiden Varianten als Netzeinspeisung bilanziert. `pv_mode` markiert, ob alte PV zur Simulation zugelassen war. Tarif-/Opportunitätskosten (für spätere Grafana-Auswertung etwa 8 ct/kWh Einspeisung und 28 ct/kWh vermiedener Netzbezug) werden nicht in Python berechnet.
- `fems_productiondcactualpower` darf nicht zusätzlich zu `mt_stall_neu_*` addiert werden, solange nicht feststeht, dass es unterschiedliche Anlagen/Abschnitte ohne Überlappung misst. PV-Anlagen erst nach Systemgrenzenprüfung in eine gemeinsame Erzeugungsbilanz überführen.

### Tagesverarbeitung, SOC und Wiederanlauf

Die Verarbeitung bleibt chronologisch vom ältesten zum neuesten Tag. Für jede Szenarioversion startet Tag N mit dem End-SOC von Tag N−1; ein lokaler Berliner Kalendertag dauert bei Zeitumstellung 23 oder 25 Stunden. Zeitstempel intern in UTC halten, Tagesfenster an Europe/Berlin definieren und nie eine feste Zahl Sekunden je Tag voraussetzen. Der Nutzer legt für den ersten Tag ohne Vortagszustand `start_soc_pct: 5` als dokumentierten/konfigurierbaren Initialwert fest.

- Energie/SOC nicht täglich zurücksetzen. Endenergie von Tag N ist Startenergie von Tag N+1 je Szenario.
- Den ersten Startzustand einer neuen Szenarioversion auf den konfigurierbaren Initialwert 5 % setzen, sofern kein Vortagszustand existiert. Das entspricht der festgelegten Notstromreserve und vermeidet unbegründete Übernahme eines realen SOC auf eine andere Kapazität.
- Ein Tagesresultat braucht mindestens Startenergie, Endenergie und Modell-/Konfigurationsfingerprint. Ein fehlender oder ungültiger Vortag darf nicht still durch einen festen Default ersetzt werden.
- Ein abgeschlossener historischer Tag gilt in der bestehenden Version als fest. Im Normalbetrieb werden chronologisch nur noch ausstehende neuere Tage verarbeitet; der Endzustand dient als Start für den Folgetag.
- Wird eine historische Neuberechnung nötig, neue Simulationsversion vergeben und alle Tage ab dem ältesten Datum chronologisch neu rechnen. Keine inkrementelle Reparatur einzelner abhängiger SOC-Folgetage in einer alten Version. Implementierungsseitig muss die neue Version leer starten und Ergebnisse dürfen nicht mit alten Versionen vermischt werden.
- Teilabbrüche und partielle Tagesergebnisse mit Tagesvollständigkeitsmarker behandeln. Rohdaten gelten nach Abschluss als unverändert; falls verspätete Rohdaten doch vorkommen, eine neue Gesamtversion starten.
- Aktueller Writer löscht keine alten Punkte. Gleiche Zeit/tag/Measurement/Field-Kombination überschreibt in InfluxDB den Feldwert; veraltete Punkte mit abweichenden Zeitstempeln oder Tags bleiben. Rechenläufe müssen deshalb idempotent sein und eine explizite Strategie für Versionswechsel/alte Punkte bekommen.

### Konkreter InfluxDB-Ergebnisvertrag

**Konkrete Bucket-Empfehlung:**

| Stage / Zweck | Input / Processing | Simulationsergebnis |
| --- | --- | --- |
| Produktion | Rohdaten aus `HomeAssistant`; korrigierte Hauslast und übriges Processing nach `HomeAssistant_processed` | `Szenario` |
| Entwicklung/Test | Rohdaten aus `HomeAssistant` read-only; korrigierte Processing-Testausgabe nach `testing` | `testing` |

Die korrigierte Hauslast ist normales Processing und wird in `HomeAssistant_processed` gespeichert; Simulationsergebnisse gehen in den eigens angelegten Bucket `Szenario`. Dev bleibt mit `testing` von produktiven Szenarioergebnissen getrennt. Der Szenariolauf liest die korrigierte Last aus dem Processing-Output und Roh-PV-/Batteriesignale aus `HomeAssistant`.

Die Processing-Buckets sind passend konfiguriert: dev liest `HomeAssistant` und schreibt nach `testing`; prod liest `HomeAssistant` und schreibt nach `HomeAssistant_processed`. Das Szenarioziel wird über `scenarios.buckets.output_bucket` gewählt: `Szenario` in prod und `testing` in dev. Der Hauslast-Processor verwendet den vorhandenen stageweiten `processing.output_bucket`.

Die Implementierung verwendet zwei Measurements im jeweiligen Szenariozielbucket, weil SOC/Leistung ein Verlauf sind und Energiekennzahlen Tageswerte:

| Measurement | Zeitstempel | Tags | Beispiel-Fields |
| --- | --- | --- | --- |
| `battery_scenario_timeseries` | UTC-Ereignis-/Intervallzeit | `scenario`, `pv_mode`, `run_version`, `run_reason`, `model_version` | `soc_pct`, `stored_energy_kwh`, `house_load_kw`, `pv_generation_kw`, `pv_to_load_kw`, `pv_to_battery_kw`, `battery_to_load_kw`, `battery_charge_dc_kw`, `battery_discharge_dc_kw`, `grid_import_kw`, `grid_export_kw`, `pv_export_kw`, `quality_valid`, `quality_uncertain` |
| `battery_scenario_daily` | lokales Tagesende (Europe/Berlin, als UTC-Instant gespeichert) | `scenario`, `pv_mode`, `run_version`, `run_reason`, `model_version` | `pv_generation_kwh`, `pv_to_load_kwh`, `pv_to_battery_kwh`, `pv_export_kwh`, `battery_to_load_kwh`, `battery_charge_dc_kwh`, `battery_discharge_dc_kwh`, `grid_import_kwh`, `grid_export_kwh`, `soc_start_pct`, `soc_end_pct`, `stored_energy_start_kwh`, `stored_energy_end_kwh`, `valid_duration_s`, `is_complete`, `quality_uncertain`, `quality_reason_code`, `input_fingerprint`, `local_day`, `config_hash` |

`scenario` ist z. B. `current_battery`, `scenario_1`, `scenario_2`; `pv_mode` ist `without_old_pv` oder `with_old_pv`. Diese Dimensionen sind Tags, nicht Fields. `run_version` trennt vollständige Neuberechnungen; `model_version` wählt den Algorithmus, `config_hash` dokumentiert die konkreten Parameter, ohne die gesamte Konfiguration als Tag zu schreiben. PV-/Last-/Netzleistungs- und Zuordnungsfelder sind AC-seitig; Batterie-Lade-/Entladeleistung und Speicherenergie sind ausdrücklich DC-seitig. Im idealisierten V1-Verlustmodell gilt die 1:1-Umrechnung als Annahme. Einheiten und AC/DC-Bezug stecken in den Field-Namen, damit verschiedene Einheiten nicht in einem mehrdeutigen `value`-Feld landen. Es gibt keine monetären Fields.

Die genannten Felder sind ein gemeinsamer Vertrag, nicht jeweils eigene Measurements. `pv_generation_kwh` zählt die im jeweiligen `pv_mode` zugelassenen PV-Quellen. `pv_export_kwh` ist der gewünschte tägliche PV-Einspeisewert pro Szenario. `grid_export_kwh` kann zusätzlich als gesamte Netzeinspeisung berichtet werden; bei der ersten Strategie ohne Batterieeinspeisung sollten beide Werte übereinstimmen, sofern keine weiteren Einspeisequellen im Modell liegen. `battery_losses_kwh` wird erst geschrieben, sobald ein Verlustmodell belastbar oder als explizite V1-Annahme aktiviert ist; fehlende Verlustkenntnis nicht als Messung von null ausgeben.

Der aktuelle `write_datapoint()`-Helper schreibt genau ein Field und fügt `entity_id`/`unit`-Tags hinzu. Für diese Mehrfeldstruktur sollte der Handler eine getrennte Mehrfeld-Schreibmethode erhalten, die einen Influx-Punkt mit gemeinsamer Tag-Menge und typisierten Fields schreibt; die vorhandene Methode bleibt für bestehende Prozessoren erhalten. Dasselbe `scenario` + `pv_mode`-Paar muss in Zeitreihe und Tagesmeasurement identisch sein, damit Grafana leicht vergleichen und auf Tagesebene aggregieren kann. Vermiedener Netzbezug und zusätzliche Eigenversorgung sind Baseline-Differenzen, keine eigenen monetären Resultate.

Python liefert ausschließlich physische Größen (W, kW, kWh, %, Dauer/Vollständigkeit). Netzpreis, Einspeisevergütung, Investitionskosten, Opportunitätskosten und Amortisation bleiben externe Grafana-Annahmen; die genannten ca. 28/8 ct/kWh werden nicht fest codiert.

### Konfigurationsgetriebene Szenarioliste

Die Implementierung soll Szenario-IDs und PV-Modi nicht im Python-Code aufzählen. `setup` enthält gemeinsame Grenzen, Eingangsreihen, Buckets und die V1-Verlustannahme; `pv_sources` beschreibt Gruppen von PV-Quellen; `pv_modes` kombiniert diese Gruppen; `definitions` enthält beliebig viele aktivierte Speicherfälle. Ein generischer Loop verarbeitet jedes aktivierte Szenario mit jedem konfigurierten PV-Modus. Damit benötigt `scenario_4` nur einen JSON-Eintrag.

Beispiel für die geplante Form (Entity-/Measurement-Namen vor Implementierung anhand des aktuellen Config-/Influxbestands exakt übernehmen):

```json
{
  "scenarios": {
    "buckets": {
      "source_bucket": "HomeAssistant",
      "processing_bucket": "HomeAssistant_processed",
      "output_bucket": "Szenario"
    },
    "setup": {
      "base_capacity_kwh": 22.4,
      "base_charge_power_dc_kw": 17.92,
      "base_discharge_power_dc_kw": 17.92,
      "min_soc_pct": 5,
      "max_soc_pct": 100,
      "initial_soc_pct": 5,
      "loss_model": "idealized_no_additional_losses_v1"
    },
    "sources": {
      "corrected_house_load": {
        "bucket_ref": "processing_bucket",
        "measurement": "W",
        "entity_id": "Hausverbrauch_korrigiert",
        "field": "value"
      },
      "fems_pv": {
        "bucket_ref": "source_bucket",
        "measurement": "W",
        "entity_id": "fems_productiondcactualpower",
        "field": "value"
      },
      "mt_stall_neu": {
        "bucket_ref": "source_bucket",
        "measurement": "W",
        "entity_id": "mt_stall_neu_leistung_ac_fixed",
        "field": "value"
      },
      "mt_stall_alt_1": {
        "bucket_ref": "source_bucket",
        "measurement": "W",
        "entity_id": "mt_stall_alt_1_leistung_ac_fixed",
        "field": "value"
      },
      "mt_stall_alt_2": {
        "bucket_ref": "source_bucket",
        "measurement": "W",
        "entity_id": "mt_stall_alt_2_leistung_ac_fixed",
        "field": "value"
      }
    },
    "pv_sources": {
      "local_pv": ["fems_pv", "mt_stall_neu"],
      "old_pv": ["mt_stall_alt_1", "mt_stall_alt_2"]
    },
    "pv_modes": {
      "without_old_pv": ["local_pv"],
      "with_old_pv": ["local_pv", "old_pv"]
    },
    "definitions": {
      "current_battery": {
        "enabled": true,
        "extra_capacity_kwh": 0,
        "extra_power_kw": 0
      },
      "scenario_1": {
        "enabled": true,
        "extra_capacity_kwh": 22.4,
        "extra_power_kw": 0
      }
    }
  }
}
```

Das Beispiel zeigt die **Produktionswerte**; in `config/dev.json` ist `output_bucket` und `processing_bucket` stattdessen `testing`. `source_bucket` bleibt in beiden Stages `HomeAssistant` und wird read-only verwendet.

Die gezeigte V1-Verlustoption ist eine transparente Idealisierung, keine gemessene Batteriekennzahl: keine zusätzlichen Speicherverluste, also 1:1-Energiebuchung zwischen AC-Modellflüssen und DC-Speicherzustand; die Leistungsgrenzen bleiben die konfigurierten DC-Grenzen. Resultate weisen diesen Modus als Annahme aus. Die widersprüchlichen realen SOC-/DC-Messquotienten werden durch `battery_validation.py` diagnostisch untersucht, aber nicht automatisch als Wirkungsgrad verwendet.

Der Hauslast-Processor verwendet den vorhandenen globalen `processing.output_bucket`: damit landet die korrigierte Reihe in prod in `HomeAssistant_processed` und in dev im isolierten Bucket `testing`. Szenarioergebnisse verwenden dagegen `scenarios.buckets.output_bucket`: prod `Szenario`, dev `testing`.

### Konkrete neue Processing-Komponenten

| Komponente | Aufgabe und Einbindung |
| --- | --- |
| `CorrectedHouseConsumptionProcessor` | Unter einem eindeutig benannten `entities_to_process`-Eintrag die zwei Quellen zeitlich zusammenführen, halten und addieren; Ergebnis versioniert über den vorhandenen Processing-Output (`HomeAssistant_processed` prod, `testing` dev) schreiben. |
| `InfluxDBHandler.get_latest_datapoint_by_time()` | Separates chronologisches `last()` für den Vorzustand von Change-only-Quellen. `get_last_datapoint()` und dessen numerische-Max-Semantik bleiben unverändert. |
| `InfluxDBHandler.write_fields_datapoint()` | Mehrere Fields gemeinsam in einem Punkt mit Tags `scenario`, `pv_mode`, `run_version`, `model_version` schreiben; bestehenden Single-Field-Writer unverändert weiterverwenden. |
| `BatteryScenarioRunner` + `BatteryScenarioEngine` | `scenarios.setup`, beliebige `definitions` und beliebige `pv_modes` laden/validieren; je Kombination inkrementell und chronologisch simulieren und `battery_scenario_timeseries` sowie `battery_scenario_daily` nach `Szenario` (dev: `testing`) schreiben. Als eigener Aufruf aus `main.py` nach dem normalen Processing, damit die korrigierte Hauslast zuvor vorliegt. |
| Tagescheckpoint/Run-Version | Start-/End-SOC, Konfigurationsfingerprint, Vollständigkeit und Laufversion speichern; bei historischem Rebuild eine neue Version ab dem ältesten Tag erzeugen. |

Die erste Implementierungsreihenfolge sollte Hauslast-Processor und Handler-Erweiterungen zuerst liefern, danach den generischen Szenariolauf, Tages-/SOC-Zustand und zum Schluss die Grafana-Abfragevalidierung. `current_battery` ist eine normale Definition ohne Zusatzkapazität oder -leistung und wird mit exakt denselben Inputs/PV-Modi wie Erweiterungen gerechnet.

**Wirtschaftlichkeit (spätere Grafana-Auswertung):** Python schreibt keine Euro-Werte. Grafana kann getrennte physische Mengen je Szenario verwenden: Netzbezug, Netzeinspeisung, PV-Eigennutzung, Batterieladung/-entladung und Verluste. Als später änderbare Beispielannahmen nennt der Nutzer etwa 28 ct/kWh für Netzbezug und 8 ct/kWh für Einspeisung. Die Wertdifferenz aus vermiedenem Netzbezug und entgangenem Einspeiseerlös wird in Grafana gerechnet; Investitionskosten und Amortisation ebenso. Die aktuelle Alt-PV-Vergütung wird durch die technischen PV-Modi nicht simuliert.

### Priorisierter Implementierungsfahrplan

#### Phase 0 – blockierende Daten- und Anlagenfragen

- [ ] PV-Quellenüberlappung und Leistungsmessgrenzen für den V1-Eingang prüfen. **Status:** Batterie-Leistung DC und FEMS-Wissensgrenze durch Nutzer geklärt; MT-Neu/Alt sind AC-Kandidaten. Vor einem belastbaren Lauf sicherstellen, dass `fems_productiondcactualpower` und MT-Neu nicht dieselbe PV-Erzeugung doppelt erfassen; DC-PV wird im idealisierten V1-Modell transparent 1:1 auf den AC-Energiebilanzpfad abgebildet.
- [x] Quellenrollen für Hauslastkorrektur: FEMS-Hausverbrauch plus separater MT-Stall-neu-Wert; Alt-PV nicht Teil dieses Korrekturpfads. **Status:** laut Nutzer fachlich festgelegt; beide Quellen als getrennte Influx-Reihen vorhanden.
- [ ] Die festgelegte Hauslastkorrektur im neuen Processor über repräsentative Zeiträume als Qualitätsprüfung auswerten (negative/auffällige korrigierte Werte, Alignment, Eventzustand). **Status:** Formel ist Nutzerfestlegung; die Stichprobe belegt ihre Größenordnung, nicht ihre Genauigkeit gegen unabhängigen Lastzähler. Kein alternativer Hauslastsensor in der begrenzten Inventur gefunden.
- [ ] Change-only-Semantik pro Reihe und Startzustandsabfrage implementieren: letzten Wert vor Tages-/Abfragebeginn laden, dann bis zum nächsten Ereignis fortschreiben, auch über Mitternacht. **Status:** Nutzer bestätigt Batterie-Leistung, SOC und die PV-Reihen als unverändert teils ohne neue Punkte; nachts 0 W PV ist normal. Für Grid und echte Recorder-Ausfälle noch unabhängig bestätigen.
- [ ] Gemeinsame Zeitintegration festlegen: empfohlen ist die sortierte Vereinigung der tatsächlichen Change-Events aller Reihen plus Tagesgrenzen; jeder bekannte Wert gilt bis zum nächsten Event und wird über die Intervalllänge integriert. Keine lineare Interpolation. DC- und AC-Seite getrennt halten. **Status:** Change-only-Fortschreibung fachlich geklärt; Zeitausrichtung, verspätete Events und Umrechnungen bleiben offen.
- [ ] Zeitversatz/Doppelpunkte/negative oder außerhalb der Gerätegrenzen liegende Werte suchen; Energiebilanz auf mehreren kurzen, synchronisierten Zeitfenstern gegen Zählerstände prüfen. **Status:** Einzel-Snapshot schloss wegen Quell-/Zeitversatz nicht; keine validierte Bilanz.
- [ ] Vorzeichen-/Messpunktdokumentation ablegen. **Status:** Grid + Bezug/− Einspeisung und Batterie-DC + Entladung/− Ladung sind durch Messmuster/Nutzerangabe gestützt; SOC-Simulationsgrenzen 5–100 % und Initialwert 5 % sind festgelegt. Exakte Measurement-Schreibweise und SOC-Quelle bleiben technische Query-Details.

#### Phase 1 – fachliche Referenz und Modellparameter

- [ ] Korrigierten Hausverbrauch als dedizierten konfigurierten Prozessor erstellen und auf Mehrtagesdaten plausibilisieren; fachliche Formel ist festgelegt.
- [ ] AC/DC-Umrechnung und Realbatterievalidierung nach V1 verfeinern. Die erste relative Vergleichsversion verwendet die unten dokumentierte idealisierte Verlustannahme; für absolute AC-Genauigkeit oder engere Abweichungstoleranzen sind PV-Inverter-/Batteriepfad oder passende AC-Messung nötig. Dies ist zusätzlich zur festgelegten Hausverbrauchskorrektur.
- [x] `current_battery` als simulierte Referenz plus getrennte Realmessungsdiagnostik definieren. Gleiche Last-/PV-Randbedingungen gelten für sämtliche Simulationen; die reale Batterie wird nicht als hypothetischer Speicherzustand verwendet.
- [x] Kapazitätssemantik: 22,4 kWh ist gesamte DC-Kapazität; Erweiterungs-kWh addieren. Davon zwischen 5–100 % SOC nutzbar sind 95 %. Lade-/Entladeleistung ist symmetrische DC-Base + Extra-Regel.
- [x] V1-Verlustmodell: `idealized_no_additional_losses_v1` als explizite Vereinfachung verwenden; reale Messdatenquotienten (113,4 % Laden/81,1 % Entladen unter Diagnoseannahmen) nicht als Wirkungsgrade übernehmen. Resultate als relative Abschätzung mit dieser Idealisierung kennzeichnen.
- [x] Gemeinsame Szenariokonfiguration (siehe Beispiel oben): Kapazität `base + extra`, symmetrische DC-Grenze `base power + extra power`, SOC 5–100 %, Betriebsstrategie, PV-Quellenmodus, Output-Bucket und V1-Verlustmodus werden schema-validiert. Maximalleistungen werden abgeleitet.
- [x] Betriebsstrategie als erste Self-Consumption-Strategie: PV deckt zuerst Last; Überschuss lädt bis DC-Leistungsgrenze und SOC 100 %, weiterer Überschuss ist Netzeinspeisung. Bei Unterdeckung deckt PV zuerst Last, Batterie liefert bis DC-Leistungsgrenze und SOC 5 %, Rest ist Netzbezug. Keine Netzladung und keine absichtliche Batterieeinspeisung. Erweiterte Strategien später.
- [x] Start-SOC ist geklärt: 5 % am ältesten Tag einer neuen Version; danach End-SOC jeweils als Folgetagsstart. Historische Korrekturen erzeugen eine neue Version und werden ab dem ältesten Tag vollständig neu gerechnet.
- [x] Messbasierte Realbatterie-Diagnostik mit Lade-/Entladeenergie, SOC-Energie und Plausibilitätsbereich implementieren. **Status:** Ein unsicherer oder widersprüchlicher Messpfad liefert `indeterminate` und keinen Korrekturfaktor.

#### Phase 2 – Konfigurationsgetriebene Szenario- und Zustandsarchitektur

- [x] Allgemeine Szenarioliste aus JSON einlesen/validieren; jede aktivierte Konfiguration wird automatisch verarbeitet, beliebige Namen sind möglich.
- [x] Simulation Engine pro Szenario mit unabhängigem Energiezustand; reale Batterie-Reihen bleiben Input/Validierung und werden nicht auf hypothetische Speicher kopiert.
- [x] Tagesfenster Europe/Berlin mit UTC-Datenpunkten über DST korrekt behandeln; Intervalle schneiden Tagesgrenzen sauber.
- [x] Persistenter Tageszustand über `battery_scenario_daily` mit Endenergie, Startenergie, Laufversion, Input-Fingerprint, Config-Fingerprint und Complete-Status.
- [x] Fehlende Erstzustände und nicht ausreichende Eingangsdaten werden als unvollständig geschrieben und nicht als vollständiger Tag fortgesetzt. Change-only-Haltewerte werden separat als `quality_uncertain` markiert.
- [x] Bei historischer Änderung startet eine neue Version ab dem Anfang; im Normalbetrieb werden nur ausstehende vollständige Tage verarbeitet.
- [x] Idempotentes Schreiben erfolgt über stabile Zeit-/Tag-/Laufversion-Schlüssel; alte Versionen bleiben getrennt erhalten und werden nicht mit neuen Punkten vermischt.

#### Phase 3 – PV-Modi und validiertes Simulationsmodell

- [ ] PV-Reihen mit dokumentierten Anlagenrollen und automatischer Überlappungsprüfung aggregieren. Die Rollen und Modi sind konfiguriert; die Messgrenzen-/Doppelzählungsprüfung gegen die reale Anlage bleibt vor der Interpretation offen.
- [x] PV-Varianten `without_old_pv` und `with_old_pv` als technische Szenariodimension verarbeiten; in beiden Fällen verbleibenden PV-Überschuss exportieren. Die aktuelle Tarif-/Vertragssituation ist nicht Teil der Simulation.
- [x] Verfügbaren Überschuss aus PV und Hauslast ableiten; reale Batterieleistung wird nur zur Realitätsdiagnostik genutzt.
- [x] Intervall-Energiefluss mit V1-Verlustannahme, SOC-Grenzen, maximaler DC-Leistung, Kapazität, gültigen Inputs und einheitlicher Strategie abbilden.
- [x] Simulierte aktuelle Batterie gegen reale Messung zeitlich und feldweise diagnostisch vergleichen. Der Vergleich berichtet SOC-MAE, Leistungs-MAE sowie Lade-/Entladeenergie; er kalibriert V1 nicht automatisch.
- [x] Erweiterungsszenarien dynamisch rechnen und alte PV-Betriebsart explizit mit Szenario-ID/Parametern kennzeichnen.

#### Phase 4 – Ergebniszeitreihen und Kennzahlen

- [x] Ergebnis-Measurements `battery_scenario_timeseries` und `battery_scenario_daily` mit Tags `scenario`, `pv_mode`, `run_version` und Mehrfeld-Fields festgelegt.
- [x] Zeitreihen von SOC/Energie, PV-Produktion und -Zuordnung, Last, Import/Export, Lade-/Entladeleistung und Intervallgüte in das Timeseries-Measurement schreiben.
- [x] Tageswerte inklusive `pv_export_kwh`, PV direkt genutzt/geladen, Batterie-Lade-/Entladeenergie, Import/Export, Start-/End-SOC, gültiger Dauer und Vollständigkeit in das Daily-Measurement schreiben.
- [ ] Vergleichswerte immer gegenüber `current_battery` und mit Definition ausweisen: zusätzlicher Eigenverbrauch, vermiedener Import, zusätzliche Einspeisung, Speicherverluste.
- [ ] Quotienten nur bei definiertem Nenner und vollständigen Daten berechnen. Eigenverbrauchsquote, Autarkiequote und „gespeicherte PV“ nicht ohne Formel/Systemgrenze berichten.
- [~] Fehlende Werte, unbekannte Werte, negative Leistungen und SOC-/Leistungsgrenzen werden geprüft; `quality_uncertain` trennt gültige Change-only-Haltewerte von nicht beweisbaren Ausfällen. Eine vollständige automatische Recorder- und PV-Überlappungsdiagnostik bleibt offen.

#### Phase 5 – Grafana und Wirtschaftlichkeit (später)

- [ ] Szenario-/Zeitreihenvergleich in Grafana aufbauen, nachdem Schema und Baseline validiert sind.
- [ ] Importpreis, Einspeisevergütung (insbesondere entgangener Alt-PV-Erlös), Investitions-/Installationskosten und Preisgültigkeitszeitraum als externe Annahmen pflegen.
- [ ] Jährlichen Mehrwert und Amortisation aus validierten kWh-Kennzahlen in Grafana berechnen; keine Wirtschaftlichkeits- oder Optimierungslogik in die erste Python-Implementierung aufnehmen.

### Offene Fragen / Annahmen

| Frage | Status | Nächster Schritt / Wirkung |
| --- | --- | --- |
| Grid-Vorzeichen und Messpunkt | **teilweise datenbasiert, zu bestätigen** | + Bezug/− Einspeisung ist durch beobachtete Muster gestützt; Zählergrenze gegen Geräteinfos prüfen. |
| Batterie-Leistungs-Vorzeichen | **teilweise datenbasiert, zu bestätigen** | + Entladung/− Ladung wird von SOC-Verlauf gestützt; Herstellerkonvention bestätigen. |
| SOC Measurement und Einheit | **Einheit plausibel, Schreibweise/Genauigkeit zu prüfen** | `%` und Bereich 5–100 % für das Modell; Measurement aktuell wörtlich `„%“`. Rohprofil enthält einmal Werte-Minimum 4 %, daher Reservegrenze/Anzeigerundung bzw. Ausnahme gegen die Vorgabe 5 % abgleichen. |
| PV Einheiten und AC/DC/Systemgrenzen | **teilweise bekannt** | Measurement `W`; `fems_productiondcactualpower` ist DC-Kandidat, MT-Neu/Alt sind AC-Kandidaten. Gemeinsamen Grid-Messpunkt und ggf. Alt-PV-Einspeisung an ihm verifizieren. |
| Change-only Batterie/SOC/PV | **fachlich geklärt, technisch zu implementieren** | Letzten Zustand bis zur nächsten Änderung fortschreiben, Vortagspunkt initial laden; Change-only-Pause nicht als Fehler oder Null behandeln. Echte Recorder-Ausfälle gesondert erkennen. |
| Grid Change-only/Ausfallverhalten | **noch zu bestätigen** | Vorzeichen ist gestützt; Schreibsemantik und Erkennung tatsächlicher Messausfälle klären. |
| FEMS-Hausverbrauchskorrektur | **fachlich geklärt; technischer Processor offen** | Ausgabe ist `elektrischer_verbrauch + mt_stall_neu_leistung_ac_fixed`; `mt_stall_alt_1/2` werden nicht addiert. Event-basiertes Alignment und Plausibilitätsprüfung implementieren. |
| AC/DC-Systemgrenzen | **teilweise geklärt** | Batterie-Leistung und Leistungsgrenzen sind DC; FEMS-PV ist DC bezeichnet, MT-Neu/Alt sowie Grid AC. Für absolute AC-Bilanzen braucht es geeignete Umrechnung/Verlustwerte. |
| Batteriekapazität | **geklärt** | 22,4 kWh gesamte DC-Kapazität; bei 5–100 % SOC 21,28 kWh nutzbar. Erweiterungskapazitäten addieren sich zur Base. |
| Lade-/Entladeleistung | **geklärt** | Für beide Richtungen DC-seitig `Base 17,92 kW + Extra Charging Power`. |
| Min-/Max-SOC und Initialwert | **fachlich geklärt** | Simulation 5–100 %, erster Tag neuer Version startet bei 5 %, danach SOC-Kontinuität. |
| Wirkungsgrad/Verluste | **V1-Annahme festgelegt; empirisch später verfeinern** | V1 nutzt explizit `idealized_no_additional_losses_v1` als relative Abschätzung. Diagnosequotienten 113.4 % Laden/81.1 % Entladen werden nicht übernommen. Reale SOC-/Leistungsmessung später auflösen. |
| Betriebsstrategie und PV-Modi | **für V1 geklärt** | PV deckt Last, Überschuss lädt; Restexport. `without_old_pv` schließt Alt-PV aus, `with_old_pv` nimmt sie lokal auf; aktueller Vertrag/Tarif wird nicht simuliert. Keine Netzladung/Batterieeinspeisung. |
| Zeitraster und Synchronisierung | **Methode festgelegt; technisch zu validieren** | Eventzeitstempel vereinigen, pro Quelle letzten Zustand halten, Summe an jeder Quelländerung schreiben und über Intervalle integrieren. Keine lineare Interpolation. FEMS-Verbrauch ist noch auf Sample-and-Hold-Semantik/echte Ausfälle zu prüfen. |
| Vorfensterzustand aus Influx | **bestehende Semantik absichtlich beibehalten** | `get_last_datapoint()` nimmt das numerische Maximum und wird derzeit vom Tagesaggregat auf Tageszählern verwendet. Neue `get_latest_datapoint_by_time()`-Funktion für Change-only-Zustände ergänzen; bestehende API nicht ändern. |
| Ziel-Buckets | **umgesetzt** | Korrigierte Hauslast nach `HomeAssistant_processed` (prod; `testing` dev), Simulation nach `Szenario` (prod; `testing` dev). |
| Ergebnis-PV-Export | **fachlich festgelegt** | Tägliches `pv_export_kwh` je `scenario`/`pv_mode` in `battery_scenario_daily` schreiben; auch für größere Batterien. Keine Euro-Felder. |
| Influx Multi-Field-Punkte | **umgesetzt** | `write_fields_datapoint()` schreibt gemeinsame Fields und Tags; der bestehende Single-Field-Writer bleibt kompatibel. |
| Reale Batterie als Referenz | **diagnostisch umgesetzt, AC-Pfad begrenzt** | DC-Leistung und SOC werden integriert, Effizienzbereiche und ein Simulationsvergleich werden protokolliert. Ein separater AC-Batterieenergiezähler sowie belastbare Grid-/PV-Vergleichsreihen fehlen; bei widersprüchlichen Verhältnissen bleibt das Ergebnis `indeterminate`. |
| Tageszustand/Wiederanlauf | **umgesetzt** | `battery_scenario_daily` speichert Start-/Endenergie, `local_day`, Fingerprints und Vollständigkeit; Neustarts rekonstruieren den SOC aus dem letzten vollständigen Tag. |
| Energie-/Wirtschaftskennzahlen | **Definition erforderlich** | Importvermeidung als Baseline-Differenz; Einspeisung, Verluste und entgangene Vergütung separat berichten. |

### Was vor einem belastbaren Simulationsergebnis noch zwingend ist

| Priorität | Zu klären/umzusetzen | Warum es blockiert oder nicht blockiert |
| --- | --- | --- |
| **Vor der Implementierung fachlich festgelegt** | Bucketziele, beide PV-Modi, V1-Verlustannahme, Storage/SOC-/Leistungsgrenzen, Tageskontinuität und Ergebnis-Measurements übernehmen. | README und Beispielkonfiguration legen die V1-Schnittstellen fest; die Werte sind keine stillen Defaults. |
| **umgesetzt** | Mehrfeld-Writer und `scenarios.buckets.output_bucket`: `Szenario` (prod), `testing` (dev). | Der bestehende Single-Field-Writer bleibt kompatibel. |
| **Implementierungsblocker** | Hauslast-Processor mit Event-Union, Hold-last-value, Vorfensterzustand und versioniertem Output ergänzen; dafür `get_latest_datapoint_by_time()` separat einführen. | Ohne korrektes Alignment/Fortschreiben ist die festgelegte Hauslast falsch. `get_last_datapoint()` bleibt unverändert für Tageszähler-Maxima. |
| **umgesetzt** | Szenario-Engine als generischen Durchlauf über `definitions × pv_modes` mit Tageszustand, Fingerprints und Vollständigkeit. | Neue Szenarien und Modi werden ausschließlich über JSON ergänzt. |
| **Vor dem ersten aussagekräftigen V1-Lauf** | Exakte Eingangsreihen prüfen, insbesondere PV-Überlappung/FEMS-PV-Systemgrenze, und den Hauslast-Processor über repräsentative Tage plausibilisieren. | Verhindert doppelt gezählte Erzeugung und falsche Last. Das ist Datenprüfung, kein offener Strategiewunsch. |
| **V1-Annahme, kein Blocker** | Idealisiertes Modell `idealized_no_additional_losses_v1` ausweisen; AC/DC-Energiebuchung 1:1 als Vereinfachung und DC-Leistungsgrenzen wahren. | Ermöglicht den gewünschten relativen Vergleich ohne erfundenen Wirkungsgrad. Absolute physikalische Genauigkeit wird dadurch nicht behauptet. |
| **Spätere Modellverbesserung** | Empirische DC-/SOC-Verluste, Kapazitätskurve, Standbyverluste und AC/DC-Konverterpfad mit besseren Messungen untersuchen. | Vorhandene Messquotienten sind widersprüchlich; die V1-Engine kann durch austauschbares Verlustmodell vorbereitet werden. Für höhere absolute Genauigkeit wird diese Verfeinerung wichtig. |
| **Spätere Grafana-Arbeit** | Kosten, Vergütung, Investitionskosten, Amortisation und Preiszeitreihen konfigurieren. | Python gibt nur physische Größen aus. Die Beispielwerte 28/8 ct/kWh bleiben editierbare Grafana-Annahmen. |

### Abschließende Einschätzung

Die technische V1 ist implementiert und für einen kontrollierten DEV-Erstlauf vorbereitet. Korrigierter Hauslast-Processor, zeitlicher Latest-Query, Mehrfeld-Writer, inkrementeller Konfigurations-/Szenariodurchlauf, persistenter Tageszustand sowie Zeitreihen- und Tages-Measurements sind vorhanden. Neue Szenarien und PV-Modi kommen ausschließlich über Konfiguration hinzu. Das Produktionsziel ist `Szenario`, das Entwicklungsziel `testing`. Die verbleibende fachliche Vorprüfung betrifft insbesondere PV-Überlappung und Messgrenzen.

Ein V1-Lauf kann mit der transparenten verlustlosen Idealisierung stattfinden und ist als relative Abschätzung zu markieren, nicht als kalibrierte Prognose. Vor Interpretation der Ergebnisse müssen die PV-Quellen auf Überlappung geprüft, die korrigierte Hauslast über repräsentative Tage plausibilisiert und unvollständige Datenintervalle kenntlich sein. Spätere Modellverbesserungen umfassen empirische Batterie-/Konverterverluste und Messgrenzenabgleich; deren aktuelle Unsicherheit blockiert nicht die V1-Implementierungsplanung.

Die Varianten `without_old_pv` und `with_old_pv` sind fachlich festgelegt. `pv_export_kwh` wird für jeden lokalen Tag und jedes Batterie-/PV-Szenario geschrieben. Die 8/28-ct-Annahmen, Opportunitätskosten und Amortisation bleiben außerhalb von Python in Grafana.

### Harte Grenzen für nachfolgende Implementierungsschritte

`elektrischer_verbrauch` darf ausschließlich als Quelle der festgelegten korrigierten Reihe verwendet werden, nicht unverändert als Simulationslast. Change-only-Werte bis zur nächsten Änderung fortschreiben; nicht beweisbare Recorder-Ausfälle als `quality_uncertain` markieren und fehlende Erstzustände als unvollständig behandeln. `get_last_datapoint()` nicht semantisch ändern; für chronologisch letzte Change-only-Zustände wird die separate Abfrage verwendet. PV-Reihen vor Interpretation auf Überlappung prüfen; DC-Leistungsgrenzen und V1-Verlustannahme explizit kennzeichnen. SOC nicht an Tagesgrenzen zurücksetzen. Python schreibt keine Kosten/Amortisation. Für jeden vollständigen lokalen Tag und jede Kombination aus `scenario`/`pv_mode` wird Tagesenergie einschließlich `pv_export_kwh` nach `Szenario` (dev: `testing`) geschrieben.

### MQTT-Status für Home Assistant (implementiert)

Die MQTT-Integration ist ein zusätzlicher Betriebs- und Health-Kanal zwischen Python-Processing und Home Assistant. Die Energiedaten und Verarbeitungsergebnisse bleiben in InfluxDB und Grafana. MQTT überträgt nur kompakte Informationen über den Betrieb des Processing-Jobs; es ersetzt weder die InfluxDB-Ausgaben noch die normalen Python-Logs.

Die Implementierung verwendet `paho-mqtt` 2.x mit MQTT 3.1.1. MQTT ist in `prod` standardmäßig aktiv und in `dev`/`test` deaktiviert. Broker, Port, TLS, Client-ID, Topic-Präfixe und Timeouts stehen in `config/<stage>.json`; Benutzername und Passwort werden ausschließlich über die dort benannten Environment-Variablen geladen. Der letzte Laufstatus wird als retained MQTT-Wert gespeichert; eine zusätzliche Statusdatei oder ein persistenter Fehler-Latch ist nicht nötig.

#### Bestehender Betrieb und Grenzen

- Der Produktivbetrieb startet `src/main.py` derzeit einmal täglich um 04:00 Uhr über Cron. `flock` verhindert überlappende Läufe; das Python-Programm beendet sich nach einem Lauf. Es handelt sich somit aktuell nicht um einen dauerhaft laufenden Daemon.
- Während der langen Pause zwischen zwei Cron-Läufen ist `offline` der erwartete Prozesszustand. Home Assistant darf diesen Zustand nicht allein als Ausfall interpretieren. Ein ausgebliebener Lauf wird über den Zeitabstand seit dem letzten erfolgreichen Lauf beziehungsweise seit dem erwarteten Laufzeitpunkt erkannt.
- `main()` bietet einen zentralen Lebenszyklus für Start, Verarbeitung, Exceptions, InfluxDB-Disconnect und Laufabschluss. Unerwartete Exceptions führen bereits zu einem Fehlercode und einer Zusammenfassung in `runs.log`.
- `HomeAssistantProcessor.process_data()` liefert aktuell eine Tagesanzahl, aber keinen verlässlichen letzten vollständig verarbeiteten Tag. Die Tagesanzahl ist das Maximum der Rückgabewerte einzelner Processor und keine globale Fortschrittsbestätigung.
- Processor erzeugen `WARNING`- und `ERROR`-Logmeldungen. Der laufbezogene Warning-Collector ordnet Warnungen dem aktuellen Lauf zu und bestimmt damit den Status `SUCCESS_WITH_WARNINGS`.
- Der Launcher kann bereits vor dem Start von Python scheitern, zum Beispiel bei `git pull`, Logrotate-Prüfung oder Start des virtuellen Environments. Ein MQTT-LWT des Python-Prozesses deckt Fehler vor dem Python-Start nicht ab. Der Zeitüberschreitungsalarm auf den erwarteten erfolgreichen Lauf bleibt deshalb erforderlich.

#### Statusmodell

Home Assistant erhält einen Status für das Ergebnis des letzten Laufs und einen getrennten Zustand für die aktuelle Ausführung:

| Zustand | Bedeutung |
| --- | --- |
| `SUCCESS` | Lauf wurde ohne Warnungen und ohne fatalen Fehler abgeschlossen. |
| `SUCCESS_WITH_WARNINGS` | Lauf wurde abgeschlossen, aber mindestens eine Python-Logging-Warnung ist aufgetreten. |
| `ERROR` | Lauf endete mit einem fatalen Fehler. |

`processing_last_run_status` bleibt zwischen Cron-Läufen durch MQTT Retain sichtbar. `processing_run_state` wechselt beim Start auf `RUNNING` und bei regulärem Abschluss auf `IDLE`. Ein neuer Lauf überschreibt den Ergebnisstatus erst bei seinem Abschluss. Ein neuer erfolgreicher Lauf oder ein erfolgreicher Lauf mit Warnungen ersetzt den vorherigen Zustand `ERROR`. Stirbt Python oder die MQTT-Verbindung unerwartet ab, veröffentlicht der Last Will `ERROR` auf dem Ergebnis-Topic; der Laufzustand bleibt dann auf `RUNNING` und zeigt den nicht regulär abgeschlossenen Lauf an.

#### Warnungen und Logging

- Alle `WARNING`-Meldungen des Python-Loggings werden laufbezogen gezählt; bis zu fünf kurze Warnungsbeispiele werden für die Anzeige ausgewählt.
- Wenn kein fataler Fehler vorliegt und mindestens eine Warnung entstand, lautet das Ergebnis `SUCCESS_WITH_WARNINGS`. Ein fataler Fehler hat Vorrang und führt zu `ERROR`.
- Die kompakte `runs.log`-Zusammenfassung enthält Status, Laufzeit, Anzahl verarbeiteter Tage und bei Warnungen mindestens die Warnungsanzahl. Vollständige Meldungen und Stacktraces bleiben in den normalen Python-Logs.
- Die Discovery-Entity `Diagnose letzter Lauf` zeigt bei Warnungen eine gekürzte, bereinigte Auswahl der Warnungsmeldungen und bei Fehlern Verarbeitungsschritt, Exception-Klasse und kurze Fehlermeldung. Zugangsdaten, Tokens, Query-Inhalte und Rohmessdaten dürfen nicht übertragen werden.

#### MQTT-Werte und Home-Assistant-Entities

| Entity | Zweck | Zustände / Einheit |
| --- | --- | --- |
| `sensor.python_processing_prod_processing_last_run_status` | Ergebnis des letzten abgeschlossenen Laufs. | `SUCCESS`, `SUCCESS_WITH_WARNINGS`, `ERROR` |
| `sensor.python_processing_prod_processing_run_state` | Aktueller Laufzustand. | `RUNNING`, `IDLE` |
| `sensor.python_processing_prod_processing_last_success` | Zeitpunkt des letzten abgeschlossenen Laufs ohne fatalen Fehler; Erfolg mit Warnungen zählt als abgeschlossen. Bei `ERROR` bleibt der ältere Wert stehen. | UTC-Zeitstempel |
| `sensor.python_processing_prod_processing_last_runtime` | Dauer des letzten Laufs. | Sekunden |
| `sensor.python_processing_prod_processing_last_processed_date` | Letzter vollständig verarbeiteter lokaler Tag, wenn die Pipeline einen Wert liefert. | `YYYY-MM-DD` |
| `sensor.python_processing_prod_processing_last_run_diagnostic` | Gekürzte Warnungsbeispiele oder Fehlerdetails des letzten Laufs. | Text |

Ein zusätzlicher Sensor „Fehler gespeichert“ wird nicht angelegt. Das Ergebnis des letzten Laufs und der aktuelle Laufzustand bleiben getrennte, eindeutige Aussagen. Das Dashboard enthält die Übersicht samt Verlauf sowie eine Karte „Diagnose letzter Lauf“.

#### MQTT Topics, Retain, LWT und Discovery

- Alle sechs Entities werden vollständig über Home-Assistant-MQTT-Discovery eingerichtet; manuelle MQTT-Sensor-YAML-Einträge in Home Assistant sind nicht nötig.
- Discovery Topics folgen dem Schema `homeassistant/<component>/<node>/<object_id>/config`. Jedes Objekt hat eine stabile, stage-spezifische `unique_id` und gemeinsame Device-Informationen für „Python Processing“.
- Ein stage-spezifisches Topic-Präfix trennt mindestens `dev`, `test` und `prod`. Die Discovery-Konfigurationen und letzte bekannte Ergebniswerte werden retained veröffentlicht, damit sie nach einem Neustart von Home Assistant oder des Brokers wiederhergestellt werden.
- Der MQTT-Client registriert einen retained Last Will mit Payload `ERROR` direkt auf dem Status-Topic. Ein unerwarteter Prozess- oder Verbindungsabbruch wird dadurch als fataler Fehler sichtbar. Bei regulärem Prozessende bleibt der zuletzt veröffentlichte Laufstatus unverändert.
- Der Status-Sensor verwendet kein kurzlebiges Availability-Topic. Andernfalls wäre er zwischen den täglichen Cron-Läufen erwartungsgemäß `unavailable` und würde den retained letzten Laufstatus im Dashboard verdecken.
- Retained Discovery-Konfigurationen müssen bei einer späteren Entfernung oder Umbenennung der Integration gezielt gelöscht werden können, damit keine verwaisten Home-Assistant-Entities zurückbleiben.

#### Fehlerbehandlung, Konfiguration und Betriebssicherheit

- MQTT-Broker, Port, TLS, Benutzername, Passwort, Client-ID, Topic-Präfix und Discovery-Präfix werden über Umgebungsvariablen oder eine geschützte lokale Konfiguration gesetzt. Zugangsdaten gehören nicht ins Repository, in MQTT-Payloads oder Logs.
- MQTT-Timeouts und begrenzte Wiederholungsversuche verhindern, dass ein nicht erreichbarer Broker die Energiedatenverarbeitung dauerhaft blockiert. Ein MQTT-Ausfall wird separat geloggt und macht einen fachlich erfolgreichen InfluxDB-Lauf nicht nachträglich zu `ERROR`. Das Ausbleiben einer frischen Erfolgsmeldung muss Home Assistant dennoch über den Zeitwächter erkennen können.
- Verarbeitungsstatus wird nach der Processing-Auswertung und dem InfluxDB-Disconnect finalisiert, damit ein Fehler beim Disconnect nicht irrtümlich als Erfolg veröffentlicht wird. Bei einem unerwarteten Abbruch der MQTT-Verbindung veröffentlicht der Last Will `ERROR`; Fehler aus Launcher-Schritten vor Python bleiben zusätzlich über Launcher-Log und den fehlenden Lauf erkennbar.
- MQTT überträgt keine vollständigen Logs oder Stacktraces. Das Dashboard zeigt kompakte Laufzeit- und Fortschrittswerte sowie die gekürzte Warnungs-/Fehlerdiagnose; die vollständigen Details stehen in den Logs und `runs.log`.

#### Umsetzungsstand

Die laufbezogene Warning-Erfassung sowie der MQTT-Statuspublisher mit Discovery, LWT und Retain sind implementiert. Home Assistant zeigt Ergebnis, aktuellen Laufzustand, letzte Erfolgszeit, Laufzeit, Verarbeitungstag und Diagnose zum letzten Lauf. Der doppelte Fehler-Latch „Fehler gespeichert“ wird entfernt. Automatisierte Tests decken Erfolgsfälle, Warnungen, Fehlerdetails, MQTT-Ausfall, Retain, Discovery und den Last Will ab.
