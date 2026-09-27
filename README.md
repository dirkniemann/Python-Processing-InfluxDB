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
- Console + detailed file logging via `moduls.logger_setup`.
- Production runs append exactly one compact status line to `/var/log/Python_Auswertung/runs.log`.
- The summary is deliberately human-readable, with date/time, stage, status, processing result, and duration:
	```
	2026-09-27 04:00:01 | SUCCESS | 3 Tage verarbeitet | Dauer: 42 s
	2026-09-28 04:00:01 | SUCCESS | Keine neuen Tage verarbeitet | Dauer: 4 s
	2026-09-29 04:00:01 | ERROR | Fehler in data processing (RuntimeError): Verbindung zu InfluxDB fehlgeschlagen | Dauer: 8 s
	```
- Failed lines contain the processing step, exception type and a sanitized error message. Credentials and raw measurements are never written to the summary.
- The detailed timestamp-based logs remain available for diagnosis. The summary is rotated by Linux `logrotate` at 10 MB and keeps ten compressed backups.

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
- Install the external launcher once as described below. Then use the provided cron snippet [cron.d_influx_job](cron.d_influx_job):
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

### Einmalige Migration auf dem bestehenden Zielgerät

Diese Schritte sind nur für die Umstellung einer bestehenden Installation nötig. Danach übernimmt der externe Launcher den täglichen Pull und die Anwendung muss nicht mehr manuell kopiert werden.

1. Bestehenden Zustand sichern:
	```bash
	sudo crontab -l > /root/python-auswertung-crontab.backup 2>/dev/null || true
	sudo cp /etc/cron.d/influx_job /root/influx_job.backup
	sudo cp -a /opt/Python-Processing-InfluxDB /root/Python-Processing-InfluxDB.backup
	```
2. Prüfen, dass kein Lauf aktiv ist. Während der Umstellung darf der alte Cron-Eintrag nicht parallel zum neuen Launcher laufen.
3. Repository einmalig aktualisieren:
	```bash
	cd /opt/Python-Processing-InfluxDB
	sudo git pull --ff-only origin master
	```
4. Prüfen, dass `root` Zugriff auf das Git-Repository und die konfigurierte Git-Authentifizierung hat. `.env`, `venv/` und `logs/` bleiben außerhalb der Git-Änderungen und werden nicht durch `git pull` überschrieben.
5. Abhängigkeiten einmalig aktualisieren:
	```bash
	sudo /opt/Python-Processing-InfluxDB/venv/bin/python -m pip install -r /opt/Python-Processing-InfluxDB/requirements.txt
	```
6. Den stabilen Launcher außerhalb des Repositories installieren:
	```bash
	sudo install -m 0755 /opt/Python-Processing-InfluxDB/deploy/launcher/python-auswertung-launcher /usr/local/sbin/python-auswertung-launcher
	```
7. Die Logrotate-Regel erstmalig installieren und sicher prüfen:
	```bash
	sudo install -D -m 0644 /opt/Python-Processing-InfluxDB/deploy/logrotate/python-auswertung /etc/logrotate.d/python-auswertung
	sudo logrotate -d /etc/logrotate.conf
	```
	Der Dry-Run darf keine Rotation erzwingen. `logrotate -f` gehört nicht in die tägliche Ausführung.
8. Launcher einmalig manuell ausführen und kontrollieren:
	```bash
	sudo /usr/local/sbin/python-auswertung-launcher
	sudo tail -n 5 /var/log/Python_Auswertung/runs.log
	```
9. Cron-Eintrag auf die neue Version umstellen:
	```bash
	sudo install -m 0644 /opt/Python-Processing-InfluxDB/cron.d_influx_job /etc/cron.d/influx_job
	sudo service cron reload
	```
10. Einen abschließenden manuellen Launcher-Lauf ausführen und sicherstellen, dass `/etc/cron.d/influx_job` nicht noch den direkten Aufruf von `run_script.sh` enthält.

Nach dieser Migration reicht ein normaler Merge in `master`. Der Cron-Launcher führt danach automatisch `git pull --ff-only`, aktualisiert die Logrotate-Regel bei Änderungen, validiert sie und startet anschließend den neuen Repository-Stand.

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
- Integrate Fronius inverter data into FENECON flow
- Cross-check processed data against Home Assistant for consistency
- Die priorisierten nächsten Schritte für Wärmepumpen-Auswertung und Batteriespeicher-Konzept stehen im Abschnitt [Aktueller Stand und To Do](#aktueller-stand-und-to-do).

## Aktueller Stand und To Do

Dieser Abschnitt dokumentiert die umgesetzte v2-Auswertung und die verbleibenden fachlichen Grenzen. Read-only InfluxDB-Abfragen über `.env_agent` wurden auf kurze, gezielte Zeiträume begrenzt; die folgenden Einheiten, Punktzahlen und Zeitabstände sind Bestandsindikatoren, keine vollständige Validierung der Gerätesemantik.

### Ist-Stand der Wärmepumpen-Auswertung

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
| `fems_productiondcactualpower` | 10 s | 2 h 48 min 13 s | lange Nacht-/Datenlücke nicht ohne Sensorsemantik als Null interpretieren |
| `fems_essdischargepower` | 5 s | 13 h 28 min 13 s | mögliche Zustands-/Änderungsreihe; lange Zeit ohne Punkt darf nicht als 0 gelten |
| `fems_esssoc` | 2 min | 14 h 23 min 35 s | SOC-Verlauf hat deutliche Lücken für Tages- oder Simulationsbilanzen |
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

### Priorität 1 – Wärmepumpen-Auswertung stabilisieren

Ziel ist eine stabile Auswertung der **Gesamtverbräuche beider Wärmepumpen** und eine gemeinsame, konservative Zuordnung zum Netz. Es ist keine zusätzliche manuelle Datenqualitätsprüfung im späteren Betrieb vorgesehen.

1. **Sensorrollen und Einheiten eindeutig konfigurieren.** Die Statistik soll explizite Rollen für WP1-Zähler, WP2-Zähler, Grid-Leistung und beide Kompressoren verwenden; die Reihenfolge einer Liste darf keine Bedeutung haben. Messung, Entity, Feld und Einheit sind pro Rolle festgelegt. Einheiten zentral normalisieren: Zähler in kWh, Grid-Leistung entsprechend der bestätigten W/kW-Konfiguration, Kompressoren als 0/1. `config/dev.json` und `config/prod.json` bei Schemaänderungen gemeinsam pflegen.
2. **Zählerstände als Energiegrundlage erhalten.** Tageszähler beider Wärmepumpen bereinigen und Differenzen bilden. Zähler-Resets und Tageswechsel dürfen keine negativen oder doppelten Verbräuche erzeugen. `elektrischer_verbrauch` wegen des bekannten Quellfehlers nicht zur Wärmepumpen- oder Hauslastberechnung verwenden.
3. **Netzbudget genau einmal für beide Pumpen berechnen.** Je Intervall den tatsächlichen positiven Netzbezug als gemeinsames Budget verwenden. Zuerst WP1- und WP2-Verbrauch zusammen betrachten; niemals jeder Pumpe unabhängig das volle Netzsignal anrechnen. Immer gilt `WP1_grid + WP2_grid <= max(grid_import, 0)` und je Wärmepumpe `0 <= grid_share <= heat_pump_energy`. Reicht das Netzbudget für den gesamten WP-Verbrauch, wird der Verbrauch vollständig dem Netz zugeordnet. Reicht es nicht oder ist die Zuordnung unsicher, gilt der konservative Worst Case: möglichst viel des gemeinsamen WP-Verbrauchs dem Netz zurechnen, aber nie mehr als den tatsächlichen Netzbezug. Den verbleibenden WP-Verbrauch als PV-/Restanteil ausweisen, nicht als separat gemessenen PV-Fluss.
4. **Gemeinsames Budget fair und deterministisch aufteilen.** Wenn beide Pumpen gleichzeitig laufen und das Netzbudget kleiner als ihr Gesamtverbrauch ist, proportional zu ihren jeweiligen Verbrauchsanteilen aus den Zählerdifferenzen aufteilen. Das bevorzugt keine Pumpe und hält die Summe gedeckelt. Sind die Verbrauchsanteile im Teilintervall wegen der Zählerauflösung nicht genau bekannt, deren Energie anhand derselben Intervall-/Aktivitätsschätzung verteilen; bei verbleibender Mehrdeutigkeit eine deterministische proportionale Schätzung verwenden.
5. **Change-Only-Kompressoren als Aktivitätsgrenzen nutzen.** Zustand ab einem bekannten Ereignis bis zum nächsten Zustandswechsel fortschreiben. Die Sensoren grenzen Laufzeiten ein, ersetzen aber nicht die Energiezähler. Ein erster Wert `1` ohne vorheriges Ereignis belegt keinen Start genau am Abfragebeginn. Bekannte aktive Zeiträume helfen, Zählerdifferenzen zeitlich zuzuordnen; Kompressor `0` allein darf einen gemessenen Zähleranstieg nicht verwerfen.
6. **Zählerauflösung und zeitversetzte Updates berücksichtigen.** Zähler mit 0,01 kWh Auflösung melden einen Verbrauchsschritt erst nach Überschreiten der Schwelle; deshalb kann der Zähleranstieg nach dem Kompressor-AUS-Ereignis erscheinen. Nicht den Zeitstempel des Zählersprungs als exakten Verbrauchszeitpunkt behandeln. Zwischen zwei Zählerständen die gemessene Energiedifferenz zeitlich schätzen/interpolieren und dabei die Kompressor-Aktivitätsfenster berücksichtigen. Eine praktikable Regel ist, die gesamte Zählerdifferenz über die überlappenden Laufzeitfenster des Ableseintervalls zu verteilen; ein kleiner konfigurierbarer Nachlauf nach AUS kann quantisierungsbedingte verspätete Sprünge dem gerade beendeten Lauf zuordnen. Energieerhaltung muss gelten: die verteilten Teilmengen ergeben exakt die Zählerdifferenz. Ist kein Aktivitätsfenster vorhanden oder ist der Zustand am Intervallanfang unbekannt, bleibt für die Verteilung die konservative Schätzung maßgeblich; Energie nicht stillschweigend verwerfen.
7. **Ergebnisformat für die neue Berechnung festlegen.** Die neue Auswertung darf eigene, fachlich passende Felder und Measurements verwenden; Abwärtskompatibilität zu `v1` ist nicht erforderlich, da noch keine Visualisierung darauf aufbaut. Tageswerte aus den neuen Intervallwerten bilden.
8. **Gezielte Tests ergänzen und ausführen.** Zu testen sind: 5 kW Netz / WP1 3 kW; 0,5 kW / WP1 4 kW; 2 kW / je 1,5 kW (gemeinsame Netzsumme höchstens 2 kW); 5 kW / je 1,5 kW; nur WP1 aktiv; nur WP2 aktiv; beide aktiv mit proportionaler Verteilung; beide aus ohne erfundenen Verbrauch; Change-Only-Zustand bis zum nächsten Wechsel; Zähleranstieg nach Kompressor-AUS durch 0,01-kWh-Quantisierung; Einheiten und Rollen unabhängig von Konfigurationsreihenfolge; Resets und Tagesgrenzen. Tests müssen auch sicherstellen, dass die Summe der zugeordneten WP-Energie die Zählerdifferenzen nicht überschreitet.

### Priorität 2 – Datenbasis und reproduzierbare Verarbeitung

- Wenn bei der Umsetzung die Signalbedeutung, Einheit, zeitliche Auflösung oder ein konkreter Verlauf unklar ist, dürfen gezielte read-only Stichproben aus InfluxDB über `.env_agent` gezogen werden. Laut Nutzer sind die gleichen Signale seit Tag 1 mit unveränderter Auflösung vorhanden; für Semantik- und Algorithmusfragen genügt deshalb grundsätzlich eine kleine repräsentative Stichprobe. Keine pauschalen Abfragen über 365 Tage. Hilfsskripte ausschließlich unter `tools/` ablegen und keine Zugangsdaten aus `.env` oder `.env copy` verwenden.
- Zeitstempel, Zeitzone, Tagesgrenzen und gemeinsame Intervallbildung dokumentieren. Zählerdifferenzen und Kompressor-Aktivitätsfenster für denselben Zeitraum ausrichten; Tageswechsel und Zähler-Resets gezielt behandeln.
- Die neue Berechnung soll für alle verfügbaren Tage erneut laufen. Gleicher Input und gleiche Berechnungsversion müssen reproduzierbare Ergebnisse liefern; wiederholtes Schreiben soll bestehende Ergebnisse derselben Version ersetzen/aktualisieren statt sie zu summieren oder zu duplizieren.
- Schätzungen aus zeitlicher Verteilung/Interpolation klar in der README und im Code beschreiben; keine umfangreiche Laufzeit-Flag- oder manuelle Datenvalidierung einführen. Bei Unsicherheit greift die Worst-Case-Netzzuordnung aus Priorität 1.
- **Test-Bucket und Berechnungsversion:** Schreib- und Integrationstests verwenden ausschließlich `config/dev.json` mit dem Bucket `testing`. `config/prod.json` schreibt nach `HomeAssistant_processed` und wird nie für Tests verwendet. Die neue Auswertung schreibt als `v2`, verarbeitet alle vollständigen Tage erneut und muss bei Wiederholung dieselben Ergebnisse aktualisieren statt Summen zu duplizieren. `v1` muss nicht erhalten oder mit `v2` kompatibel sein. Vor jedem Schreibtest ist das Bucket-Ziel zu prüfen; produktive Zugangsdaten und Buckets bleiben unberührt. Änderungen am Konfigurationsschema und an erforderlichen Sensorrollen werden in beiden JSON-Dateien gespiegelt.

### Priorität 3 – Betriebslogging und Laufübersicht

Das aktuelle Logging erzeugt pro Lauf eine eigene, detaillierte Datei und schreibt zusätzlich technische Ausgaben in das Cron-Log. Für den täglichen Betrieb soll stattdessen eine zentrale, schnell lesbare Laufübersicht entstehen. Die Umsetzung erfolgt später; in diesem Arbeitsschritt wird nur die Anforderung festgehalten.

1. **Ein dauerhaftes Übersichtslog verwenden.** Es soll eine feste Datei geben, zum Beispiel `/var/log/Python_Auswertung/runs.log`. Jeder erfolgreiche Lauf erhält genau eine kompakte Zeile mit Startzeit, Endzeit oder Dauer, Stage, Ergebnis `SUCCESS` und einer kurzen Statistik wie Anzahl verarbeiteter Tage. Bei einem Lauf ohne zu verarbeitende Tage soll ebenfalls eine erfolgreiche Zeile geschrieben werden.
2. **Fehler ausführlich dokumentieren.** Bei einem fehlgeschlagenen Lauf muss der Eintrag mindestens Startzeit, Endzeit, Stage, Ergebnis `ERROR`, Fehlerklasse, verständliche Fehlermeldung und den betroffenen Verarbeitungsschritt enthalten. Ein vollständiger Traceback darf unterhalb der kompakten Fehlerzeile folgen, damit die Ursache ohne Zugriff auf weitere Dateien nachvollziehbar bleibt. Zugangsdaten, Tokens und sensible Konfigurationswerte dürfen niemals im Log erscheinen.
3. **Erfolg und technische Details trennen.** Bei erfolgreichen Läufen sollen keine umfangreichen Debug- oder Einzelpunktinformationen in der Übersicht stehen. Detaillierte technische Logs dürfen weiterhin separat und nur bei Bedarf für Diagnosezwecke geschrieben werden. Das Übersichtslog muss auch nach Wochen ohne besondere Werkzeuge scanbar bleiben.
4. **Exit-Code und Logstatus konsistent machen.** Ein Lauf darf nur dann als `SUCCESS` protokolliert werden, wenn das Skript vollständig und mit Exit-Code 0 beendet wurde. Jeder Fehler in `git pull`, Umgebung, Verbindung, Datenabfrage, Verarbeitung oder Schreiben muss als `ERROR` sichtbar sein und den Prozess mit einem Fehlercode beenden.
5. **Loggröße begrenzen.** Bei genau einer kompakten Erfolgszeile pro Lauf beziehungsweise Tag bleibt das Übersichtslog auch über mehrere Jahre sehr klein; ein periodischer Reset ist dafür nicht nötig. Das Größenrisiko entsteht hauptsächlich durch ausführliche Fehlerblöcke und Tracebacks. Empfohlen wird deshalb eine größenbasierte externe Rotation über `logrotate`: zunächst ein großzügiges Limit von zum Beispiel `10M` oder `50M`, danach Komprimierung und Aufbewahrung mehrerer rotierter Dateien. Eine zeitbasierte tägliche Rotation ist für dieses Übersichtslog nicht erforderlich. Bei Erreichen der Größenbegrenzung soll rotiert werden, ohne die aktive Datei manuell zu kürzen oder ihren Inhalt während eines Laufs zu löschen. Die Rotation muss atomar erfolgen, darf laufende Cron-Läufe nicht abschneiden und muss die Berechtigungen für den Cron-Benutzer berücksichtigen.
6. **Deployment und Logrotate-Installation sauber trennen.** Eine feste Regel, zum Beispiel `deploy/logrotate/python-auswertung`, soll im Repository liegen und auf `/var/log/Python_Auswertung/runs.log` zeigen. Das täglich gestartete Skript soll sich nicht auf eine während der eigenen Ausführung per `git pull` veränderte Kopie verlassen. Stattdessen soll ein kleiner, außerhalb des Repositories liegender und stabiler Launcher zuerst den Repository-Stand aktualisieren, danach die aktuelle Logrotate-Regel idempotent nach `/etc/logrotate.d/python-auswertung` installieren oder aktualisieren, zum Beispiel mit `install -m 0644`, und erst anschließend das aktualisierte `run_script.sh` per `exec` starten. Die tägliche Installation ist technisch unkritisch, soll aber nur bei einer tatsächlichen Änderung erfolgen oder zumindest ohne unnötige Seiteneffekte idempotent sein. Ein separater manueller Kopiervorgang ist nur bei der Ersteinrichtung nötig. Schlägt Pull, Regelinstallation oder Prüfung fehl, darf der eigentliche Verarbeitungslauf nicht gestartet werden.
7. **Logrotate-Regel prüfen.** Nach einer Änderung der installierten Regel soll der Launcher die Regel mit `logrotate -d /etc/logrotate.conf` beziehungsweise einer gezielten Dry-Run-Prüfung validieren. Dabei dürfen keine Dateien verändert werden. Die Regel soll Besitzer, Gruppe und Dateirechte der neu angelegten aktiven Logdatei ausdrücklich festlegen. Ein erzwungener Rotationslauf mit `logrotate -f` gehört nicht in den täglichen Launcher- oder `run_script`-Ablauf, sondern bleibt ein manueller Test.
8. **Tests ergänzen.** Erfolgreicher Lauf, Lauf ohne neue Tage, Fehler in jedem wesentlichen Verarbeitungsschritt, fehlende Schreibrechte, Rotation nach Größe, Installation beziehungsweise Aktualisierung der Regel und ein sicherer Dry-Run müssen automatisiert getestet werden. Zusätzlich ist zu prüfen, dass ein wiederholter Lauf genau eine neue Zusammenfassungszeile erzeugt und keine Secrets oder vollständigen Rohdaten loggt.

9. **Einmalige manuelle Migration dokumentieren.** Nach Umsetzung dieser Änderung muss die Produktionsumgebung einmalig händisch umgestellt werden. Die Reihenfolge soll mindestens sein: aktuellen Zustand und Cron-Konfiguration sichern; Repository einmal manuell auf den gewünschten Stand bringen; den stabilen Launcher außerhalb des Repositories installieren; die Logrotate-Regel erstmalig nach `/etc/logrotate.d/` installieren und mit einem Dry-Run prüfen; den Launcher manuell erfolgreich testen; anschließend den Cron-Eintrag von `/opt/Python-Processing-InfluxDB/run_script.sh` auf den externen Launcher ändern; Cron neu laden und einen abschließenden manuellen Testlauf ausführen. Erst danach darf der tägliche Launcher selbst Pull, Regelaktualisierung und Start des aktuellen Repository-Skripts übernehmen. Die alten und neuen Cron-Einträge dürfen nicht parallel aktiv sein, damit kein doppelter Lauf entsteht.

Die konkrete Rotation soll bevorzugt durch die vorhandene Linux-Administration (`logrotate`) erfolgen und nicht durch selbst implementiertes Löschen der ersten Datei-/Logzeilen oder einen manuellen Reset. Für den Anfang ist ein Größenlimit von `10M` ausreichend; `50M` bietet zusätzlichen Puffer für Fehlerberichte. Nach der Rotation sollten beispielsweise fünf bis zehn komprimierte Backups behalten werden. Dadurch bleiben Aufbewahrung, Komprimierung, Größenlimit und Wiederherstellung nach Neustarts transparent administrierbar. Die bestehende detaillierte Zeitstempel-Logdatei kann während der Umstellung als Diagnosekanal erhalten bleiben, ist aber nicht die tägliche Laufübersicht.

### Priorität 4 – Batteriespeicher-Simulation (nur Konzept, später implementieren)

**In diesem Arbeitsschritt keine Batteriesimulation, keinen Szenario-Prozessor, keine Szenario-Measurements, keine Wirtschaftlichkeitsberechnung, kein Grafana-Dashboard und keine Batterieoptimierung bauen.** Erst beginnen, wenn Wärmepumpen-Auswertung und zeitliche Energiebilanz belastbar sind.

Geplanter Datenfluss:

```text
Home Assistant
      ↓
InfluxDB-Rohdaten
      ↓
Datenaufbereitung und Qualitätsprüfung
      ↓
zeitlich ausgerichtete Energiebilanz
      ↓
Batteriesimulation in Python
      ↓
versionierte Szenario-Ergebnisse
      ↓
InfluxDB
      ↓
Grafana
```

Vor der Simulation müssen PV-Leistung/-Erzeugung, Gesamtverbrauch, Netzbezug, Einspeisung, Batterie-SOC, Lade- und Entladeleistung, Wärmepumpenverbrauch und relevante weitere Verbraucher auf gemeinsame Zeitintervalle gebracht und Einheiten/Vorzeichen bestätigt werden. `elektrischer_verbrauch` ist dafür wegen des Quellfehlers ungeeignet; die Hauslast muss aus validierten Flussmessungen und einer bestätigten Systemgrenze rekonstruiert oder über eine geeignete alternative Messung bezogen werden. Die real gemessene Batterie soll als Referenz zur Kalibrierung und Validierung dienen; Baseline und alternative Speicher sollten mit derselben Modelllogik simuliert werden, damit Szenarien vergleichbar sind. Die Betriebsstrategie des realen Speichers muss dabei so gut wie möglich berücksichtigt werden.

**PV-Szenarien für die beiden alten Anlagen:** Die Inventur findet `mt_stall_alt_1_leistung_ac_fixed` und `mt_stall_alt_2_leistung_ac_fixed` (beide `W`) sowie `mt_stall_neu_leistung_ac_fixed` (`W`). Laut Nutzer wird die alte Anlage derzeit vollständig eingespeist und lädt die Batterie nicht. Szenarien müssen deshalb mindestens zwei klar getrennte Varianten rechnen: (A) heutiger Betrieb mit alter PV als fest eingespeister Erzeugung, ohne lokale Nutzung/Ladung durch diese Anlagen; (B) Vergleich ohne `mt_stall_alt`-Erzeugung. Variante B darf nicht so interpretiert werden, als wäre die alte PV für Eigenverbrauch verfügbar; zusätzlich muss der entgangene Einspeiseerlös in den Wirtschaftlichkeitsvergleich eingehen. Eine mögliche spätere Änderung der Verschaltung, bei der alte PV lokal genutzt oder zum Laden verfügbar wäre, ist ein eigenes drittes Szenario und muss durch reale Anlagenkonfiguration gestützt sein. `fems_productiondcactualpower` und `mt_stall_neu_leistung_ac_fixed` sind zunächst Kandidaten für lokal verfügbare PV, aber ihre Systemgrenze und tatsächliche Kopplung zur Batterie sind vor Nutzung zu bestätigen.

Szenarioparameter: nutzbare Speicherkapazität, maximale Lade- und Entladeleistung, Lade-/Entladewirkungsgrad, minimale/maximale SOC-Grenze, Betriebsstrategie, Strompreis, Einspeisevergütung und Investitionskosten. Modellversion und Parameter pro Lauf dauerhaft nachvollziehbar speichern.

Zu erzeugende Kennzahlen: Lade-/Entladeenergie und Verluste, SOC, zusätzlicher Eigenverbrauch, vermiedener Netzbezug, zusätzliche Einspeisung, wirtschaftlicher Nettoeffekt gegenüber der Baseline, Stromkosten, Einspeiseerlöse und Amortisationsdauer. Wirtschaftlichkeit muss Nutzen aus vermiedenem Netzbezug gegen entgangene Einspeisevergütung, Verluste und Investitionskosten abwägen. Eine größere Batterie ist nicht automatisch sinnvoll: zusätzliche Kapazität zählt nur, wenn sie tatsächlich geladen und die gespeicherte Energie später genutzt werden kann.

Python soll Zeitreihenaufbereitung, Simulation und reproduzierbare Intervallberechnungen übernehmen. InfluxDB eignet sich für Zeitreihen und Grafana-Abfragen; Szenarioparameter/Laufmetadaten müssen zusätzlich versioniert nachvollziehbar bleiben. Grafana soll Auswahl, Aggregation und Szenariovergleich übernehmen, keine komplexe Batteriephysik.

### Priorität 5 – MQTT-Status für Home Assistant

Nach der Stabilisierung von Logging und Deployment soll das Skript seinen Laufstatus zusätzlich per MQTT an Home Assistant melden. Die MQTT-Verbindung dient als Betriebsinformation und darf den eigentlichen InfluxDB-Verarbeitungslauf nicht unnötig blockieren oder bei einem MQTT-Ausfall als alleinige Ursache den fachlich erfolgreichen Lauf nachträglich als fehlgeschlagen darstellen.

1. **Laufstatus veröffentlichen.** Vor dem Start, während der Verarbeitung und nach dem Abschluss werden kompakte Statusinformationen veröffentlicht. Der Abschluss enthält mindestens `SUCCESS` oder `ERROR`, Startzeit, Endzeit beziehungsweise Dauer und die Anzahl verarbeiteter Tage. Ein Lauf ohne neue Tage wird als erfolgreicher Lauf mit `0` Tagen gemeldet.
2. **Fehler verständlich melden.** Bei Problemen werden der betroffene Verarbeitungsschritt, die Fehlerklasse und eine kurze, bereinigte Fehlermeldung übertragen. Zugangsdaten, Tokens, vollständige Konfigurationen, Query-Inhalte und Rohmessdaten dürfen nicht in MQTT-Nachrichten erscheinen.
3. **Home-Assistant-Entitäten bereitstellen.** Die Topics sollen sich für MQTT-Sensoren und eine Laufstatus-Anzeige in Home Assistant eignen, zum Beispiel für Status, letzte erfolgreiche Ausführung, letzte Ausführung, Dauer, verarbeitete Tage und letzte Fehlermeldung. Die MQTT Discovery kann genutzt werden, damit die Entitäten ohne umfangreiche manuelle YAML-Konfiguration angelegt werden.
4. **Verfügbarkeit und retained Zustände.** Der aktuelle Abschlussstatus sowie die letzten bekannten Kennzahlen sollen mit Bedacht als retained Nachrichten veröffentlicht werden. Zusätzlich soll ein Last-Will-/Availability-Topic anzeigen, ob der Prozess beziehungsweise die MQTT-Verbindung verfügbar ist. Veraltete Fehlermeldungen dürfen nicht dauerhaft als aktueller Fehler erscheinen, wenn ein späterer Lauf erfolgreich war.
5. **Logübermittlung begrenzen.** Für Diagnosezwecke darf ein gekürzter, bereinigter Logauszug über ein eigenes Topic übertragen werden. Das vollständige technische Log und unbegrenzte Tracebacks gehören nicht in eine einzelne MQTT-Nachricht. Nachrichtengröße, Aufbewahrung und Übertragungsfrequenz müssen begrenzt werden; Secrets und sensible InfluxDB-Inhalte sind vor der Übertragung zu entfernen.
6. **Verbindung und Sicherheit konfigurieren.** Broker-Adresse, Port, TLS, Benutzername, Passwort, Client-ID, Topic-Präfix und Discovery-Präfix werden ausschließlich über sichere Umgebungsvariablen oder eine geschützte lokale Konfiguration bereitgestellt. Passwörter und Tokens dürfen weder im Repository noch in Logs oder MQTT-Payloads stehen. Die Verbindung muss Timeouts, Wiederverbindung und eine saubere Trennung zwischen Entwicklungs- und Produktionspräfix unterstützen.
7. **Fehlerisolierung und Tests.** MQTT-Fehler werden separat protokolliert und nachvollziehbar gemeldet. Die fachliche Erfolgs-/Fehlerzeile in `runs.log` bleibt maßgeblich für den Prozessstatus. Zu testen sind erfolgreiche Läufe, Läufe ohne neue Tage, Verarbeitungsfehler, MQTT-Broker-Ausfall, Wiederverbindung, retained Status, Discovery-Payloads, Last-Will/Availability, Payload-Bereinigung und abgeschnittene Logauszüge.
