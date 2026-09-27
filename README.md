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
- Integrate Fronius inverter data into FENECON flow
- Cross-check processed data against Home Assistant for consistency
- Die priorisierten nächsten Schritte für Wärmepumpen-Auswertung und Batteriespeicher-Konzept stehen im Abschnitt [Aktueller Stand und To Do](#aktueller-stand-und-to-do).

## Aktueller Stand und To Do

Dieser Abschnitt ist die Arbeitsanweisung für die nächste Umsetzung. **Derzeit wurde noch keine der hier beschriebenen Codeänderungen umgesetzt.** Read-only InfluxDB-Abfragen über `.env_agent` wurden auf kurze, gezielte Zeiträume begrenzt; die folgenden Einheiten, Punktzahlen und Zeitabstände sind Bestandsindikatoren, keine vollständige Validierung der Gerätesemantik.

### Ist-Stand der Wärmepumpen-Auswertung

- `fix_waermepumpe_stromverbrauch` bereinigt die Tageszählerstände beider Wärmepumpen und schreibt sie als `value` in kWh.
- `Waermepumpe_statistik` verarbeitet die zwei Zähler und `fems_gridactivepower`. Die aktuelle Implementierung berechnet den Netzanteil für jede Wärmepumpe unabhängig. Dadurch kann die Summe beider Zuordnungen größer als der gesamte Netzbezug werden.
- Die aktuelle Berechnung behandelt Grid-Werte als Watt und teilt durch 1000. Diese Annahme ist nicht durch Code oder Konfiguration abgesichert.
- Die Statistik interpretiert die Rollen über Listenpositionen: Einträge 0 und 1 sind die Pumpen, Eintrag 2 ist das Netzsignal.
- Kompressor-Binary-Sensoren werden aktuell nicht abgefragt oder verwendet.
- Die bestehende `pv_contribution` ist ein rechnerischer Rest nach Abzug des geschätzten Netzanteils. Sie ist kein direkt gemessener PV-Stromfluss zur Wärmepumpe.

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

**Vor Beginn der Prio-1-Implementierung gezielt noch klären:** (1) Grid-Leistungsvorzeichen und genaue Messpunkt/Systemgrenze anhand Gerätemetadaten bestätigen; (2) ob `fems_productiondcactualpower` DC-Generatorleistung und nicht AC-seitige PV-Leistung am Hausbus ist; (3) Bedeutung, AC/DC-Seite, Vorzeichen und Systemgrenze von `mt_stall_neu_leistung_ac_fixed` sowie beiden Alt-Anlagen verifizieren; (4) exakte SOC-Measurement-Zeichenfolge `„%“`; (5) ob Grid-, PV- und Batterie-Leistungsreihen regelmäßig periodisch schreiben oder nur bei Änderungen und wie lange Werte fortgeschrieben werden dürfen; (6) ob lange Zählerabstände echte Recorder-Lücken, seltene Zähleränderungen oder beides sind. `elektrischer_verbrauch` ist laut Nutzer bereits an der Quelle fehlerhaft und teilweise negativ: ihn nicht zur Lastberechnung verwenden und nicht versuchen, negative Werte einfach auf null zu begrenzen. Für Change-Only-Kompressoren ist die Ereignisfreiheit laut Nutzer die Zustandssemantik und kein alleiniger Lückenbeweis.

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
- **Test-Bucket und Berechnungsversion:** Für Integrations-/Schreibtests ist ausdrücklich `config/prod.json` als Konfiguration zu laden und zu verarbeiten. Im Test-Bucket liegen Ergebnisse von `v1`; die neue Auswertung soll als `v2` schreiben und anschließend für alle Tage erneut berechnet werden. `v1` muss nicht erhalten oder mit `v2` kompatibel sein und kann bei Bedarf gelöscht werden. Vor jedem Schreibtest sicherstellen, dass das Ziel der Test-Bucket ist. Änderungen am Konfigurationsschema oder an erforderlichen Sensorrollen auch in `config/prod.json` nachziehen. Dieselbe Konfiguration wird später in der Produktivumgebung verwendet und schreibt dort in einen anderen Bucket; produktive Zugangsdaten/Bucket nicht für Tests verwenden.

### Priorität 3 – Batteriespeicher-Simulation (nur Konzept, später implementieren)

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
