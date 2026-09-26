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

Die folgende Arbeit zuerst und ohne Batterie-Szenariocode umsetzen:

1. **Konfiguration explizit machen.** Die Statistik-Konfiguration soll Rollen statt einer geordneten `entities`-Liste enthalten, zum Beispiel `heat_pump_1`, `heat_pump_2`, `grid_power`, `compressor_1` und `compressor_2`. Pro Rolle mindestens `measurement`, `entity_id`, `field` und erwartete `unit` angeben. Zählerrollen sind als Energie in `kWh`, Netzleistung als explizit festgelegtes `W` oder `kW`, Kompressorstatus als dimensionsloser Zustand mit erlaubten Werten 0/1 zu validieren. Keine Rolle aus einem Listenindex ableiten.
2. **Konfiguration beim Start validieren.** Pflichtrollen, nichtleere Strings, erlaubte Einheiten, eindeutige Sensorreferenzen, gültige Feldnamen und plausible Grenzwerte (z. B. maximale zulässige Signallücke) prüfen. Fehlende oder doppelt belegte Rollen müssen mit verständlicher Fehlermeldung abbrechen. Die bestehende `entities`-Liste für die Zählerbereinigung kann davon getrennt bleiben; die Statistik darf sie nicht zur Rollenzuordnung verwenden.
3. **Einheiten zentral normalisieren.** Rohwerte aus `grid_power` anhand der Konfiguration genau einmal in kW umrechnen. Bei fehlender oder unbekannter Einheit abbrechen. Falls die Influx-Reihe keine Unit-Metadaten enthält, ist die Konfiguration die verbindliche Deklaration; die reale Sensor-Unit muss vor dem produktiven Aktivieren manuell bestätigt werden. Zählerwerte vor Differenzbildung auf kWh prüfen. Kompressorwerte außerhalb 0/1 als Datenfehler markieren und nicht stillschweigend interpretieren.
4. **Zeitachse und Change-Only-Zustände aufbereiten.** Für beide Kompressor-Reihen den letzten Zustand vor Intervallbeginn sowie alle Änderungen im Intervall laden. Den letzten Zustand über beliebig lange ereignisfreie Zeit bis zur nächsten Änderung fortschreiben; bei Change-Only-Reihen ist ein langer Eventabstand für sich keine Datenlücke. `unknown` gilt nur, wenn im verfügbaren Datenbestand kein vorheriger Zustand existiert oder ein Recorder-/Retention-Verlust bekannt ist. Wenn der erste bekannte Messpunkt erst nach Intervallbeginn liegt und kein Vorgänger abgefragt werden kann, bleibt die Zeit davor `unknown`; nicht rückwirkend `1` annehmen. Abfragegrenzen müssen ausreichend Vorlauf für den letzten bekannten Zustand enthalten.
5. **Gemeinsame Zeitintervalle bilden.** Die Zeitpunkte beider bereinigter Zähler, beider Kompressorereignisse und der Netzleistungsmessung als Grenzen verwenden. Das Netzsignal gilt gemäß bestätigter Sensorsemantik bis zum nächsten Messpunkt; bei zu großer Lücke gilt es als unbekannt. Zählerdifferenzen bleiben die primäre gemessene Energie. Da sie nur zwischen zwei Messpunkten bekannt ist, darf eine Verteilung innerhalb dieses Intervalls höchstens als Schätzung ausgewiesen werden.
6. **Gemeinsame, deterministische Zuordnung berechnen.** Je gemeinsamem Teilintervall nur den positiven Netzbezug als verfügbares Netzbudget verwenden; Einspeisung ergibt kein positives Budget. Als Nachfragegewicht je aktiver Wärmepumpe dient deren aus Zählerdifferenz und Intervalllänge abgeleitete mittlere Leistung. Wenn beide Kompressoren laufen, das Netzbudget proportional zu diesen beiden Gewichten aufteilen. Wenn nur eine Pumpe nachweislich läuft, darf nur sie Netzanteil erhalten. Wenn beide aus sind, keine Wärmepumpenenergie ableiten. Immer begrenzen: `WP1_grid + WP2_grid <= max(grid_import, 0)`; außerdem je Pumpe `0 <= grid <= gemessene Pumpenenergie`. Rundung erst nach der Aufteilung, damit sie die Summenbedingung nicht verletzt. Bei gleichen Gewichten erfolgt die Aufteilung gleich; keine feste Pumpenpriorität.
7. **Messung und Schätzung getrennt halten.** Zählerdifferenzen, Netzleistung und Kompressorereignisse sind Eingabemessungen. Bereinigte Zählerwerte bleiben als solche nachvollziehbar. Die zeitliche Zuteilung einer Zählerdifferenz auf feinere Teilintervalle sowie die Pro-rata-Netzaufteilung sind abgeleitete Schätzungen. Ergebnisse benötigen mindestens einen Qualitäts-/Gültigkeitsindikator oder ein zusätzliches Feld für nicht zuordenbare Energie. Eine fehlende Netz- oder Kompressormessung darf nicht als `0` behandelt und der Rest nicht automatisch als bestätigter PV-Anteil ausgegeben werden.
8. **Widersprüche und Lücken explizit behandeln.** Kompressor `0` bei steigenden Zählerständen: gemessenen Zähleranstieg erhalten, Konflikt markieren und nicht als Kompressor-bedingten Verbrauch oder bestätigten PV-Anteil ausgeben. Kompressor `1` ohne Zähleranstieg: keine Energie erfinden; optional lange Laufzeit ohne Zähleränderung als Qualitätswarnung markieren. Lücken über Grenzwert bei Grid, Kompressor oder Zähler: betroffene Zuordnung als ungültig/unbekannt markieren, protokollieren und nicht stillschweigend mit null oder Extrapolation füllen. Ein Tageswert muss erkennen lassen, wenn Teilintervalle ungültig oder unklassifiziert waren.
9. **Datenmodell und Versionsführung prüfen.** Bestehende Feldnamen (`pv_contribution`, `grid_import`, `daily_pv`, `daily_grid_import`) nicht ohne Migrations-/Abwärtskompatibilitätsentscheidung umdeuten. Ggf. `unclassified_energy`, Qualitätsflags und eine neue Berechnungsversion ergänzen. Daily totals müssen aus den tatsächlich geschriebenen Intervallwerten entstehen. Eine erneute Tagesverarbeitung muss idempotent sein oder alte Resultate einer Version gezielt ersetzen.
10. **Tests ergänzen und ausführen.** Die reine Zuordnungsfunktion separat testbar halten. Mindestens prüfen: 5 kW Grid / WP1 3 kW / WP2 0 → WP1 3 kW; 0,5 kW / WP1 4 kW → Grid 0,5 kW und Rest nicht mehr als Grid; 2 kW / 1,5 kW je Pumpe → gemeinsame Summe höchstens 2 kW; 5 kW / 1,5 kW je Pumpe → je 1,5 kW; nur WP1 aktiv; nur WP2 aktiv; beide aktiv mit proportionaler Regel; beide aus → keine Energie; Change-Only-Zustand gilt bis zum nächsten Wechsel; unbekannter Anfangszustand und veralteter Zustand; ungültige Einheit und Sensorrollen unabhängig von Konfigurationsreihenfolge; Datenlücken und widersprüchliche Zähler-/Kompressorsignale. Danach vollständige Testsuite ausführen.

**Interpolation:** Keine lineare Interpolation als vermeintliche Messung speichern. Falls Zählerdifferenzen für die Intervallzuordnung auf Teilintervalle verteilt werden müssen, ist eine konstante mittlere Leistung zwischen zwei gültigen Zählerpunkten eine nachvollziehbare Schätzung. Kompressor-An/Aus-Ereignisse begrenzen diese Schätzung zeitlich, liefern aber selbst keine elektrische Leistung. Die Schätzung muss markiert werden. Vor Implementierung anhand realer Zeitabstände entscheiden, ab welcher Zählerlücke keine Verteilung mehr zulässig ist.

**Vor Implementierung anhand echter Daten klären:** (a) Einheit und Vorzeichen von `fems_gridactivepower`, insbesondere ob positiver Wert Netzbezug bedeutet; (b) tatsächliche Schreibweise beider Binary-Measurements und ob `entity_id`/`_field` wie angegeben vorhanden sind; (c) Units und Updateabstände beider Wärmepumpenzähler; (d) typische/maximale Abstände und eventuelle Retention der drei Signalgruppen; (e) ob Netzsignal tatsächlich den gesamten Haus-Netzfluss abbildet und wie Batterie-Laden/Entladen darin enthalten ist; (f) wie unklassifizierte Energie und widersprüchliche Messungen in Grafana dargestellt werden sollen. Ohne diese Bestätigung bleibt jede Zuordnung eine Schätzung.

### Priorität 2 – Datenqualität und Reproduzierbarkeit

Nach Stabilisierung der Zuordnung:

- Zeitauflösung, Zeitzone, Tagesgrenzen und Intervallsemantik je Signal dokumentieren; Sommerzeitwechsel ausdrücklich berücksichtigen.
- Zähler-Resets und fehlende Tagesgrenzen robust erkennen; keine fehlenden Messwerte ohne Kennzeichnung synthetisch ergänzen.
- Lücken, doppelte/out-of-order Messpunkte, negative Differenzen und stale Zustände mit Qualitätsstatus und nachvollziehbaren Logs behandeln.
- Tagesläufe reproduzierbar und idempotent gestalten. Verarbeitungszeitraum, Eingabestand und Berechnungsversion nachvollziehbar halten.
- Gemessene, bereinigte, abgeleitete und geschätzte Reihen/Felder trennbar benennen und dokumentieren.
- Reale Tagesausschnitte nach Bestätigung der Datensemantik gegen manuelle Plausibilitätsprüfungen und die Energieflussbilanz abgleichen.
- `elektrischer_verbrauch` wegen des bekannten Quellfehlers nicht als Messgrundlage verwenden; Fehlerwerte weder stillschweigend clippen noch als Verbrauch interpretieren. Als unabhängiges Diagnose-Signal nur kennzeichnen, bis die Quelle repariert ist.

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
