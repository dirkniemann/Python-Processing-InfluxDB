# Python-Auswertung für Home Assistant und InfluxDB

Dieses Repository verarbeitet Home-Assistant-Zeitreihen aus InfluxDB 2.x. Es bereinigt Wärmepumpenzähler, erstellt Wärmepumpenstatistiken und einen korrigierten Hausverbrauch. Zusätzlich enthält es eine konfigurationsgesteuerte Batteriesimulation. Betriebsergebnisse werden in InfluxDB, Logs und – in prod – über MQTT an Home Assistant gemeldet.

**Status:** Die bestehende Wärmepumpenverarbeitung und der MQTT-Laufstatus sind implementiert. Die Batteriesimulation ist technisch implementiert, aber fachlich noch nicht für den Produktivbetrieb freigegeben. In prod sind alle Batterieszenarien deaktiviert.

## Überblick

| Bereich | Implementierung | Ausgabe |
| --- | --- | --- |
| Wärmepumpen-Tageszähler | Fehlende bzw. verschobene Tagesresets korrigieren und Tageskurven monotonisieren | Measurement fix_waermepumpe_stromverbrauch |
| Tagesaggregate | Bereinigte Zählerstände pro Tag und zusammengefasster Gesamtwert | Measurement Waermepumpe_statistik, Feld daily_sum |
| Wärmepumpenstatistik | Pumpenenergie zeitlich aufteilen und ein gemeinsames positives Netzbudget höchstens einmal zuweisen | Measurement Waermepumpe_statistik; Intervall- und Tageswerte |
| Korrigierter Hausverbrauch | FEMS-Hausverbrauch und MT-Stall-neu-Leistung zeitgewichtet auf ein konfigurierbares Intervall mitteln und addieren | Measurement Hausverbrauch_korrigiert |
| Batterieszenarien | Sample-and-hold-Eingänge, idealisiertes Speichermodell, Tagesfortschreibung und Wiederanlauf | battery_scenario_timeseries und battery_scenario_daily |
| Laufstatus | Laufzeit, Ergebnis, Warnungen und Diagnose über MQTT Discovery | Retained Sensoren im Home-Assistant-Dashboard |

Die normalen Prozessoren werden unter src/moduls/processing/ konfiguriert. Die Batteriesimulation liegt unter src/moduls/szenarios/. src/main.py steuert den Lauf und src/moduls/influxdb_handler.py kapselt InfluxDB-Abfragen und Schreibzugriffe.

## Verarbeitungsablauf

1. main.py lädt Stage-Konfiguration und .env, richtet Logging und MQTT ein und verbindet sich mit InfluxDB.
2. Verarbeitungstage sind vollständige lokale Kalendertage in Europe/Berlin; der aktuelle Tag wird nicht verarbeitet.
3. HomeAssistantProcessor führt die unter processing.entities_to_process konfigurierten Prozessoren der Reihe nach aus.
4. Wenn scenarios in der Konfiguration vorhanden ist, startet der Szenario-Runner. Sind keine Definitionen aktiviert, protokolliert er dies und überspringt die Simulation.
5. Der Lauf wird mit Status, Laufzeit und Diagnose abgeschlossen; InfluxDB und MQTT werden getrennt.

Die Tagesverarbeitung ist inkrementell: Prozessoren suchen den letzten geschriebenen Tag ihrer Ausgabeversion und arbeiten danach bis gestern weiter. Tagesgrenzen werden als Europe/Berlin nach UTC umgerechnet. Die allgemeinen Prozessoren bestimmen „heute“ allerdings über die lokale Systemzeitzone; das Serversystem muss deshalb aktuell auf Europe/Berlin laufen. Die Szenarioverarbeitung verwendet die Berliner Zeitzone explizit.

## Daten und fachliche Grenzen

### Wärmepumpenstatistik

Die beiden Wärmepumpenzähler werden anhand der Kompressorsignale und des konfigurierbaren Nachlaufs (aktuell 15 Minuten) zeitlich gewichtet. Positive Werte von fems_gridactivepower bilden ein gemeinsames Netzbudget. Dieses Budget wird nicht doppelt auf beide Pumpen verteilt. Der ausgewiesene PV-Anteil ist ein rechnerischer Rest, kein direkt gemessener PV-Stromfluss.

Die Konfiguration benennt Sensorrollen ausdrücklich. Die Quellen müssen in InfluxDB mit exakt passendem, case-sensitive Measurement, Entity, Field und Unit vorhanden sein.

### Korrigierter Hausverbrauch

Der konfigurierte Rechenweg lautet:

    zeitgewichteter Mittelwert von elektrischer_verbrauch

Der bereits mit MT-Stall-neu korrigierte Sensor wird als Change-only-Leistungswert behandelt: Der zuletzt bekannte Wert gilt bis zur nächsten Änderung; ein Zustand vor Tagesbeginn wird geladen, falls vorhanden. Die Leistung wird energieerhaltend zeitgewichtet auf gemeinsame Intervalle gemittelt (dev/prod: 300 Sekunden, über `interval_seconds` konfigurierbar). Negative Intervallmittelwerte werden auf null begrenzt. MT-Stall-neu wird nicht erneut zum Hausverbrauch addiert. Ausgegeben wird der Mittelwert mit dem Zeitstempel des Intervallstarts. Die Korrektur läuft in Version v4; die Batterieszenarien lesen ebenfalls v4.

### Batteriesimulation

Der Runner verarbeitet jede aktivierte Szenariodefinition mit jedem konfigurierten PV-Modus. Er schreibt Leistungs- und SOC-Zeitreihen sowie lokale Tagesenergien. Tageszustände, Eingabefingerprints und Vollständigkeit werden gespeichert. Bei geänderten historischen Eingaben beginnt eine neue Simulationsversion; der SOC wird ab dem ältesten betroffenen Tag chronologisch neu berechnet.

Das V1-Modell ist eine transparente Idealisierung ohne zusätzliche Lade-/Entladeverluste. PV deckt zuerst die Last, Überschüsse laden den Speicher, verbleibende Überschüsse werden exportiert; bei Lastdefizit entlädt der Speicher bis zu seinen Grenzen, der Rest wird importiert. Das Modell bildet die konfigurierten DC-Leistungsgrenzen ab, ist aber keine kalibrierte Prognose und keine vollständige AC-Bilanz.

Aktuell konfigurierte Annahmen sind 22,4 kWh Grundkapazität, 17,92 kW Lade- und Entladeleistung, 5–100 % SOC und 5 % Start-SOC für eine neue Simulationsversion. PV-Modi sind without_old_pv und with_old_pv. Prod schreibt in den konfigurierten Bucket Szenarios; dev schreibt nach testing.

Die reale Batterie wird zusätzlich pro neu simuliertem Tag für `current_battery` bewertet, wenn `scenarios.quality.enabled` aktiv ist. Unter den Tages-Entity-IDs `soc_pct`, `grid_import` und `grid_export` werden `quality` und `signed_error` gespeichert. `quality` ist der zeitgewichtete SOC-MAE in Prozentpunkten beziehungsweise der absolute Import-/Exportfehler in kWh; `signed_error` ist Simulation minus Realität. Die realen Grid-Import- und Exportenergien aus `fems_gridactivepower` werden für den Vergleich integriert, aber nicht gespeichert. Ohne gültige Zustände an der lokalen Tagesgrenze wird die Bewertung für den Tag ausgelassen. Der Schalter ist in dev aktiviert und in prod deaktiviert. Die separate Auswertung von gemessener Batterie-DC-Leistung und SOC bleibt diagnostisch; widersprüchliche Lade-/Entladeresiduen werden nicht als Wirkungsgrad-Korrekturfaktor verwendet.

## Konfiguration

Stage-Dateien sind config/dev.json und config/prod.json. main.py akzeptiert außerdem test; dafür muss config/test.json angelegt sein. Wichtige Abschnitte:

- processing: Eingabe- und Ausgabe-Bucket sowie Prozessorversionen, Quellen und Ziel-Measurements.
- scenarios: Buckets, Modellgrenzen, Quellen, PV-Gruppen/-Modi und aktivierte Speicherdefinitionen.
- mqtt: Broker, Discovery-Präfix, Topics, Timeouts und Schalter.

| Stage | MQTT | Verarbeitungsausgabe | Batterieszenarien |
| --- | --- | --- | --- |
| dev | deaktiviert | testing | alle vier Definitionen aktiviert |
| prod | aktiviert | HomeAssistant_processed | alle Definitionen deaktiviert |
| test | config/test.json erforderlich | stageabhängig | stageabhängig |

InfluxDB-Anmeldedaten müssen als Umgebungsvariablen bereitstehen:

    INFLUX_URL=http://influxdb:8086
    INFLUX_TOKEN=...
    INFLUX_ORG=...

MQTT verwendet nur dann Benutzername und Passwort, wenn der Broker sie verlangt. Die Variablennamen sind über mqtt.username_env und mqtt.password_env konfiguriert; die Vorgaben sind MQTT_USERNAME und MQTT_PASSWORD. Zugangsdaten gehören nicht in die JSON-Dateien oder ins Repository. .env wird aus dem Repository-Stamm geladen und ist ignoriert.

## Lokal starten

Voraussetzung: Python 3.10 oder neuer und Zugriff auf die benötigte InfluxDB. Die produktive Installation nutzt Python 3 und InfluxDB-Client/MQTT-Abhängigkeiten aus requirements.txt.

Windows PowerShell:

    py -m venv venv_dev
    .\venv_dev\Scripts\Activate.ps1
    python -m pip install -r requirements_dev.txt
    python src/main.py --stage dev

Linux:

    python3 -m venv venv
    . venv/bin/activate
    python -m pip install -r requirements.txt
    python src/main.py --stage dev

CLI-Optionen:

- --stage {dev,test,prod} wählt config/<stage>.json; Standard ist dev.
- --log-level {DEBUG,INFO,WARNING,ERROR} überschreibt die Stage-Vorgabe.
- --log-file <Pfad> setzt eine eigene Logdatei.

Der Einstiegspunkt für den direkten Aufruf ist python src/main.py. Vor einem produktiven Lauf Stage-Konfiguration, Bucketnamen und Zugangsdaten prüfen: ein Lauf schreibt in die dort angegebenen InfluxDB-Buckets.

## Tests

Tests starten aus dem Repository-Stamm:

    python -m pytest

In der Windows-Entwicklungsumgebung:

    .\venv_dev\Scripts\python.exe -m pytest

Die Tests verwenden Fakes für InfluxDB und MQTT; sie prüfen keine echte Broker-/InfluxDB-Verbindung und ersetzen keinen Abgleich gegen reale Messdaten.

## Logs und Laufstatus

Detaillierte Logs werden in dev/test standardmäßig im Verzeichnis logs/ und in prod unter /var/log/Python_Auswertung/ abgelegt. Alte timestamp-basierte Logdateien werden nach 30 Tagen entfernt. Jeder Python-Lauf versucht zusätzlich, eine kompakte Zeile an /var/log/Python_Auswertung/runs.log anzuhängen. Dieser Pfad ist derzeit fest im Code hinterlegt und damit vor allem für das Linux-Produktivsystem passend. Im Deployment rotiert logrotate diese Datei ab 10 MB und behält zehn Archive.

Python-interne Ergebniswerte sind:

| Interner Status | Bedeutung |
| --- | --- |
| SUCCESS | Ohne Warnungen und fatalen Fehler beendet |
| SUCCESS_WITH_WARNINGS | Beendet, mindestens eine Python-Logging-Warnung aufgetreten |
| ERROR | Fataler Fehler oder behandelter Abbruch, einschließlich Ctrl+C |

Home Assistant erhält über MQTT Discovery deutsche Werte: Ergebnis Erfolgreich, Erfolgreich mit Warnungen oder Fehler; Laufzustand Läuft oder Leerlauf. Das Dashboard in docs/home_assistant_dashboard.yaml färbt Erfolg grün, Warnungen orange, Fehler rot und einen laufenden Job grün. Diagnose letzter Lauf zeigt gekürzte Warnungsbeispiele oder den Fehler samt Verarbeitungsschritt. Laufzeit, letzter erfolgreicher Lauf und letzter Verarbeitungstag bleiben erhalten.

Die Statuswerte werden retained übertragen. Das Availability-Topic bleibt zwischen Cron-Läufen online, obwohl der Python-Prozess nach jedem Lauf beendet wird. Ctrl+C wird regulär behandelt und mit Fehlerstatus und Abbruchdiagnose veröffentlicht. Bei einem harten Prozessabbruch setzt der MQTT-Last-Will den Ergebnisstatus auf Fehler; der letzte Laufzustand kann dabei retained auf Läuft stehen, und der Last-Will kann keine neue Diagnose erzeugen.

## Produktivdeployment (Debian/Ubuntu)

Vorhandene Deployment-Dateien:

- cron.d_influx_job: täglicher Start um 04:00 Uhr mit flock.
- deploy/launcher/python-auswertung-launcher: git pull --ff-only origin master, Logrotate-Prüfung und Start des Jobs.
- run_script.sh: aktiviert /opt/Python-Processing-InfluxDB/venv, installiert geänderte Requirements und startet src/main.py --stage prod.
- deploy/logrotate/python-auswertung: Rotation von runs.log.

Der Launcher läuft im Beispiel als root und aktualisiert den Checkout vor jedem Lauf. Vor Installation müssen Repositorypfad, Remote/Branch (origin master), Verzeichnisrechte, .env, MQTT-Broker und Cron-Zeile zum Zielsystem passen. Launcher-Fehler vor dem Python-Start werden in runs.log geschrieben; sie können nicht vom Python-MQTT-Last-Will gemeldet werden.

## Hilfswerkzeuge

- tools/influx_audit/inventory.ps1 ist eine read-only Bestandsprüfung für InfluxDB. Die Optionen und Grenzen stehen in tools/influx_audit/README.md. Das Skript liest ausdrücklich die Zugangsdaten aus der Repository-Datei .env_agent und unterdrückt Credential-Werte in der Ausgabe.
- infludxdb_delete.py löscht InfluxDB-Punkte anhand von Bucket, Measurement, Entity, Version und Zeitfenster. Standardziel ist der Bucket testing und das Measurement Waermepumpe_statistik. Ohne --all baut das Skript eine eingegrenzte Predicate-Abfrage; mit --all löscht es alle Daten im Zielbucket innerhalb des Zeitfensters. Die Optionen vorher prüfen und nicht mit --all gegen einen produktiven Bucket ausführen.

## TODO

### Prio 1
- Die Qulitätskontrolle der current Batterei überarbeiten. Aktuell wird um 23 Uhr ein error quality wert geschirbeen bei soc_pct und bei grid_import und grid_export. das muss auf den tagesende, wie ich das bei den anderen tageswerten auch schon machen, genauso der signed_error
- ergänze noch ein error signal, was bei allen 3 die differenzen direkt ist, also ein grpah nachher ist und nicht nur die summe
- der fesm_gridactivepower ist in W, die grid_import und grid_export geschichte ist in kW. korrigiere das, mache das auch in W, damit das sauber zu meinem rest passt. kontrolliere auch, wo ich noch solche ungenauigkeiten habe und die daten der simulation nicht zu den rohdaten passen, also die einheiten unterschiedlich sind
- solche fehler sollten nicht zum skippen führen:
2026-09-28 20:34:19 - moduls.szenarios.scenarios_processor - WARNING - Skipping daily quality for 2025-07-23: invalid battery_soc sample on2025-07-23 13:09:48.406547+00:00
2026-09-28 20:34:20 - moduls.szenarios.scenarios_processor - WARNING - Skipping daily quality for 2025-07-23; no valid local-midnight statefor source(s): battery_soc
mache die qulitätsberechnung stabil. es ist doch egal, ob es um mitternacht daten gibt
## Prio 2
- kontrolliere meinen ganzen code auf Logikfehler und dokumenteire die, fixe die erstmal noch nicht, dokumenteire es in der readme oder eine zusätzlichen datei
- kontrolliere den code auf best practice und fehlerhadnling, wo sorgt eine exception nicht sauber für ein abbruch und so, wo sollte noch eine log mehr entstehen, das kannst du direkt fixen


### Vor einem aussagekräftigen DEV-Szenariolauf

- [ ] Tagesbestimmung der normalen Prozessoren und der Simulation vereinheitlichen oder die erforderliche Systemzeitzone Europe/Berlin beim Start prüfen. Aktuell verwendet ein Teil der Pipeline die Host-Zeitzone, während die Szenarios sie explizit setzt.

### Vor Aktivierung der Batterie in prod

- [ ] Nach DEV-Abnahme entscheiden, welche Szenarien in config/prod.json aktiviert werden. Aktuell ist enabled für alle vier Definitionen false; die Simulation produziert daher in prod keine Szenarioausgabe.
- [ ] V1-Verlustannahme und AC/DC-Grenzen vor der Interpretation der Kennzahlen sichtbar dokumentieren; Simulationsergebnisse sind bis dahin relative, idealisierte Schätzungen.
- [ ] Reale Batterie-Diagnose mit belastbareren Energie-/SOC-Daten und längeren Zeiträumen prüfen. Die aktuelle DC-Leistung-/SOC-Auswertung ist diagnostisch; widersprüchliche Residuen erlauben keinen Wirkungsgrad-Korrekturfaktor.
- [ ] Dashboard-/Grafana-Auswertung der Measurements battery_scenario_timeseries und battery_scenario_daily anhand eines fachlich geprüften Laufs ergänzen.

### Betrieb und Wartbarkeit

- [ ] MQTT-Verhalten bei hartem Abbruch verbessern oder dokumentieren: Last-Will setzt nur das Ergebnis auf Fehler; Laufzustand bleibt Läuft, und Diagnose bleibt beim letzten regulär veröffentlichten Text.
- [ ] Den fest hinterlegten Pfad für runs.log konfigurierbar machen oder nach Stage trennen. Ein lokaler dev/test-Lauf kann sonst an fehlenden Rechten auf /var/log/Python_Auswertung scheitern; der Summary-Fehler wird aktuell selbst als fataler Laufabschluss behandelt.

## Repository-Orientierung

- src/main.py – CLI und Lebenszyklus des Processing-Laufs.
- src/moduls/influxdb_handler.py – InfluxDB-Zeitabfragen, Tagesgrenzen, Versionsabfragen und Writer.
- src/moduls/processing/ – Tageszähler, Aggregate, Wärmepumpenstatistik und korrigierter Hausverbrauch.
- src/moduls/szenarios/ – Batterieparameter, idealisierte Engine, Eingabevalidierung, Wiederanlauf und Measurements.
- src/moduls/mqtt_status.py – MQTT Discovery, Retain, Last-Will, Verfügbarkeits- und Ergebnisstatus.
- config/ – Stage-Konfiguration.
- docs/ – Home-Assistant-Dashboard und Anlagen-/Modbus-Unterlagen.
- deploy/ sowie cron.d_influx_job und run_script.sh – Linux-Betrieb.
- tests/ – Unit- und Komponentenprüfungen.
- tools/influx_audit/ – read-only Inventurhilfe für InfluxDB.

Zuletzt lokal geprüfter Stand: 72 Tests bestanden. Tests bei Code- oder Konfigurationsänderungen erneut ausführen.
