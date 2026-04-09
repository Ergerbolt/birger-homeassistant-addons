# WNT/Deye Block Monitor over Modbus TCP

Dieses Add-on liest einen festen Modbus-TCP-Datenblock von einem WNT/Deye-artigen BMS oder Gateway und veroeffentlicht die daraus dekodierten Live-Daten per MQTT Discovery an Home Assistant.

Die Feldzuordnung basiert auf dem dekompilierten Windows-Tool `BMSTool` und ist damit deutlich naeher an der Original-Registerkarte als die fruehere reine Heuristik.

## Voraussetzungen

- Ein erreichbarer Modbus-TCP-Endpunkt
- Ein Geraet, das auf `Read Holding Registers` mit einem festen 254-Byte-Block antwortet
- MQTT Broker in Home Assistant, z. B. Mosquitto

## Add-on-Optionen

- `mqtt_server`: meist `core-mosquitto`
- `mqtt_user`, `mqtt_pass`: Zugangsdaten fuer MQTT
- `device`: Hostname oder IP des Modbus-TCP-Endpunkts, z. B. `10.0.0.135`
- `device_id`: frei waehlbare Geraetekennung
- `cells_in_series`: z. B. `16`
- `poll_interval_seconds`: z. B. `10`
- `modbus_port`: meist `502`
- `modbus_unit_id`: Standard in diesem Add-on `81`
- `modbus_start`: Startregister, standardmaessig `0`
- `modbus_count`: Anzahl Register, standardmaessig `127`
- `socket_timeout`: Socket-Timeout in Sekunden, z. B. `3`

## Verhalten

- Das Add-on liest pro Zyklus genau einen Block per Modbus TCP.
- Es publiziert rohe Hex-Daten und dekodierte Register aus dem `0x00..0x7E`-Live-Block.
- Zellspannungen und NTC-Temperaturen werden direkt aus der Registerkarte gelesen.
- Mehrere Status- und Diagnosewerte werden als Sensoren oder Binary-Sensoren in Home Assistant angelegt.

## Verfuegbare Daten

Unter anderem werden jetzt folgende Werte veroeffentlicht:

- SOC, Packspannung, Strom, Leistung
- Berechnete Restladung in Ah und direkte Restkapazitaet aus dem BMS
- Einzelzellspannungen, Min/Max/Differenz, aktive Balance-Zellen
- Temperatur 1 bis 8 sowie Max/Min/Temperaturdifferenz
- Charge-, Discharge-, Precharge-, Heat- und Fan-MOS als Binary-Sensoren
- MOS-, Board- und Heater-Temperatur
- Backup Current, Limit Current, Cycle Count, BMS Life
- DI/DO, Wakeup Source, Battery Status, Charge Detect, Load Detect
- AFE Current, AFE Factor, AFE Offset, AFE ADC, PWM Duty, PWM Voltage
- Aktive Alarme inklusive Alarmliste als MQTT-Attribute

## Installation

1. Dieses Repository als eigenes Add-on-Repository in Home Assistant hinzufuegen.
2. Das Add-on `WNT/Deye Block Monitor over Modbus TCP` installieren.
3. Optionen setzen.
4. Add-on starten.
5. MQTT Discovery erzeugt die Entitaeten automatisch.
