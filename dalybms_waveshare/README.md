# WNT/Deye Block Monitor over Modbus TCP

Dieses Add-on liest einen festen Modbus-TCP-Datenblock von einem WNT/Deye-artigen BMS oder Gateway und veroeffentlicht die aus dem Block abgeleiteten Kandidatenwerte per MQTT Discovery an Home Assistant.

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
- Es publiziert rohe Hex-Daten sowie mehrere heuristische Kandidatenwerte.
- Zellspannungen werden aus den ersten Worten des Blocks abgeleitet.
- SOC, Spannung, Strom, Temperatur und Status sind aktuell als Kandidaten bzw. Rohwerte zu verstehen und koennen je nach Geraet noch Feintuning brauchen.

## Installation

1. Dieses Repository als eigenes Add-on-Repository in Home Assistant hinzufuegen.
2. Das Add-on `WNT/Deye Block Monitor over Modbus TCP` installieren.
3. Optionen setzen.
4. Add-on starten.
5. MQTT Discovery erzeugt die Entitaeten automatisch.
