# Daly Smart BMS over Waveshare for Home Assistant

Dieses Add-on liest ein Daly Smart BMS über einen Waveshare TCP-zu-Seriell-Wandler aus und veröffentlicht die Werte per MQTT Discovery an Home Assistant.

## Voraussetzungen

- Waveshare im transparenten TCP-Server-Modus
- Seriell: 9600 / 8N1
- Das Daly ist am Waveshare seriell angeschlossen
- MQTT Broker in Home Assistant, z. B. Mosquitto

## Empfohlene Waveshare-Einstellung

- Work Mode: TCP Server
- Device Port: 4196
- Baud: 9600
- Data bits: 8
- Parity: None
- Stop bits: 1
- Protokoll: transparenter TCP-Seriell-Betrieb (kein Modbus-Gateway)

## Add-on-Optionen

- `mqtt_server`: meist `core-mosquitto`
- `mqtt_user`, `mqtt_pass`: Zugangsdaten für MQTT
- `device`: z. B. `socket://10.0.0.135:4196`
- `device_id`: frei wählbare Gerätekennung
- `cells_in_series`: z. B. `16`
- `poll_interval_seconds`: z. B. `2`

## Installation

1. Dieses Repository als eigenes Add-on-Repository in Home Assistant hinzufügen.
2. Das Add-on `Daly Smart BMS over Waveshare` installieren.
3. Optionen setzen.
4. Add-on starten.
5. MQTT Discovery erzeugt die Entitäten automatisch.
