# Daly WNT BMS MQTT Bridge (Waveshare RS485/TCP)

Dieses Add-on verbindet ein Daly WNT Board ueber RS485 mit Home Assistant.
Die Verbindung laeuft ueber einen Waveshare RS485-to-RJ45 Ethernet Converter (TCP/IP zu Serial).
Das Add-on liest den festen Live-Datenblock per Modbus TCP und publiziert die dekodierten Werte per MQTT Discovery.

Hinweis: Kein Deye-Inverter-Addon. Der Wechselrichter ist hier nicht beteiligt.

## Hardware-Setup

- Daly WNT Board (RS485)
- Waveshare RS485 to RJ45 Ethernet Converter Module (Industrial Rail-Mount Isolated RS485 Serial Server, TCP/IP to Serial)
- Home Assistant mit MQTT Broker (z. B. Mosquitto)

## Add-on Optionen

- `mqtt_server`: meist `core-mosquitto`
- `mqtt_user`, `mqtt_pass`: MQTT Zugangsdaten
- `mqtt_client_id`: eindeutiger MQTT Client Name
- `mqtt_discovery_prefix`: in der Regel `homeassistant`
- `device`: IP/Hostname des Waveshare TCP Moduls, z. B. `bms.local`
- `device_id`: frei waehlbare BMS Kennung in Home Assistant
- `cells_in_series`: Anzahl Zellen in Serie, z. B. `16`
- `nominal_capacity_ah`: optional fuer berechnete Restladung
- `poll_interval_seconds`: Abfrageintervall
- `modbus_port`: meist `502`
- `modbus_unit_id`: Standard `81`
- `modbus_start`: Standard `0`
- `modbus_count`: Standard `127`
- `socket_timeout`: Timeout fuer TCP Verbindungen
- `debug_logging`: `true` fuer sehr ausfuehrliche Logs, `false` fuer normale Logs (empfohlen)

## Was publiziert wird

- Zellspannungen je Zelle, Min/Max/Diff, aktive Balance-Zellen
- SOC, Packspannung, Strom, Leistung, Restkapazitaet
- Temperaturen (1..8), MOS/Board/Heat Temperaturen
- MOS Zustaende (Charge/Discharge/Precharge/Heat/Fan)
- Alarmdaten (`error_bytes`) und erweiterte Faults (`new_fault_bytes`)
- Weitere Diagnosewerte wie AFE, DI/DO, Wakeup Source, Battery Status

## Installation

1. Repository in Home Assistant als Add-on Repository hinzufuegen
2. Add-on `Daly WNT BMS MQTT Bridge (Waveshare RS485/TCP)` installieren
3. Optionen konfigurieren
4. Add-on starten
5. Entitaeten erscheinen automatisch ueber MQTT Discovery
