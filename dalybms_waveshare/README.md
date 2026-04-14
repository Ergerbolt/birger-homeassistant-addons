# Daly WNT BMS MQTT Bridge (Waveshare RS485/TCP)

Dieses Add-on verbindet ein Daly WNT Board ueber RS485 mit Home Assistant.
Die Verbindung laeuft ueber einen Waveshare RS485-to-RJ45 Ethernet Converter (TCP/IP zu Serial).
Das Add-on liest den festen Live-Datenblock per Modbus TCP und publiziert die dekodierten Werte per MQTT Discovery.
Optional kann es gezielte Modbus-Schreibbefehle per MQTT an das BMS senden.

Hinweis: Kein Deye-Inverter-Addon. Der Wechselrichter ist hier nicht beteiligt.

## Haftungsausschluss

Die Nutzung dieses Add-ons erfolgt auf eigene Gefahr. Schreibzugriffe auf BMS-Parameter koennen Fehlkonfigurationen verursachen und im schlimmsten Fall zu Beschaedigungen von Batterie, BMS oder angeschlossener Hardware fuehren.

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
- `enable_write_commands`: aktiviert MQTT-basierte Modbus-Write-Kommandos (`false` empfohlen als Default)
- `write_command_topic`: optionales MQTT Topic fuer Write-Kommandos (leer = auto)
- `write_result_topic`: optionales MQTT Topic fuer Write-Antworten (leer = auto)
- `write_allowed_registers`: CSV-Registerliste, die geschrieben werden darf (Default `9,10,11,12,33,34,64,65,69,70,265,266,267,268,289,290,320,321,325,326,503,504`)

## Schreibbefehle (optional)

Wenn `enable_write_commands=true` ist, lauscht das Add-on standardmaessig auf:

- Command Topic: `homeassistant/sensor/<device_id>/set/write`
- Result Topic: `homeassistant/sensor/<device_id>/write_result`

Zusaetzlich werden in Home Assistant steuerbare Entitaeten per MQTT Discovery angelegt:

- Switch: `Charge MOS Control` (Register `289`)
- Switch: `Discharge MOS Control` (Register `290`)
- Number: `Rated Capacity` in `Ah` (Register `265/266`, 32-bit)
- Number: `Actual Capacity` in `Ah` (Register `267/268`, 32-bit)
- Number: `Max Charge Current L1/L2` in `A` (Register `320/321`)
- Number: `Max Discharge Current L1/L2` in `A` (Register `325/326`)

Unterstuetzte Formate:

```json
{"register":503,"value":1}
```

```json
{"register":503,"values":[1,0]}
```

Hinweise:

- `value` nutzt Modbus Funktion `0x06` (Single Register Write).
- `values` nutzt Modbus Funktion `0x10` (Multi Register Write).
- Registers ausserhalb von `write_allowed_registers` werden abgewiesen.
- Jede Antwort enthaelt `ok`, `function`, `register`, `values`, `timestamp` und optional `error`.
- Stromgrenzen werden intern wie im BMSTool kodiert (`30000 +/- A*10`).
- Kapazitaet wird intern als `Ah * 1000` im 32-bit Registerpaar geschrieben.
- Fuer unterschiedliche Firmware-Varianten versucht das Add-on automatisch Register mit und ohne `+256` Offset.

## Was publiziert wird

- Zellspannungen je Zelle, Min/Max/Diff, aktive Balance-Zellen
- SOC, Packspannung, Strom, Leistung, Restkapazitaet
- Temperaturen (1..8), MOS/Board/Heat Temperaturen
- MOS Zustaende (Charge/Discharge/Precharge/Heat/Fan)
- Alarmdaten (`error_bytes`) und erweiterte Faults (`new_fault_bytes`)
- Weitere Diagnosewerte wie Wakeup Source und Battery Status

## Installation

1. Repository in Home Assistant als Add-on Repository hinzufuegen
2. Add-on `Daly WNT BMS MQTT Bridge (Waveshare RS485/TCP)` installieren
3. Optionen konfigurieren
4. Add-on starten
5. Entitaeten erscheinen automatisch ueber MQTT Discovery
