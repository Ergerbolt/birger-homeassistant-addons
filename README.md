# Home Assistant Add-ons

Dieses Repository enthaelt eigene Home Assistant Add-ons.
Aktuell ist ein Add-on fuer ein WNT/Deye (Daly-kompatibles) BMS ueber Modbus TCP enthalten.

## Enthaltene Add-ons

- `dalybms_waveshare`: Liest einen festen Modbus-TCP-Block und publiziert BMS-Daten per MQTT Discovery.

## Installation in Home Assistant

1. Home Assistant -> `Settings` -> `Add-ons` -> `Add-on Store`.
2. Im Store rechts oben auf das Menue -> `Repositories`.
3. Repository-URL eintragen:
   `https://github.com/Ergerbolt/homeassistant-addons`
4. Speichern und den Store neu laden.
5. Add-on `WNT/Deye Block Monitor over Modbus TCP` installieren.



## Add-on konfigurieren

Wichtige Optionen im Add-on:

- `mqtt_server`, `mqtt_user`, `mqtt_pass`
- `device` (IP/Host vom Modbus-TCP-Endpunkt)
- `device_id`
- `cells_in_series`
- `modbus_unit_id` (standard: `81`)
- `poll_interval_seconds`

Danach Add-on starten. Die Entitaeten werden ueber MQTT Discovery automatisch in Home Assistant angelegt.


