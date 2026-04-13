# Home Assistant Add-ons

Dieses Repository enthaelt Home Assistant Add-ons fuer Daly WNT BMS Daten ueber MQTT.

## Enthaltenes Add-on

- `dalybms_waveshare`: `Daly WNT BMS MQTT Bridge (Waveshare RS485/TCP)`

## Wofuer das Add-on gedacht ist

Das Add-on liest Live-Daten vom Daly WNT Board ueber RS485 aus.
Die RS485-Verbindung wird ueber einen Waveshare RS485-to-RJ45 Ethernet Converter (TCP/IP zu Serial) angebunden.
Die Daten werden per MQTT Discovery in Home Assistant als Sensoren und Binary Sensoren angelegt.

Hinweis: Der Name "Deye" ist hier nicht passend, der Wechselrichter ist nicht Teil dieses Add-ons.

## Installation in Home Assistant

1. Home Assistant -> `Settings` -> `Add-ons` -> `Add-on Store`
2. Oben rechts `...` -> `Repositories`
3. Diese URL eintragen: `https://github.com/Ergerbolt/homeassistant-addons`
4. Store neu laden
5. Add-on `Daly WNT BMS MQTT Bridge (Waveshare RS485/TCP)` installieren

## Schneller Start

1. MQTT in Home Assistant bereitstellen (z. B. Mosquitto Add-on)
2. Im Add-on `mqtt_server`, `mqtt_user`, `mqtt_pass` eintragen
3. `device` auf IP/Hostname des Waveshare TCP Moduls setzen
4. `cells_in_series` und `device_id` passend setzen
5. `debug_logging` bei Bedarf aktivieren (standardmaessig `false`)
6. Add-on starten

Danach erscheinen die Entitaeten automatisch per MQTT Discovery.
