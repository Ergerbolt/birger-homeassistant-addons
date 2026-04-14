# Changelog

## 1.1.18
- Fixed Docker build issue by adding a default value for `BUILD_FROM`.
- Improved package install fallback for `paho-mqtt`.
- Updated service run script to include `debug_logging` env handling.

## 1.1.17
- Removed unclear entities from Home Assistant:
  - `Serial Port Type`
  - `Status Candidate Raw`
- Added MQTT discovery cleanup for removed entities.

## 1.1.16
- Removed non-essential/unclear entities from Home Assistant:
  - `AFE Current`, `AFE Factor`, `AFE Offset`, `AFE ADC`
  - `DI State`, `DO State`
- Added automatic cleanup of deprecated retained MQTT discovery topics.

## 1.1.15
- Added `debug_logging` option to add-on configuration.
- Implemented runtime switch between normal (`INFO`) and verbose (`DEBUG`) logging.
- Reduced log spam in normal mode by moving cyclic poll logs to debug level.

## 1.1.14
- Reworked naming and documentation for the actual use case:
  - Daly WNT board over Waveshare RS485-to-Ethernet converter
  - MQTT integration for Home Assistant
- Removed misleading Deye naming from add-on title/description/docs.
- Added decoding of `new_fault_bytes` and exposed decoded active faults.
