# Changelog

## 1.1.22
- Improved write robustness:
  - Retry once if a write command receives a `0x03` response frame first.
  - Better handling for converter/gateway response quirks during write operations.
- Improved control value prefill:
  - Falls back to single-register reads if a range read does not return usable data.
- Control command writes now always try both known register variants (base and `+256` offset), independent of legacy allowlist settings.

## 1.1.21
- Fixed write handling for firmware variants with different parameter register offsets.
- Added automatic fallback between base registers and `+256` offset registers for:
  - MOS control
  - Rated/actual capacity
  - Max charge/discharge current level 1/2
- Expanded default write allowlist to include both register variants.

## 1.1.20
- Added Home Assistant MQTT control entities when `enable_write_commands=true`:
  - `Charge MOS Control` / `Discharge MOS Control` switches
  - `Rated Capacity` / `Actual Capacity` numbers
  - `Max Charge Current L1/L2` and `Max Discharge Current L1/L2` numbers
- Added automatic readback/publish of control register state from Modbus block `265..326`.
- Added payload decoding/encoding for current and capacity writes to match BMSTool register format.
- Expanded default write allowlist to include control and parameter registers.

## 1.1.19
- Added optional Modbus write command support via MQTT.
- Added safe write allowlist (`write_allowed_registers`, default `290,503,504`).
- Added write command/result topics (`write_command_topic`, `write_result_topic`) with automatic defaults.
- Added Modbus TCP function `0x06` (single register write) and `0x10` (multiple register write).
- Added thread-safe Modbus request handling so read polling and writes do not collide.

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
