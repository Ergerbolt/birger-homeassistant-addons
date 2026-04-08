#!/usr/bin/env python3
"""Daly Smart BMS monitor for Home Assistant via MQTT.

Adapted for Waveshare TCP-to-serial bridges via pySerial URL handlers, e.g.
`socket://10.0.0.135:4196`.

This version adds:
- continuous background reader
- frame synchronization on Daly 13-byte frames
- checksum validation
- per-command response collection
- extensive logging
"""

import json
import logging
import os
import sys
import threading
import time
from collections import defaultdict
from typing import Dict, List, Optional

import paho.mqtt.client as mqtt
import serial


print("=== monitor.py started ===", flush=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("dalybms")


def hexdump(data: bytes) -> str:
    if not data:
        return "<empty>"
    return " ".join(f"{b:02X}" for b in data)


def checksum_ok(frame: bytes) -> bool:
    if len(frame) != 13:
        return False
    return (sum(frame[:12]) & 0xFF) == frame[12]


def open_serial(device: str):
    if "://" in device:
        log.info("Opening TCP serial bridge with serial_for_url: %s", device)
        return serial.serial_for_url(device, baudrate=9600, timeout=0.1)
    log.info("Opening local serial port: %s", device)
    return serial.Serial(device, 9600, timeout=0.1)


DEVICE = os.environ["DEVICE"]
DEVICE_ID = os.environ["DEVICE_ID"]
CELL_COUNT = int(os.environ["CELL_COUNT"])
POLL_INTERVAL_SECONDS = max(1, int(os.environ.get("POLL_INTERVAL_SECONDS", "2")))

MQTT_SERVER = os.environ["MQTT_SERVER"]
MQTT_USER = os.environ["MQTT_USER"]
MQTT_PASS = os.environ["MQTT_PASS"]
MQTT_CLIENT_ID = os.environ["MQTT_CLIENT_ID"]
MQTT_DISCOVERY_PREFIX = os.environ["MQTT_DISCOVERY_PREFIX"]

log.info("Starting Daly BMS monitor")
log.info("DEVICE=%s", DEVICE)
log.info("DEVICE_ID=%s", DEVICE_ID)
log.info("CELL_COUNT=%s", CELL_COUNT)
log.info("POLL_INTERVAL_SECONDS=%s", POLL_INTERVAL_SECONDS)
log.info("MQTT_SERVER=%s", MQTT_SERVER)
log.info("MQTT_CLIENT_ID=%s", MQTT_CLIENT_ID)
log.info("MQTT_DISCOVERY_PREFIX=%s", MQTT_DISCOVERY_PREFIX)

ser = open_serial(DEVICE)
log.info("Serial/TCP connection opened successfully")

client = mqtt.Client(client_id=MQTT_CLIENT_ID)
client.username_pw_set(MQTT_USER, MQTT_PASS)


def on_connect(client, userdata, flags, rc):
    log.info("MQTT connected with result code: %s", rc)


def on_disconnect(client, userdata, rc):
    log.warning("MQTT disconnected with result code: %s", rc)


client.on_connect = on_connect
client.on_disconnect = on_disconnect

log.info("Connecting to MQTT broker...")
client.connect(MQTT_SERVER)
client.loop_start()

BASE_TOPIC = f"{MQTT_DISCOVERY_PREFIX}/sensor/"
STATE_TOPIC = f"{BASE_TOPIC}{DEVICE_ID}"
STATUS_TOPIC = f"{STATE_TOPIC}_status"
CELLS_TOPIC = f"{STATE_TOPIC}_balance"
TEMP_TOPIC = f"{STATE_TOPIC}_temp"
MOS_TOPIC = f"{STATE_TOPIC}_mos"


def publish(topic: str, payload: dict, retain: bool = False):
    try:
        payload_json = json.dumps(payload)
        log.debug("MQTT publish topic=%s retain=%s payload=%s", topic, retain, payload_json)
        info = client.publish(topic, payload_json, qos=0, retain=retain)
        log.debug("MQTT publish result rc=%s mid=%s", info.rc, info.mid)
    except Exception:
        log.exception("Failed to publish MQTT topic=%s payload=%s", topic, payload)


def publish_discovery():
    device_conf = {
        "manufacturer": "Dongguan Daly Electronics",
        "name": "Smart BMS",
        "identifiers": [DEVICE_ID],
    }

    discovery = {
        f"{STATE_TOPIC}_soc/config": {
            "device_class": "battery",
            "name": "Battery SOC",
            "state_topic": f"{STATE_TOPIC}/state",
            "unit_of_measurement": "%",
            "value_template": "{{ value_json.soc }}",
            "unique_id": f"{DEVICE_ID}_soc",
            "device": device_conf,
            "json_attributes_topic": f"{STATUS_TOPIC}/state",
        },
        f"{STATE_TOPIC}_voltage/config": {
            "device_class": "voltage",
            "name": "Battery Voltage",
            "state_topic": f"{STATE_TOPIC}/state",
            "unit_of_measurement": "V",
            "value_template": "{{ value_json.voltage }}",
            "unique_id": f"{DEVICE_ID}_voltage",
            "device": device_conf,
        },
        f"{STATE_TOPIC}_current/config": {
            "device_class": "current",
            "name": "Battery Current",
            "state_topic": f"{STATE_TOPIC}/state",
            "unit_of_measurement": "A",
            "value_template": "{{ value_json.current }}",
            "unique_id": f"{DEVICE_ID}_current",
            "device": device_conf,
        },
        f"{CELLS_TOPIC}/config": {
            "device_class": "voltage",
            "name": "Battery Cell Balance",
            "state_topic": f"{CELLS_TOPIC}/state",
            "unit_of_measurement": "V",
            "value_template": "{{ value_json.diff }}",
            "json_attributes_topic": f"{CELLS_TOPIC}/state",
            "unique_id": f"{DEVICE_ID}_balance",
            "device": device_conf,
        },
        f"{TEMP_TOPIC}/config": {
            "device_class": "temperature",
            "name": "Battery Temperature",
            "state_topic": f"{TEMP_TOPIC}/state",
            "unit_of_measurement": "°C",
            "value_template": "{{ value_json.value }}",
            "unique_id": f"{DEVICE_ID}_temp",
            "device": device_conf,
            "json_attributes_topic": f"{TEMP_TOPIC}/state",
        },
        f"{MOS_TOPIC}/config": {
            "name": "MOS Status",
            "state_topic": f"{MOS_TOPIC}/state",
            "value_template": "{{ value_json.value }}",
            "unique_id": f"{DEVICE_ID}_mos",
            "device": device_conf,
            "json_attributes_topic": f"{MOS_TOPIC}/state",
        },
    }

    log.info("Publishing MQTT discovery for %d entities", len(discovery))
    for topic, payload in discovery.items():
        publish(topic, payload, retain=True)


class DalyTransport:
    def __init__(self, serial_port):
        self.ser = serial_port
        self.rx_buffer = bytearray()
        self.frames_by_cmd: Dict[int, List[bytes]] = defaultdict(list)
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.reader = threading.Thread(target=self._reader_loop, daemon=True)
        self.reader.start()
        log.info("Background reader thread started")

    def close(self):
        self.stop_event.set()
        try:
            self.reader.join(timeout=1.0)
        except Exception:
            log.exception("Failed joining reader thread")

    def _reader_loop(self):
        log.info("Reader loop running")
        while not self.stop_event.is_set():
            try:
                chunk = self.ser.read(256)
                if not chunk:
                    time.sleep(0.02)
                    continue

                log.debug("RX raw chunk (%d bytes): %s", len(chunk), hexdump(chunk))

                with self.lock:
                    self.rx_buffer.extend(chunk)
                    self._extract_frames_locked()

            except Exception:
                log.exception("Reader loop exception")
                time.sleep(0.2)

    def _extract_frames_locked(self):
        """Extract valid 13-byte Daly frames from the raw byte stream.

        We search for frames starting with 0xA5 and valid checksum.
        """
        while len(self.rx_buffer) >= 13:
            start = self.rx_buffer.find(b"\xA5")
            if start < 0:
                if self.rx_buffer:
                    log.debug("Dropping %d unsynced bytes (no A5 found)", len(self.rx_buffer))
                self.rx_buffer.clear()
                return

            if start > 0:
                dropped = bytes(self.rx_buffer[:start])
                log.debug("Dropping %d unsynced bytes before A5: %s", len(dropped), hexdump(dropped))
                del self.rx_buffer[:start]

            if len(self.rx_buffer) < 13:
                return

            candidate = bytes(self.rx_buffer[:13])

            if checksum_ok(candidate):
                cmd_id = candidate[2]
                self.frames_by_cmd[cmd_id].append(candidate)
                log.debug(
                    "Accepted frame cmd=0x%02X frame=%s",
                    cmd_id,
                    hexdump(candidate),
                )
                del self.rx_buffer[:13]
            else:
                log.debug("Rejected candidate frame (bad checksum): %s", hexdump(candidate))
                del self.rx_buffer[0]

    def transact(self, command: bytes, wait_time: float = 0.4) -> List[bytes]:
        """Send one Daly command and collect matching response frames."""
        cmd_id = command[2]

        with self.lock:
            self.frames_by_cmd[cmd_id].clear()
            self.rx_buffer.clear()

        try:
            log.debug("TX: %s", hexdump(command))
            self.ser.write(command)
            self.ser.flush()
        except Exception:
            log.exception("Failed to write command %s", hexdump(command))
            return []

        time.sleep(wait_time)

        with self.lock:
            frames = list(self.frames_by_cmd[cmd_id])

        if not frames:
            log.warning("No valid Daly frame received for cmd=0x%02X", cmd_id)
        else:
            log.debug("Collected %d valid frame(s) for cmd=0x%02X", len(frames), cmd_id)
            for i, frame in enumerate(frames, start=1):
                log.debug("Frame %d cmd=0x%02X: %s", i, cmd_id, hexdump(frame))

        return frames


transport = DalyTransport(ser)


def extract_cells_v(frame: bytes):
    return [
        int.from_bytes(frame[5:7], byteorder="big", signed=False),
        int.from_bytes(frame[7:9], byteorder="big", signed=False),
        int.from_bytes(frame[9:11], byteorder="big", signed=False),
    ]


def get_cell_balance(cell_count: int):
    frames = transport.transact(b"\xA5\x40\x95\x08\x00\x00\x00\x00\x00\x00\x00\x00\x82", wait_time=0.5)
    if not frames:
        log.warning("Empty response get_cell_balance")
        return

    try:
        cells = []
        for frame in frames:
            cells += extract_cells_v(frame)

        cells = cells[:cell_count]
        if not cells:
            log.warning("No cells parsed from balance frames")
            return

        cells = [round(v / 1000, 3) for v in cells]
        total = round(sum(cells), 3)
        min_v = min(cells)
        max_v = max(cells)

        payload = {f"cell_{i+1}": cell for i, cell in enumerate(cells)}
        payload.update(
            {
                "sum": round(total, 1),
                "avg": round(total / len(cells), 3),
                "min": min_v,
                "minCell": cells.index(min_v) + 1,
                "max": max_v,
                "maxCell": cells.index(max_v) + 1,
                "diff": round(max_v - min_v, 3),
            }
        )

        log.info(
            "Parsed cell balance: min=%.3fV cell=%d max=%.3fV cell=%d diff=%.3fV",
            payload["min"],
            payload["minCell"],
            payload["max"],
            payload["maxCell"],
            payload["diff"],
        )
        publish(f"{CELLS_TOPIC}/state", payload)

    except Exception:
        log.exception("Failed to parse cell balance response")


def get_battery_state():
    frames = transport.transact(b"\xA5\x40\x90\x08\x00\x00\x00\x00\x00\x00\x00\x00\x7D", wait_time=0.35)
    if not frames:
        log.warning("Empty response get_battery_state")
        return

    try:
        frame = frames[0]
        voltage = int.from_bytes(frame[4:6], byteorder="big", signed=False) / 10
        acquisition = int.from_bytes(frame[6:8], byteorder="big", signed=False) / 10
        current = int.from_bytes(frame[8:10], byteorder="big", signed=False) / 10 - 3000
        soc = int.from_bytes(frame[10:12], byteorder="big", signed=False) / 10

        log.info(
            "Parsed battery state: voltage=%.1fV acquisition=%.1fA current=%.1fA soc=%.1f%%",
            voltage,
            acquisition,
            current,
            soc,
        )

        publish(
            f"{STATE_TOPIC}/state",
            {
                "voltage": voltage,
                "acquisition": acquisition,
                "current": round(current, 1),
                "soc": soc,
            },
        )

    except Exception:
        log.exception("Failed to parse battery state response")


def get_battery_status():
    frames = transport.transact(b"\xA5\x40\x94\x08\x00\x00\x00\x00\x00\x00\x00\x00\x81", wait_time=0.35)
    if not frames:
        log.warning("Empty response get_battery_status")
        return

    try:
        frame = frames[0]
        batt_string = int.from_bytes(frame[4:5], byteorder="big", signed=False)
        temp = int.from_bytes(frame[5:6], byteorder="big", signed=False)
        charger = int.from_bytes(frame[6:7], byteorder="big", signed=False) == 1
        load = int.from_bytes(frame[7:8], byteorder="big", signed=False) == 1
        cycles = int.from_bytes(frame[9:11], byteorder="big", signed=False)

        log.info(
            "Parsed battery status: batt_string=%s temp=%s charger=%s load=%s cycles=%s",
            batt_string,
            temp,
            charger,
            load,
            cycles,
        )

        publish(
            f"{STATUS_TOPIC}/state",
            {
                "batt_string": batt_string,
                "temp": temp,
                "charger": charger,
                "load": load,
                "cycles": cycles,
            },
        )

    except Exception:
        log.exception("Failed to parse battery status response")


def get_battery_temp():
    frames = transport.transact(b"\xA5\x40\x92\x08\x00\x00\x00\x00\x00\x00\x00\x00\x7F", wait_time=0.35)
    if not frames:
        log.warning("Empty response get_battery_temp")
        return

    try:
        frame = frames[0]
        max_temp = int.from_bytes(frame[4:5], byteorder="big", signed=False) - 40
        max_temp_cell = int.from_bytes(frame[5:6], byteorder="big", signed=False)
        min_temp = int.from_bytes(frame[6:7], byteorder="big", signed=False) - 40
        min_temp_cell = int.from_bytes(frame[7:8], byteorder="big", signed=False)

        payload = {
            "value": (max_temp + min_temp) / 2,
            "maxTemp": max_temp,
            "maxTempCell": max_temp_cell,
            "minTemp": min_temp,
            "minTempCell": min_temp_cell,
        }

        log.info(
            "Parsed temperatures: avg=%.1f°C max=%s°C(cell %s) min=%s°C(cell %s)",
            payload["value"],
            max_temp,
            max_temp_cell,
            min_temp,
            min_temp_cell,
        )

        publish(f"{TEMP_TOPIC}/state", payload)

    except Exception:
        log.exception("Failed to parse battery temp response")


def get_battery_mos_status():
    frames = transport.transact(b"\xA5\x40\x93\x08\x00\x00\x00\x00\x00\x00\x00\x00\x80", wait_time=0.35)
    if not frames:
        log.warning("Empty response get_battery_mos_status")
        return

    try:
        frame = frames[0]
        value_byte = int.from_bytes(frame[4:5], byteorder="big", signed=False)
        value = "discharging" if value_byte == 2 else ("charging" if value_byte == 1 else "idle")
        charge_mos = int.from_bytes(frame[5:6], byteorder="big", signed=False)
        discharge_mos = int.from_bytes(frame[6:7], byteorder="big", signed=False)
        bms_life = int.from_bytes(frame[7:8], byteorder="big", signed=False)
        residual_capacity = int.from_bytes(frame[8:12], byteorder="big", signed=False)

        payload = {
            "value": value,
            "chargingMOS": charge_mos,
            "dischargingMOS": discharge_mos,
            "BMSLife": bms_life,
            "residualCapacity": residual_capacity,
        }

        log.info(
            "Parsed MOS status: value=%s charge_mos=%s discharge_mos=%s life=%s residual_capacity=%s",
            value,
            charge_mos,
            discharge_mos,
            bms_life,
            residual_capacity,
        )

        publish(f"{MOS_TOPIC}/state", payload)

    except Exception:
        log.exception("Failed to parse battery MOS response")


publish_discovery()

try:
    while True:
        log.info("Polling Daly BMS...")
        try:
            get_battery_state()
            get_cell_balance(CELL_COUNT)
            get_battery_status()
            get_battery_temp()
            get_battery_mos_status()
        except Exception:
            log.exception("Unexpected error during poll cycle")

        time.sleep(POLL_INTERVAL_SECONDS)

finally:
    log.info("Shutting down Daly BMS monitor")
    try:
        transport.close()
    except Exception:
        log.exception("Failed to stop transport")
    try:
        ser.close()
        log.info("Serial/TCP connection closed")
    except Exception:
        log.exception("Failed to close serial/TCP connection")
    finally:
        client.loop_stop()
        client.disconnect()
        log.info("MQTT disconnected")
