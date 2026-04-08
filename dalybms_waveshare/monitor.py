#!/usr/bin/env python3
import json
import logging
import os
import socket
import struct
import sys
import time
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import paho.mqtt.client as mqtt

print("=== monitor.py started (modbus block mode) ===", flush=True)

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


DEVICE = os.environ["DEVICE"]
DEVICE_ID = os.environ["DEVICE_ID"]
CELL_COUNT = int(os.environ["CELL_COUNT"])
POLL_INTERVAL_SECONDS = max(1, int(os.environ.get("POLL_INTERVAL_SECONDS", "10")))

MQTT_SERVER = os.environ["MQTT_SERVER"]
MQTT_USER = os.environ["MQTT_USER"]
MQTT_PASS = os.environ["MQTT_PASS"]
MQTT_CLIENT_ID = os.environ["MQTT_CLIENT_ID"]
MQTT_DISCOVERY_PREFIX = os.environ["MQTT_DISCOVERY_PREFIX"]
NOMINAL_CAPACITY_AH = float(os.environ.get("NOMINAL_CAPACITY_AH", "0"))

MODBUS_PORT = int(os.environ.get("MODBUS_PORT", "502"))
MODBUS_UNIT_ID = int(os.environ.get("MODBUS_UNIT_ID", "81"))
MODBUS_START = int(os.environ.get("MODBUS_START", "0"))
MODBUS_COUNT = int(os.environ.get("MODBUS_COUNT", "127"))
SOCKET_TIMEOUT = float(os.environ.get("SOCKET_TIMEOUT", "3"))


def resolve_connection_target(device: str, default_port: int) -> Tuple[str, int]:
    host = device.strip()

    if "://" in host:
        parsed = urlparse(host)
        if parsed.hostname:
            host = parsed.hostname
        if parsed.port is not None and parsed.port != default_port:
            log.warning(
                "DEVICE contains port %s but MODBUS_PORT=%s is configured; using MODBUS_PORT",
                parsed.port,
                default_port,
            )
    elif host.count(":") == 1:
        maybe_host, maybe_port = host.rsplit(":", 1)
        if maybe_host and maybe_port.isdigit():
            host = maybe_host
            if int(maybe_port) != default_port:
                log.warning(
                    "DEVICE contains port %s but MODBUS_PORT=%s is configured; using MODBUS_PORT",
                    maybe_port,
                    default_port,
                )

    if not host:
        raise ValueError("DEVICE must not be empty")

    return host, default_port


CONNECTION_HOST, CONNECTION_PORT = resolve_connection_target(DEVICE, MODBUS_PORT)

log.info("Starting WNT/Deye block monitor")
log.info("DEVICE=%s", DEVICE)
log.info("DEVICE_ID=%s", DEVICE_ID)
log.info("CELL_COUNT=%s", CELL_COUNT)
log.info("POLL_INTERVAL_SECONDS=%s", POLL_INTERVAL_SECONDS)
log.info("MQTT_SERVER=%s", MQTT_SERVER)
log.info("MQTT_CLIENT_ID=%s", MQTT_CLIENT_ID)
log.info("MQTT_DISCOVERY_PREFIX=%s", MQTT_DISCOVERY_PREFIX)
log.info("NOMINAL_CAPACITY_AH=%s", NOMINAL_CAPACITY_AH)
log.info("MODBUS_PORT=%s", MODBUS_PORT)
log.info("MODBUS_UNIT_ID=%s", MODBUS_UNIT_ID)
log.info("MODBUS_START=%s", MODBUS_START)
log.info("MODBUS_COUNT=%s", MODBUS_COUNT)
log.info("CONNECTION_HOST=%s", CONNECTION_HOST)
log.info("CONNECTION_PORT=%s", CONNECTION_PORT)


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
RAW_TOPIC = f"{STATE_TOPIC}_raw"
DEBUG_TOPIC = f"{STATE_TOPIC}_debug"


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
        "manufacturer": "Dongguan Daly Electronics / WNT",
        "name": "Smart BMS",
        "identifiers": [DEVICE_ID],
    }

    discovery = {
        f"{STATE_TOPIC}_soc/config": {
            "device_class": "battery",
            "name": "SOC",
            "state_topic": f"{STATE_TOPIC}/state",
            "unit_of_measurement": "%",
            "value_template": "{{ value_json.soc }}",
            "unique_id": f"{DEVICE_ID}_soc",
            "device": device_conf,
            "json_attributes_topic": f"{DEBUG_TOPIC}/state",
        },
        f"{STATE_TOPIC}_voltage/config": {
            "device_class": "voltage",
            "name": "Voltage",
            "state_topic": f"{STATE_TOPIC}/state",
            "unit_of_measurement": "V",
            "value_template": "{{ value_json.voltage }}",
            "unique_id": f"{DEVICE_ID}_voltage",
            "device": device_conf,
        },
        f"{STATE_TOPIC}_current/config": {
            "device_class": "current",
            "name": "Current",
            "state_topic": f"{STATE_TOPIC}/state",
            "unit_of_measurement": "A",
            "value_template": "{{ value_json.current }}",
            "unique_id": f"{DEVICE_ID}_current",
            "device": device_conf,
        },
        f"{STATE_TOPIC}_power/config": {
            "device_class": "power",
            "name": "Power",
            "state_topic": f"{STATE_TOPIC}/state",
            "unit_of_measurement": "W",
            "value_template": "{{ value_json.power }}",
            "unique_id": f"{DEVICE_ID}_power",
            "device": device_conf,
        },
        f"{STATE_TOPIC}_remaining_ah/config": {
            "name": "Charge",
            "state_topic": f"{STATE_TOPIC}/state",
            "unit_of_measurement": "Ah",
            "value_template": "{{ value_json.remaining_charge_ah }}",
            "unique_id": f"{DEVICE_ID}_remaining_charge_ah",
            "device": device_conf,
        },
        f"{CELLS_TOPIC}/config": {
            "device_class": "voltage",
            "name": "Cell Volt delta",
            "state_topic": f"{CELLS_TOPIC}/state",
            "unit_of_measurement": "V",
            "value_template": "{{ value_json.diff }}",
            "json_attributes_topic": f"{CELLS_TOPIC}/state",
            "unique_id": f"{DEVICE_ID}_cell_delta",
            "device": device_conf,
        },
        f"{CELLS_TOPIC}_avg/config": {
            "device_class": "voltage",
            "name": "Cell Volt average",
            "state_topic": f"{CELLS_TOPIC}/state",
            "unit_of_measurement": "V",
            "value_template": "{{ value_json.avg }}",
            "unique_id": f"{DEVICE_ID}_cell_avg",
            "device": device_conf,
        },
        f"{CELLS_TOPIC}_min/config": {
            "device_class": "voltage",
            "name": "Cell Volt min",
            "state_topic": f"{CELLS_TOPIC}/state",
            "unit_of_measurement": "V",
            "value_template": "{{ value_json.min }}",
            "unique_id": f"{DEVICE_ID}_cell_min",
            "device": device_conf,
        },
        f"{CELLS_TOPIC}_max/config": {
            "device_class": "voltage",
            "name": "Cell Volt max",
            "state_topic": f"{CELLS_TOPIC}/state",
            "unit_of_measurement": "V",
            "value_template": "{{ value_json.max }}",
            "unique_id": f"{DEVICE_ID}_cell_max",
            "device": device_conf,
        },
        f"{CELLS_TOPIC}_min_idx/config": {
            "name": "Cell Index min",
            "state_topic": f"{CELLS_TOPIC}/state",
            "value_template": "{{ value_json.minCell }}",
            "unique_id": f"{DEVICE_ID}_cell_min_index",
            "device": device_conf,
        },
        f"{CELLS_TOPIC}_max_idx/config": {
            "name": "Cell Index max",
            "state_topic": f"{CELLS_TOPIC}/state",
            "value_template": "{{ value_json.maxCell }}",
            "unique_id": f"{DEVICE_ID}_cell_max_index",
            "device": device_conf,
        },
        f"{TEMP_TOPIC}/config": {
            "device_class": "temperature",
            "name": "Temperature 1",
            "state_topic": f"{TEMP_TOPIC}/state",
            "unit_of_measurement": "°C",
            "value_template": "{{ value_json.temperature_1 }}",
            "unique_id": f"{DEVICE_ID}_temperature_1",
            "device": device_conf,
            "json_attributes_topic": f"{TEMP_TOPIC}/state",
        },
        f"{TEMP_TOPIC}_2/config": {
            "device_class": "temperature",
            "name": "Temperature 2",
            "state_topic": f"{TEMP_TOPIC}/state",
            "unit_of_measurement": "°C",
            "value_template": "{{ value_json.temperature_2 }}",
            "unique_id": f"{DEVICE_ID}_temperature_2",
            "device": device_conf,
        },
        f"{RAW_TOPIC}/config": {
            "name": "Raw Block Hex",
            "state_topic": f"{RAW_TOPIC}/state",
            "value_template": "{{ value_json.prefix }}",
            "unique_id": f"{DEVICE_ID}_raw_hex",
            "device": device_conf,
            "entity_category": "diagnostic",
            "json_attributes_topic": f"{RAW_TOPIC}/state",
        },
        f"{MOS_TOPIC}/config": {
            "name": "Status Candidate Raw",
            "state_topic": f"{MOS_TOPIC}/state",
            "value_template": "{{ value_json.value }}",
            "unique_id": f"{DEVICE_ID}_status_candidate_raw",
            "device": device_conf,
            "entity_category": "diagnostic",
            "json_attributes_topic": f"{MOS_TOPIC}/state",
        },
    }

    for cell_index in range(1, CELL_COUNT + 1):
        discovery[f"{CELLS_TOPIC}_{cell_index:02d}/config"] = {
            "device_class": "voltage",
            "name": f"Cell Volt {cell_index:02d}",
            "state_topic": f"{CELLS_TOPIC}/state",
            "unit_of_measurement": "V",
            "value_template": f"{{{{ value_json.cell_{cell_index} }}}}",
            "unique_id": f"{DEVICE_ID}_cell_{cell_index:02d}",
            "device": device_conf,
        }

    log.info("Publishing MQTT discovery for %d entities", len(discovery))
    for topic, payload in discovery.items():
        publish(topic, payload, retain=True)


class ModbusTcpClient:
    def __init__(self, host: str, port: int, timeout: float = 3):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.tx_id = 1

    def read_holding_registers(self, unit_id: int, start: int, count: int) -> Optional[bytes]:
        tx_id = self.tx_id & 0xFFFF
        self.tx_id += 1

        pdu = struct.pack(">BHH", 0x03, start, count)
        mbap = struct.pack(">HHHB", tx_id, 0, len(pdu) + 1, unit_id)
        request = mbap + pdu

        log.debug("Modbus TX: %s", hexdump(request))

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)

        try:
            sock.connect((self.host, self.port))
            sock.sendall(request)

            header = self._recv_exact(sock, 7)
            if not header:
                log.warning("No MBAP header received")
                return None

            log.debug("Modbus RX header: %s", hexdump(header))

            rx_tx_id, proto_id, length, unit = struct.unpack(">HHHB", header)
            if rx_tx_id != tx_id:
                log.warning("Unexpected transaction id: tx=%s rx=%s", tx_id, rx_tx_id)
            if proto_id != 0:
                log.warning("Unexpected proto_id=%s", proto_id)
            if unit != unit_id:
                log.warning("Unexpected unit id: tx=%s rx=%s", unit_id, unit)

            payload_len = length - 1
            payload = self._recv_exact(sock, payload_len)
            if payload is None:
                log.warning("No Modbus payload received")
                return None

            full = header + payload
            log.debug("Modbus RX full (%d bytes): %s", len(full), hexdump(full))
            return full

        except Exception:
            log.exception("Modbus TCP request failed")
            return None
        finally:
            try:
                sock.close()
            except Exception:
                pass

    def _recv_exact(self, sock: socket.socket, n: int) -> Optional[bytes]:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf


def parse_modbus_block(frame: bytes) -> Optional[bytes]:
    """Return the 254-byte data block if the response matches 03 FE."""
    if not frame or len(frame) < 10:
        return None

    payload = frame[7:]
    if len(payload) < 2:
        return None

    func = payload[0]
    if func != 0x03:
        log.warning("Unexpected function code: 0x%02X", func)
        return None

    byte_count = payload[1]
    if byte_count != 0xFE:
        log.warning("Unexpected byte_count=%s", byte_count)
        return None

    data = payload[2:]
    if len(data) < 254:
        log.warning("Payload shorter than expected: %d", len(data))
        return None

    return data[:254]


def u16(data: bytes, offset: int) -> int:
    return int.from_bytes(data[offset : offset + 2], byteorder="big", signed=False)


def parse_cell_voltages(block: bytes, cell_count: int) -> List[float]:
    """Heuristic based on observed block: first 16 words look like cell voltages in mV."""
    cells = []
    for i in range(cell_count):
        mv = u16(block, i * 2)
        cells.append(round(mv / 1000.0, 3))
    return cells


def cells_look_plausible(cells: List[float]) -> bool:
    return bool(cells) and all(2.0 <= cell <= 4.5 for cell in cells)


def parse_temperature(raw_value: int) -> Optional[float]:
    if raw_value in (0, 255, 65535):
        return None
    value = raw_value - 40
    if -40 <= value <= 120:
        return float(value)
    return None


def parse_main_metrics(block: bytes, cells_payload: Optional[dict] = None) -> dict:
    voltage_raw = u16(block, 112)
    current_raw = u16(block, 114)
    soc_raw = u16(block, 116)
    temp_1_raw = u16(block, 96)
    temp_2_raw = u16(block, 98)

    voltage = round(voltage_raw / 10.0, 1) if 0 < voltage_raw < 1000 else None
    if voltage is None and cells_payload is not None:
        voltage = round(cells_payload["sum"], 1)

    current = None
    if 25000 <= current_raw <= 35000:
        current = round((current_raw - 30000) / 10.0, 1)

    soc = round(soc_raw / 10.0, 1) if 0 < soc_raw <= 1000 else None
    power = round(voltage * current, 1) if voltage is not None and current is not None else None

    remaining_charge_ah = None
    if soc is not None and NOMINAL_CAPACITY_AH > 0:
        remaining_charge_ah = round(NOMINAL_CAPACITY_AH * soc / 100.0, 2)

    return {
        "voltage": voltage,
        "voltage_raw": voltage_raw,
        "current": current,
        "current_raw": current_raw,
        "soc": soc,
        "soc_raw": soc_raw,
        "power": power,
        "remaining_charge_ah": remaining_charge_ah,
        "temperature_1": parse_temperature(temp_1_raw),
        "temperature_2": parse_temperature(temp_2_raw),
        "temperature_1_raw": temp_1_raw,
        "temperature_2_raw": temp_2_raw,
    }


def publish_raw(block: bytes):
    payload = {
        "prefix": hexdump(block[:32]),
        "length": len(block),
        "full_hex": hexdump(block),
    }
    publish(f"{RAW_TOPIC}/state", payload)


def publish_cells(block: bytes) -> Optional[dict]:
    try:
        cells = parse_cell_voltages(block, CELL_COUNT)
        if not cells_look_plausible(cells):
            log.warning("Cell voltages look implausible: %s", cells)
            return None

        min_v = min(cells)
        max_v = max(cells)
        payload = {f"cell_{i+1}": v for i, v in enumerate(cells)}
        payload.update(
            {
                "sum": round(sum(cells), 3),
                "avg": round(sum(cells) / len(cells), 3),
                "min": min_v,
                "minCell": cells.index(min_v) + 1,
                "max": max_v,
                "maxCell": cells.index(max_v) + 1,
                "diff": round(max_v - min_v, 3),
            }
        )
        log.info(
            "Cells parsed: min=%.3fV cell=%d max=%.3fV cell=%d diff=%.3fV",
            payload["min"],
            payload["minCell"],
            payload["max"],
            payload["maxCell"],
            payload["diff"],
        )
        publish(f"{CELLS_TOPIC}/state", payload)
        return payload
    except Exception:
        log.exception("Failed to parse/publish cells")
        return None


def publish_candidates(block: bytes, cells_payload: Optional[dict] = None):
    """Publish parsed metrics and a few raw debug fields for further mapping."""
    try:
        metrics = parse_main_metrics(block, cells_payload)
        state_payload = {
            "voltage": metrics["voltage"],
            "current": metrics["current"],
            "power": metrics["power"],
            "soc": metrics["soc"],
            "remaining_charge_ah": metrics["remaining_charge_ah"],
        }
        log.info("Parsed metrics: %s", state_payload)
        publish(f"{STATE_TOPIC}/state", state_payload)

        debug_payload = {
            "word_48": u16(block, 96),
            "word_49": u16(block, 98),
            "word_56": u16(block, 112),
            "word_57": u16(block, 114),
            "word_58": u16(block, 116),
            "word_59": u16(block, 118),
            "word_60": u16(block, 120),
            "word_61": u16(block, 122),
            "word_62": u16(block, 124),
            "word_63": u16(block, 126),
            "word_64": u16(block, 128),
            "word_65": u16(block, 130),
            "word_66": u16(block, 132),
            "word_67": u16(block, 134),
            "word_68": u16(block, 136),
            "word_69": u16(block, 138),
            "word_70": u16(block, 140),
            "word_75": u16(block, 150),
            "word_76": u16(block, 152),
            "word_78": u16(block, 156),
            "pack_voltage_from_cells": round(cells_payload["sum"], 1) if cells_payload else None,
            "voltage_raw": metrics["voltage_raw"],
            "current_raw": metrics["current_raw"],
            "soc_raw": metrics["soc_raw"],
        }
        publish(f"{DEBUG_TOPIC}/state", debug_payload)

        temp_payload = {
            "temperature_1": metrics["temperature_1"],
            "temperature_2": metrics["temperature_2"],
            "raw_1": metrics["temperature_1_raw"],
            "raw_2": metrics["temperature_2_raw"],
        }
        publish(f"{TEMP_TOPIC}/state", temp_payload)

        mos_payload = {
            "value": str(u16(block, 182)),
            "raw_180": u16(block, 180),
            "raw_182": u16(block, 182),
            "raw_184": u16(block, 184),
        }
        publish(f"{MOS_TOPIC}/state", mos_payload)

        status_payload = {
            "raw_150": u16(block, 150),
            "raw_152": u16(block, 152),
            "raw_154": u16(block, 154),
            "raw_156": u16(block, 156),
            "raw_158": u16(block, 158),
            "raw_160": u16(block, 160),
        }
        publish(f"{STATUS_TOPIC}/state", status_payload)

    except Exception:
        log.exception("Failed to publish candidates")


publish_discovery()

modbus = ModbusTcpClient(CONNECTION_HOST, CONNECTION_PORT, SOCKET_TIMEOUT)

try:
    while True:
        log.info("Polling WNT/Deye block via Modbus TCP...")
        frame = modbus.read_holding_registers(MODBUS_UNIT_ID, MODBUS_START, MODBUS_COUNT)

        if not frame:
            log.warning("No Modbus frame received")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        block = parse_modbus_block(frame)
        if not block:
            log.warning("Could not parse Modbus block")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        log.info("Parsed fixed block with %d bytes", len(block))
        publish_raw(block)
        cells_payload = publish_cells(block)
        publish_candidates(block, cells_payload)

        time.sleep(POLL_INTERVAL_SECONDS)

finally:
    log.info("Shutting down block monitor")
    try:
        client.loop_stop()
        client.disconnect()
        log.info("MQTT disconnected")
    except Exception:
        log.exception("Failed to disconnect MQTT")
