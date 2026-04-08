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
            "name": "Battery SOC Candidate",
            "state_topic": f"{STATE_TOPIC}/state",
            "unit_of_measurement": "%",
            "value_template": "{{ value_json.soc_candidate }}",
            "unique_id": f"{DEVICE_ID}_soc_candidate",
            "device": device_conf,
            "json_attributes_topic": f"{DEBUG_TOPIC}/state",
        },
        f"{STATE_TOPIC}_voltage/config": {
            "device_class": "voltage",
            "name": "Battery Voltage Candidate",
            "state_topic": f"{STATE_TOPIC}/state",
            "unit_of_measurement": "V",
            "value_template": "{{ value_json.pack_voltage_candidate }}",
            "unique_id": f"{DEVICE_ID}_pack_voltage_candidate",
            "device": device_conf,
        },
        f"{STATE_TOPIC}_current/config": {
            "name": "Battery Current Candidate Raw",
            "state_topic": f"{STATE_TOPIC}/state",
            "unit_of_measurement": "raw",
            "value_template": "{{ value_json.current_candidate_raw }}",
            "unique_id": f"{DEVICE_ID}_current_candidate_raw",
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
            "name": "Temperature Candidate Raw",
            "state_topic": f"{TEMP_TOPIC}/state",
            "unit_of_measurement": "raw",
            "value_template": "{{ value_json.value }}",
            "unique_id": f"{DEVICE_ID}_temp_candidate_raw",
            "device": device_conf,
            "json_attributes_topic": f"{TEMP_TOPIC}/state",
        },
        f"{RAW_TOPIC}/config": {
            "name": "Raw Block Hex",
            "state_topic": f"{RAW_TOPIC}/state",
            "value_template": "{{ value_json.prefix }}",
            "unique_id": f"{DEVICE_ID}_raw_hex",
            "device": device_conf,
            "json_attributes_topic": f"{RAW_TOPIC}/state",
        },
        f"{MOS_TOPIC}/config": {
            "name": "Status Candidate Raw",
            "state_topic": f"{MOS_TOPIC}/state",
            "value_template": "{{ value_json.value }}",
            "unique_id": f"{DEVICE_ID}_status_candidate_raw",
            "device": device_conf,
            "json_attributes_topic": f"{MOS_TOPIC}/state",
        },
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
    """Publish candidate fields we can correlate over time."""
    try:
        pack_voltage_from_cells = None
        if cells_payload is not None:
            pack_voltage_from_cells = round(cells_payload["sum"], 1)

        candidates = {
            "soc_candidate": u16(block, 160),
            "pack_voltage_candidate": pack_voltage_from_cells,
            "current_candidate_raw": u16(block, 168),
        }
        log.info("Candidates: %s", candidates)
        publish(f"{STATE_TOPIC}/state", candidates)

        debug_payload = {
            "word_80": u16(block, 160),
            "word_81": u16(block, 162),
            "word_82": u16(block, 164),
            "word_83": u16(block, 166),
            "word_84": u16(block, 168),
            "word_85": u16(block, 170),
            "word_86": u16(block, 172),
            "word_87": u16(block, 174),
            "word_88": u16(block, 176),
            "word_89": u16(block, 178),
            "pack_voltage_from_cells": pack_voltage_from_cells,
        }
        publish(f"{DEBUG_TOPIC}/state", debug_payload)

        temp_payload = {
            "value": u16(block, 176),
            "raw_176": u16(block, 176),
            "raw_178": u16(block, 178),
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
