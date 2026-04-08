#!/usr/bin/env python3
"""Daly Smart BMS monitor for Home Assistant via MQTT.

Based on the protocol approach from MindFreeze/dalybms, adapted to support
TCP-to-serial bridges like Waveshare through pySerial URL handlers, e.g.
`socket://10.0.0.135:4196`.
"""

import json
import os
import time
from typing import List

import paho.mqtt.client as mqtt
import serial


def open_serial(device: str):
    if "://" in device:
        return serial.serial_for_url(device, baudrate=9600, timeout=1)
    return serial.Serial(device, 9600, timeout=1)


DEVICE = os.environ["DEVICE"]
DEVICE_ID = os.environ["DEVICE_ID"]
CELL_COUNT = int(os.environ["CELL_COUNT"])
POLL_INTERVAL_SECONDS = max(1, int(os.environ.get("POLL_INTERVAL_SECONDS", "2")))

print(f"Starting Daly BMS monitor on {DEVICE} ...")
ser = open_serial(DEVICE)

client = mqtt.Client(client_id=os.environ["MQTT_CLIENT_ID"])
client.username_pw_set(os.environ["MQTT_USER"], os.environ["MQTT_PASS"])
client.connect(os.environ["MQTT_SERVER"])
client.loop_start()

BASE_TOPIC = f"{os.environ['MQTT_DISCOVERY_PREFIX']}/sensor/"
STATE_TOPIC = f"{BASE_TOPIC}{DEVICE_ID}"
STATUS_TOPIC = f"{STATE_TOPIC}_status"
CELLS_TOPIC = f"{STATE_TOPIC}_balance"
TEMP_TOPIC = f"{STATE_TOPIC}_temp"
MOS_TOPIC = f"{STATE_TOPIC}_mos"


def publish(topic: str, payload: dict, retain: bool = False):
    client.publish(topic, json.dumps(payload), qos=0, retain=retain)


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

    for topic, payload in discovery.items():
        client.publish(topic, json.dumps(payload), qos=0, retain=True)


# Protocol command helpers taken from the Daly UART/RS485 protocol used by the
# upstream project. Frames are 13 bytes in the simple requests/responses below.

def cmd(command: bytes) -> List[bytes]:
    frames: List[bytes] = []
    ser.reset_input_buffer()
    ser.write(command)
    time.sleep(0.15)
    while True:
        buf = ser.read(13)
        if buf == b"":
            break
        frames.append(buf)
        # Some queries return a single frame, some multiple. A short timeout ends reading.
    return frames


def extract_cells_v(buffer: bytes):
    return [
        int.from_bytes(buffer[5:7], byteorder="big", signed=False),
        int.from_bytes(buffer[7:9], byteorder="big", signed=False),
        int.from_bytes(buffer[9:11], byteorder="big", signed=False),
    ]


def get_cell_balance(cell_count: int):
    res = cmd(b"\xa5\x40\x95\x08\x00\x00\x00\x00\x00\x00\x00\x00\x82")
    if not res:
        print("Empty response get_cell_balance")
        return
    cells = []
    for frame in res:
        cells += extract_cells_v(frame)
    cells = cells[:cell_count]
    cells = [round(v / 1000, 3) for v in cells]
    total = round(sum(cells), 3)
    min_v = min(cells)
    max_v = max(cells)
    payload = {f"cell_{i+1}": cell for i, cell in enumerate(cells)}
    payload.update(
        {
            "sum": round(total, 1),
            "avg": round(total / cell_count, 3),
            "min": min_v,
            "minCell": cells.index(min_v) + 1,
            "max": max_v,
            "maxCell": cells.index(max_v) + 1,
            "diff": round(max_v - min_v, 3),
        }
    )
    publish(f"{CELLS_TOPIC}/state", payload)


def get_battery_state():
    res = cmd(b"\xa5\x40\x90\x08\x00\x00\x00\x00\x00\x00\x00\x00\x7d")
    if not res:
        print("Empty response get_battery_state")
        return
    buffer = res[0]
    voltage = int.from_bytes(buffer[4:6], byteorder="big", signed=False) / 10
    acquisition = int.from_bytes(buffer[6:8], byteorder="big", signed=False) / 10
    current = int.from_bytes(buffer[8:10], byteorder="big", signed=False) / 10 - 3000
    soc = int.from_bytes(buffer[10:12], byteorder="big", signed=False) / 10
    publish(
        f"{STATE_TOPIC}/state",
        {
            "voltage": voltage,
            "acquisition": acquisition,
            "current": round(current, 1),
            "soc": soc,
        },
    )


def get_battery_status():
    res = cmd(b"\xa5\x40\x94\x08\x00\x00\x00\x00\x00\x00\x00\x00\x81")
    if not res:
        print("Empty response get_battery_status")
        return
    buffer = res[0]
    batt_string = int.from_bytes(buffer[4:5], byteorder="big", signed=False)
    temp = int.from_bytes(buffer[5:6], byteorder="big", signed=False)
    charger = int.from_bytes(buffer[6:7], byteorder="big", signed=False) == 1
    load = int.from_bytes(buffer[7:8], byteorder="big", signed=False) == 1
    cycles = int.from_bytes(buffer[9:11], byteorder="big", signed=False)
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


def get_battery_temp():
    res = cmd(b"\xa5\x40\x92\x08\x00\x00\x00\x00\x00\x00\x00\x00\x7f")
    if not res:
        print("Empty response get_battery_temp")
        return
    buffer = res[0]
    max_temp = int.from_bytes(buffer[4:5], byteorder="big", signed=False) - 40
    max_temp_cell = int.from_bytes(buffer[5:6], byteorder="big", signed=False)
    min_temp = int.from_bytes(buffer[6:7], byteorder="big", signed=False) - 40
    min_temp_cell = int.from_bytes(buffer[7:8], byteorder="big", signed=False)
    publish(
        f"{TEMP_TOPIC}/state",
        {
            "value": (max_temp + min_temp) / 2,
            "maxTemp": max_temp,
            "maxTempCell": max_temp_cell,
            "minTemp": min_temp,
            "minTempCell": min_temp_cell,
        },
    )


def get_battery_mos_status():
    res = cmd(b"\xa5\x40\x93\x08\x00\x00\x00\x00\x00\x00\x00\x00\x80")
    if not res:
        print("Empty response get_battery_mos_status")
        return
    buffer = res[0]
    value_byte = int.from_bytes(buffer[4:5], byteorder="big", signed=False)
    value = "discharging" if value_byte == 2 else ("charging" if value_byte == 1 else "idle")
    charge_mos = int.from_bytes(buffer[5:6], byteorder="big", signed=False)
    discharge_mos = int.from_bytes(buffer[6:7], byteorder="big", signed=False)
    bms_life = int.from_bytes(buffer[7:8], byteorder="big", signed=False)
    residual_capacity = int.from_bytes(buffer[8:12], byteorder="big", signed=False)
    publish(
        f"{MOS_TOPIC}/state",
        {
            "value": value,
            "chargingMOS": charge_mos,
            "dischargingMOS": discharge_mos,
            "BMSLife": bms_life,
            "residualCapacity": residual_capacity,
        },
    )


publish_discovery()

try:
    while True:
        get_battery_state()
        get_cell_balance(CELL_COUNT)
        get_battery_status()
        get_battery_temp()
        get_battery_mos_status()
        time.sleep(POLL_INTERVAL_SECONDS)
finally:
    try:
        ser.close()
    finally:
        client.loop_stop()
        client.disconnect()
