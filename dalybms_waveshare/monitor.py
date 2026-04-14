#!/usr/bin/env python3
import datetime as dt
import json
import logging
import os
import socket
import struct
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import paho.mqtt.client as mqtt

print("=== monitor.py started (modbus block mode) ===", flush=True)


def env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


DEBUG_LOG_ENABLED = env_flag("DEBUG_LOG_ENABLED", False)
LOG_LEVEL = logging.DEBUG if DEBUG_LOG_ENABLED else logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
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
ENABLE_WRITE_COMMANDS = env_flag("ENABLE_WRITE_COMMANDS", False)
WRITE_COMMAND_TOPIC = os.environ.get("WRITE_COMMAND_TOPIC", "").strip()
WRITE_RESULT_TOPIC = os.environ.get("WRITE_RESULT_TOPIC", "").strip()
WRITE_ALLOWED_REGISTERS_RAW = os.environ.get(
    "WRITE_ALLOWED_REGISTERS",
    "9,10,11,12,33,34,64,65,69,70,265,266,267,268,289,290,320,321,325,326,503,504",
).strip()

DEFAULT_WRITE_ALLOWED_REGISTERS = {
    9,
    10,
    11,
    12,
    33,
    34,
    64,
    65,
    69,
    70,
    265,
    266,
    267,
    268,
    289,
    290,
    320,
    321,
    325,
    326,
    503,
    504,
}


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


def parse_allowed_write_registers(raw_value: str) -> Set[int]:
    allowed: Set[int] = set()
    for token in raw_value.replace(";", ",").split(","):
        value = token.strip()
        if not value:
            continue
        if not value.isdigit():
            log.warning("Ignoring invalid write register token: %r", token)
            continue
        register = int(value)
        if not 0 <= register <= 65535:
            log.warning("Ignoring out-of-range write register: %s", register)
            continue
        allowed.add(register)
    if not allowed:
        return set(DEFAULT_WRITE_ALLOWED_REGISTERS)
    return allowed


CONNECTION_HOST, CONNECTION_PORT = resolve_connection_target(DEVICE, MODBUS_PORT)

log.info("Starting Daly WNT block monitor")
log.info("DEVICE_ID=%s", DEVICE_ID)
log.info("CELL_COUNT=%s", CELL_COUNT)
log.info("POLL_INTERVAL_SECONDS=%s", POLL_INTERVAL_SECONDS)
log.info("MQTT broker configured")
log.info("MQTT_CLIENT_ID=%s", MQTT_CLIENT_ID)
log.info("MQTT_DISCOVERY_PREFIX=%s", MQTT_DISCOVERY_PREFIX)
log.info("NOMINAL_CAPACITY_AH=%s", NOMINAL_CAPACITY_AH)
log.info("MODBUS_PORT=%s", MODBUS_PORT)
log.info("MODBUS_UNIT_ID=%s", MODBUS_UNIT_ID)
log.info("MODBUS_START=%s", MODBUS_START)
log.info("MODBUS_COUNT=%s", MODBUS_COUNT)
log.info("Modbus target configured")
log.info("CONNECTION_PORT=%s", CONNECTION_PORT)
log.info("DEBUG_LOG_ENABLED=%s", DEBUG_LOG_ENABLED)

BASE_TOPIC = f"{MQTT_DISCOVERY_PREFIX}/sensor/"
BINARY_BASE_TOPIC = f"{MQTT_DISCOVERY_PREFIX}/binary_sensor/"
SWITCH_BASE_TOPIC = f"{MQTT_DISCOVERY_PREFIX}/switch/"
NUMBER_BASE_TOPIC = f"{MQTT_DISCOVERY_PREFIX}/number/"
STATE_TOPIC = f"{BASE_TOPIC}{DEVICE_ID}"
STATUS_TOPIC = f"{STATE_TOPIC}_status"
CELLS_TOPIC = f"{STATE_TOPIC}_balance"
TEMP_TOPIC = f"{STATE_TOPIC}_temp"
MOS_TOPIC = f"{STATE_TOPIC}_mos"
RAW_TOPIC = f"{STATE_TOPIC}_raw"
DEBUG_TOPIC = f"{STATE_TOPIC}_debug"
CONTROL_TOPIC = f"{STATE_TOPIC}_control"

REG_RATED_CAPACITY_HIGH = 265
REG_RATED_CAPACITY_LOW = 266
REG_ACTUAL_CAPACITY_HIGH = 267
REG_ACTUAL_CAPACITY_LOW = 268
REG_CHARGE_MOS_CONTROL = 289
REG_DISCHARGE_MOS_CONTROL = 290
REG_MAX_CHARGE_CURRENT_LEVEL_1 = 320
REG_MAX_CHARGE_CURRENT_LEVEL_2 = 321
REG_MAX_DISCHARGE_CURRENT_LEVEL_1 = 325
REG_MAX_DISCHARGE_CURRENT_LEVEL_2 = 326

REG_RATED_CAPACITY_HIGH_BASE = 9
REG_RATED_CAPACITY_LOW_BASE = 10
REG_ACTUAL_CAPACITY_HIGH_BASE = 11
REG_ACTUAL_CAPACITY_LOW_BASE = 12
REG_CHARGE_MOS_CONTROL_BASE = 33
REG_DISCHARGE_MOS_CONTROL_BASE = 34
REG_MAX_CHARGE_CURRENT_LEVEL_1_BASE = 64
REG_MAX_CHARGE_CURRENT_LEVEL_2_BASE = 65
REG_MAX_DISCHARGE_CURRENT_LEVEL_1_BASE = 69
REG_MAX_DISCHARGE_CURRENT_LEVEL_2_BASE = 70

CONTROL_REG_START = REG_RATED_CAPACITY_HIGH
CONTROL_REG_END = REG_MAX_DISCHARGE_CURRENT_LEVEL_2
CONTROL_REG_COUNT = CONTROL_REG_END - CONTROL_REG_START + 1

CONTROL_REG_START_BASE = REG_RATED_CAPACITY_HIGH_BASE
CONTROL_REG_END_BASE = REG_MAX_DISCHARGE_CURRENT_LEVEL_2_BASE
CONTROL_REG_COUNT_BASE = CONTROL_REG_END_BASE - CONTROL_REG_START_BASE + 1

CHARGE_MOS_SET_TOPIC = f"{STATE_TOPIC}/set/charge_mos_control"
DISCHARGE_MOS_SET_TOPIC = f"{STATE_TOPIC}/set/discharge_mos_control"
RATED_CAPACITY_SET_TOPIC = f"{STATE_TOPIC}/set/rated_capacity_ah"
ACTUAL_CAPACITY_SET_TOPIC = f"{STATE_TOPIC}/set/actual_capacity_ah"
MAX_CHARGE_CURRENT_LEVEL_1_SET_TOPIC = f"{STATE_TOPIC}/set/max_charge_current_level_1"
MAX_CHARGE_CURRENT_LEVEL_2_SET_TOPIC = f"{STATE_TOPIC}/set/max_charge_current_level_2"
MAX_DISCHARGE_CURRENT_LEVEL_1_SET_TOPIC = f"{STATE_TOPIC}/set/max_discharge_current_level_1"
MAX_DISCHARGE_CURRENT_LEVEL_2_SET_TOPIC = f"{STATE_TOPIC}/set/max_discharge_current_level_2"

if not WRITE_COMMAND_TOPIC:
    WRITE_COMMAND_TOPIC = f"{STATE_TOPIC}/set/write"
if not WRITE_RESULT_TOPIC:
    WRITE_RESULT_TOPIC = f"{STATE_TOPIC}/write_result"
ALLOWED_WRITE_REGISTERS = parse_allowed_write_registers(WRITE_ALLOWED_REGISTERS_RAW)

log.info("ENABLE_WRITE_COMMANDS=%s", ENABLE_WRITE_COMMANDS)
if ENABLE_WRITE_COMMANDS:
    log.info("WRITE_COMMAND_TOPIC=%s", WRITE_COMMAND_TOPIC)
    log.info("WRITE_RESULT_TOPIC=%s", WRITE_RESULT_TOPIC)
    log.info("WRITE_ALLOWED_REGISTERS=%s", sorted(ALLOWED_WRITE_REGISTERS))


client = mqtt.Client(client_id=MQTT_CLIENT_ID)
client.username_pw_set(MQTT_USER, MQTT_PASS)
modbus: Optional["ModbusTcpClient"] = None
active_control_profile = "offset"


def on_connect(client, userdata, flags, rc):
    log.info("MQTT connected with result code: %s", rc)
    if rc == 0 and ENABLE_WRITE_COMMANDS:
        command_topics = [
            WRITE_COMMAND_TOPIC,
            CHARGE_MOS_SET_TOPIC,
            DISCHARGE_MOS_SET_TOPIC,
            RATED_CAPACITY_SET_TOPIC,
            ACTUAL_CAPACITY_SET_TOPIC,
            MAX_CHARGE_CURRENT_LEVEL_1_SET_TOPIC,
            MAX_CHARGE_CURRENT_LEVEL_2_SET_TOPIC,
            MAX_DISCHARGE_CURRENT_LEVEL_1_SET_TOPIC,
            MAX_DISCHARGE_CURRENT_LEVEL_2_SET_TOPIC,
        ]
        for topic in command_topics:
            result, mid = client.subscribe(topic, qos=0)
            log.info("Subscribed command topic=%s result=%s mid=%s", topic, result, mid)


def on_disconnect(client, userdata, rc):
    log.warning("MQTT disconnected with result code: %s", rc)


def on_message(client, userdata, message):
    if not ENABLE_WRITE_COMMANDS:
        return
    if message.retain:
        log.warning("Ignoring retained command on topic=%s", message.topic)
        return
    if message.topic == WRITE_COMMAND_TOPIC:
        handle_mqtt_write_command(message.payload)
        return
    handle_simple_control_command(message.topic, message.payload)


client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

ALARM_TEXTS = [
    "Single overcharge alarm",
    "Single voltage high level 2",
    "Single over-discharge alarm",
    "Single voltage low level 2",
    "Total overcharge alarm",
    "Total voltage high level 2",
    "Total over-discharge alarm",
    "Total voltage low level 2",
    "Charging high temperature alarm",
    "Charge temperature high level 2",
    "Charging low temperature alarm",
    "Charge temperature low level 2",
    "Discharge high temperature alarm",
    "Discharge temperature high level 2",
    "Discharge low temperature alarm",
    "Discharge temperature low level 2",
    "Charging overcurrent alarm",
    "Charge current high level 2",
    "Discharge overcurrent alarm",
    "Discharge current high level 2",
    "SOC too high alarm",
    "SOC high level 2",
    "SOC too low alarm",
    "SOC low level 2",
    "Large voltage difference alarm",
    "Voltage difference high level 2",
    "Large temperature difference alarm",
    "Temperature difference high level 2",
    "MOS temperature too high alarm",
    "MOS temperature high level 2",
    "Ambient temperature too high alarm",
    "Ambient temperature high level 2",
    "Charging MOS overheated",
    "Discharge MOS overheated",
    "MOS temperature too high",
    "Ambient temperature too high",
    "Charging MOS adhesion fault",
    "Discharge MOS adhesion fault",
    "Charging MOS open fault",
    "Discharge MOS open fault",
    "AFE IC error",
    "Cell open wire",
    "Cell temperature detect error",
    "EEPROM error",
    "RTC error",
    "Pre-charge failed",
    "Vehicle communication failure",
    "Internal communication failure",
    "Current module fault",
    "Internal voltage detect fault",
    "Short circuit",
    "Low voltage prohibit charging fault",
    "Switch control MOS",
    "Charging cabinet offline",
    "Thermal runaway fault",
    "Heating fault",
    "Balance module communication fault",
    "Balance condition not met",
    "Voltage sample abnormal",
    "Battery fully charged",
    "Error code 16",
    "Error code 32",
    "Error code 64",
    "Error code 128",
]

SPECIAL_ALARM_TEXTS = [
    "Charge current high level 3",
    "Discharge current high level 3",
    "Voltage difference high level 3",
]

FAULT_LEVEL_BASE_TEXTS = [
    "single voltage high",
    "single voltage low",
    "voltage diff high",
    "charging temp high",
    "charging temp low",
    "discharging temp high",
    "discharging temp low",
    "temp diff high",
    "total voltage high",
    "total voltage low",
    "charging current high",
    "discharging current high",
    "SOC low",
    "SOH low",
    "MOS temp high",
    "thermal runaway",
    "ambient temperature",
    "error2",
    "error3",
    "error4",
    "error5",
    "error6",
]

FAULT_BIT_TEXTS = [
    "smart charger connection",
    "smart charger disconnection",
    "smart discharger connection",
    "smart discharger disconnection",
    "charging MOS temp high",
    "charging MOS temp detection failure",
    "discharging MOS temp high",
    "discharging MOS temp detection failure",
    "short circuit protection",
    "upgrade sign",
    "low voltage prohibit charging",
    "high voltage prohibit discharging",
    "intranet parallel comm ok",
    "intranet parallel comm fail",
    "BLE communication error",
    "program inconsistent BMS",
    "balance module communication error",
    "balance opening condition not met",
    "battery fully charged",
    "error code Byte9 Bit7",
    "error code Byte10 Bit6",
    "error code Byte10 Bit7",
    "AFE IC fault",
    "AFE IC communication fault",
    "AFE IC AD fault",
    "voltage acquisition failure",
    "voltage acquisition line disconnected",
    "total voltage detection failure",
    "current detection failure",
    "temperature detection failure",
    "temperature acquisition line disconnected",
    "EEPROM fault",
    "FLASH fault",
    "RTC fault",
    "charge MOS fault",
    "discharge MOS fault",
    "pre-charge MOS fault",
    "pre-charge failed",
    "communication command turned off charge MOS",
    "communication command turned off discharge MOS",
    "key turned off charge MOS",
    "key turned off discharge MOS",
    "fan work",
    "heat work",
    "current limiting module works",
    "heating fault",
    "heating status",
    "DMOS force on status",
    "full battery charge",
    "balance module communication fault",
]


def publish(topic: str, payload: dict, retain: bool = False):
    try:
        payload_json = json.dumps(payload)
        log.debug("MQTT publish topic=%s retain=%s payload=%s", topic, retain, payload_json)
        info = client.publish(topic, payload_json, qos=0, retain=retain)
        log.debug("MQTT publish result rc=%s mid=%s", info.rc, info.mid)
    except Exception:
        log.exception("Failed to publish MQTT topic=%s payload=%s", topic, payload)


def build_sensor_discovery(
    name: str,
    unique_suffix: str,
    state_topic: str,
    field: str,
    device: dict,
    *,
    unit: Optional[str] = None,
    device_class: Optional[str] = None,
    suggested_display_precision: Optional[int] = None,
    entity_category: Optional[str] = None,
    icon: Optional[str] = None,
    json_attributes_topic: Optional[str] = None,
    value_template: Optional[str] = None,
) -> dict:
    payload = {
        "name": name,
        "state_topic": state_topic,
        "value_template": value_template or f"{{{{ value_json.{field} }}}}",
        "unique_id": f"{DEVICE_ID}_{unique_suffix}",
        "device": device,
    }
    if unit is not None:
        payload["unit_of_measurement"] = unit
    if device_class is not None:
        payload["device_class"] = device_class
    if suggested_display_precision is not None:
        payload["suggested_display_precision"] = suggested_display_precision
    if entity_category is not None:
        payload["entity_category"] = entity_category
    if icon is not None:
        payload["icon"] = icon
    if json_attributes_topic is not None:
        payload["json_attributes_topic"] = json_attributes_topic
    return payload


def build_binary_sensor_discovery(
    name: str,
    unique_suffix: str,
    state_topic: str,
    field: str,
    device: dict,
    *,
    entity_category: Optional[str] = None,
    icon: Optional[str] = None,
    value_template: Optional[str] = None,
) -> dict:
    payload = {
        "name": name,
        "state_topic": state_topic,
        "value_template": value_template or f"{{{{ 'ON' if value_json.{field} else 'OFF' }}}}",
        "payload_on": "ON",
        "payload_off": "OFF",
        "unique_id": f"{DEVICE_ID}_{unique_suffix}",
        "device": device,
    }
    if entity_category is not None:
        payload["entity_category"] = entity_category
    if icon is not None:
        payload["icon"] = icon
    return payload


def build_switch_discovery(
    name: str,
    unique_suffix: str,
    state_topic: str,
    state_field: str,
    command_topic: str,
    device: dict,
    *,
    icon: Optional[str] = None,
) -> dict:
    payload = {
        "name": name,
        "unique_id": f"{DEVICE_ID}_{unique_suffix}",
        "state_topic": state_topic,
        "command_topic": command_topic,
        "value_template": f"{{{{ 'ON' if value_json.{state_field} else 'OFF' }}}}",
        "state_on": "ON",
        "state_off": "OFF",
        "payload_on": "ON",
        "payload_off": "OFF",
        "device": device,
    }
    if icon is not None:
        payload["icon"] = icon
    return payload


def build_number_discovery(
    name: str,
    unique_suffix: str,
    state_topic: str,
    state_field: str,
    command_topic: str,
    device: dict,
    *,
    unit: Optional[str] = None,
    min_value: float = 0.0,
    max_value: float = 1000.0,
    step: float = 1.0,
    suggested_display_precision: Optional[int] = None,
    icon: Optional[str] = None,
) -> dict:
    payload = {
        "name": name,
        "unique_id": f"{DEVICE_ID}_{unique_suffix}",
        "state_topic": state_topic,
        "command_topic": command_topic,
        "value_template": f"{{{{ value_json.{state_field} }}}}",
        "command_template": "{{ value }}",
        "min": min_value,
        "max": max_value,
        "step": step,
        "mode": "box",
        "device": device,
    }
    if unit is not None:
        payload["unit_of_measurement"] = unit
    if suggested_display_precision is not None:
        payload["suggested_display_precision"] = suggested_display_precision
    if icon is not None:
        payload["icon"] = icon
    return payload


def publish_discovery():
    device_conf = {
        "manufacturer": "Dongguan Daly Electronics / WNT",
        "name": "Smart BMS",
        "identifiers": [DEVICE_ID],
    }

    discovery = {
        f"{STATE_TOPIC}_soc/config": build_sensor_discovery(
            "SOC",
            "soc",
            f"{STATE_TOPIC}/state",
            "soc",
            device_conf,
            unit="%",
            device_class="battery",
            suggested_display_precision=1,
            json_attributes_topic=f"{DEBUG_TOPIC}/state",
        ),
        f"{STATE_TOPIC}_voltage/config": build_sensor_discovery(
            "Voltage",
            "voltage",
            f"{STATE_TOPIC}/state",
            "voltage",
            device_conf,
            unit="V",
            device_class="voltage",
            suggested_display_precision=1,
        ),
        f"{STATE_TOPIC}_current/config": build_sensor_discovery(
            "Current",
            "current",
            f"{STATE_TOPIC}/state",
            "current",
            device_conf,
            unit="A",
            device_class="current",
            suggested_display_precision=1,
        ),
        f"{STATE_TOPIC}_power/config": build_sensor_discovery(
            "Power",
            "power",
            f"{STATE_TOPIC}/state",
            "power",
            device_conf,
            unit="W",
            device_class="power",
            suggested_display_precision=0,
        ),
        f"{STATE_TOPIC}_remaining_ah/config": build_sensor_discovery(
            "Charge",
            "remaining_charge_ah",
            f"{STATE_TOPIC}/state",
            "remaining_charge_ah",
            device_conf,
            unit="Ah",
            suggested_display_precision=2,
        ),
        f"{STATE_TOPIC}_remaining_capacity_ah/config": build_sensor_discovery(
            "Remaining Capacity",
            "remaining_capacity_ah",
            f"{STATE_TOPIC}/state",
            "remaining_capacity_ah",
            device_conf,
            unit="Ah",
            suggested_display_precision=1,
        ),
        f"{STATE_TOPIC}_backup_current/config": build_sensor_discovery(
            "Backup Current",
            "backup_current",
            f"{STATE_TOPIC}/state",
            "backup_current",
            device_conf,
            unit="A",
            device_class="current",
            suggested_display_precision=1,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_bms_life/config": build_sensor_discovery(
            "BMS Life",
            "bms_life",
            f"{STATE_TOPIC}/state",
            "bms_life",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_detected_cell_count/config": build_sensor_discovery(
            "Detected Cell Count",
            "detected_cell_count",
            f"{STATE_TOPIC}/state",
            "detected_cell_count",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_detected_ntc_count/config": build_sensor_discovery(
            "Detected Temp Sensors",
            "detected_ntc_count",
            f"{STATE_TOPIC}/state",
            "detected_ntc_count",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_cycle_time/config": build_sensor_discovery(
            "Cycle Count",
            "cycle_time",
            f"{STATE_TOPIC}/state",
            "cycle_time",
            device_conf,
        ),
        f"{STATE_TOPIC}_max_temperature/config": build_sensor_discovery(
            "Max Temperature",
            "max_temperature",
            f"{STATE_TOPIC}/state",
            "max_temperature",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
        ),
        f"{STATE_TOPIC}_max_temperature_sensor/config": build_sensor_discovery(
            "Max Temp Sensor",
            "max_temperature_sensor",
            f"{STATE_TOPIC}/state",
            "max_temperature_sensor",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_min_temperature/config": build_sensor_discovery(
            "Min Temperature",
            "min_temperature",
            f"{STATE_TOPIC}/state",
            "min_temperature",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
        ),
        f"{STATE_TOPIC}_min_temperature_sensor/config": build_sensor_discovery(
            "Min Temp Sensor",
            "min_temperature_sensor",
            f"{STATE_TOPIC}/state",
            "min_temperature_sensor",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_temperature_diff/config": build_sensor_discovery(
            "Temperature Diff",
            "temperature_diff",
            f"{STATE_TOPIC}/state",
            "temperature_diff",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
        ),
        f"{STATE_TOPIC}_mos_temperature/config": build_sensor_discovery(
            "MOS Temperature",
            "mos_temperature",
            f"{STATE_TOPIC}/state",
            "mos_temperature",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
        ),
        f"{STATE_TOPIC}_board_temperature/config": build_sensor_discovery(
            "Board Temperature",
            "board_temperature",
            f"{STATE_TOPIC}/state",
            "board_temperature",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
        ),
        f"{STATE_TOPIC}_heat_temperature/config": build_sensor_discovery(
            "Heat Temperature",
            "heat_temperature",
            f"{STATE_TOPIC}/state",
            "heat_temperature",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
        ),
        f"{STATE_TOPIC}_heat_current/config": build_sensor_discovery(
            "Heat Current",
            "heat_current",
            f"{STATE_TOPIC}/state",
            "heat_current",
            device_conf,
            unit="A",
            device_class="current",
            suggested_display_precision=0,
        ),
        f"{STATE_TOPIC}_limit_state/config": build_sensor_discovery(
            "Limit State",
            "limit_state",
            f"{STATE_TOPIC}/state",
            "limit_state",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_limit_current/config": build_sensor_discovery(
            "Limit Current",
            "limit_current",
            f"{STATE_TOPIC}/state",
            "limit_current",
            device_conf,
            unit="A",
            device_class="current",
            suggested_display_precision=1,
        ),
        f"{STATE_TOPIC}_charge_full_time/config": build_sensor_discovery(
            "Charge Full Time",
            "charge_full_time",
            f"{STATE_TOPIC}/state",
            "charge_full_time",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_wakeup_source/config": build_sensor_discovery(
            "Wakeup Source",
            "wakeup_source",
            f"{STATE_TOPIC}/state",
            "wakeup_source",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_pwm_duty/config": build_sensor_discovery(
            "PWM Duty",
            "pwm_duty",
            f"{STATE_TOPIC}/state",
            "pwm_duty",
            device_conf,
            unit="%",
            suggested_display_precision=1,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_pwm_voltage/config": build_sensor_discovery(
            "PWM Voltage",
            "pwm_voltage",
            f"{STATE_TOPIC}/state",
            "pwm_voltage",
            device_conf,
            unit="V",
            device_class="voltage",
            suggested_display_precision=1,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_rtc/config": build_sensor_discovery(
            "RTC",
            "rtc",
            f"{STATE_TOPIC}/state",
            "rtc",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_battery_status/config": build_sensor_discovery(
            "Battery Status",
            "battery_status",
            f"{STATE_TOPIC}/state",
            "battery_status",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_charge_detect/config": build_sensor_discovery(
            "Charge Detect",
            "charge_detect",
            f"{STATE_TOPIC}/state",
            "charge_detect",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{STATE_TOPIC}_load_detect/config": build_sensor_discovery(
            "Load Detect",
            "load_detect",
            f"{STATE_TOPIC}/state",
            "load_detect",
            device_conf,
            entity_category="diagnostic",
        ),
        f"{CELLS_TOPIC}/config": build_sensor_discovery(
            "Cell Volt delta",
            "cell_delta",
            f"{CELLS_TOPIC}/state",
            "diff",
            device_conf,
            unit="V",
            device_class="voltage",
            suggested_display_precision=3,
            json_attributes_topic=f"{CELLS_TOPIC}/state",
        ),
        f"{CELLS_TOPIC}_avg/config": build_sensor_discovery(
            "Cell Volt average",
            "cell_avg",
            f"{CELLS_TOPIC}/state",
            "avg",
            device_conf,
            unit="V",
            device_class="voltage",
            suggested_display_precision=3,
        ),
        f"{CELLS_TOPIC}_min/config": build_sensor_discovery(
            "Cell Volt min",
            "cell_min",
            f"{CELLS_TOPIC}/state",
            "min",
            device_conf,
            unit="V",
            device_class="voltage",
            suggested_display_precision=3,
        ),
        f"{CELLS_TOPIC}_max/config": build_sensor_discovery(
            "Cell Volt max",
            "cell_max",
            f"{CELLS_TOPIC}/state",
            "max",
            device_conf,
            unit="V",
            device_class="voltage",
            suggested_display_precision=3,
        ),
        f"{CELLS_TOPIC}_min_idx/config": build_sensor_discovery(
            "Cell Index min",
            "cell_min_index",
            f"{CELLS_TOPIC}/state",
            "minCell",
            device_conf,
        ),
        f"{CELLS_TOPIC}_max_idx/config": build_sensor_discovery(
            "Cell Index max",
            "cell_max_index",
            f"{CELLS_TOPIC}/state",
            "maxCell",
            device_conf,
        ),
        f"{CELLS_TOPIC}_balance_count/config": build_sensor_discovery(
            "Balance Active Cells",
            "balance_active_cell_count",
            f"{CELLS_TOPIC}/state",
            "balance_active_cell_count",
            device_conf,
        ),
        f"{TEMP_TOPIC}/config": build_sensor_discovery(
            "Temperature 1",
            "temperature_1",
            f"{TEMP_TOPIC}/state",
            "temperature_1",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
            json_attributes_topic=f"{TEMP_TOPIC}/state",
        ),
        f"{TEMP_TOPIC}_2/config": build_sensor_discovery(
            "Temperature 2",
            "temperature_2",
            f"{TEMP_TOPIC}/state",
            "temperature_2",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
        ),
        f"{TEMP_TOPIC}_max/config": build_sensor_discovery(
            "Temp Max",
            "temperature_max",
            f"{TEMP_TOPIC}/state",
            "max_temperature",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
        ),
        f"{TEMP_TOPIC}_min/config": build_sensor_discovery(
            "Temp Min",
            "temperature_min",
            f"{TEMP_TOPIC}/state",
            "min_temperature",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
        ),
        f"{TEMP_TOPIC}_diff/config": build_sensor_discovery(
            "Temp Diff",
            "temperature_diff_register",
            f"{TEMP_TOPIC}/state",
            "temperature_diff",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
        ),
        f"{STATUS_TOPIC}_alarm_count/config": build_sensor_discovery(
            "Active Alarms",
            "active_alarm_count",
            f"{STATUS_TOPIC}/state",
            "active_alarm_count",
            device_conf,
            entity_category="diagnostic",
            json_attributes_topic=f"{STATUS_TOPIC}/state",
        ),
        f"{STATUS_TOPIC}_fault_count/config": build_sensor_discovery(
            "Active Faults",
            "active_fault_count",
            f"{STATUS_TOPIC}/state",
            "active_fault_count",
            device_conf,
            entity_category="diagnostic",
            json_attributes_topic=f"{STATUS_TOPIC}/state",
        ),
        f"{RAW_TOPIC}/config": build_sensor_discovery(
            "Raw Block Hex",
            "raw_hex",
            f"{RAW_TOPIC}/state",
            "prefix",
            device_conf,
            entity_category="diagnostic",
            json_attributes_topic=f"{RAW_TOPIC}/state",
        ),
        f"{BINARY_BASE_TOPIC}{DEVICE_ID}_charge_mos/config": build_binary_sensor_discovery(
            "Charge MOS",
            "charge_mos_on",
            f"{MOS_TOPIC}/state",
            "charge_mos_on",
            device_conf,
        ),
        f"{BINARY_BASE_TOPIC}{DEVICE_ID}_discharge_mos/config": build_binary_sensor_discovery(
            "Discharge MOS",
            "discharge_mos_on",
            f"{MOS_TOPIC}/state",
            "discharge_mos_on",
            device_conf,
        ),
        f"{BINARY_BASE_TOPIC}{DEVICE_ID}_precharge_mos/config": build_binary_sensor_discovery(
            "Precharge MOS",
            "precharge_mos_on",
            f"{MOS_TOPIC}/state",
            "precharge_mos_on",
            device_conf,
        ),
        f"{BINARY_BASE_TOPIC}{DEVICE_ID}_heat_mos/config": build_binary_sensor_discovery(
            "Heat MOS",
            "heat_mos_on",
            f"{MOS_TOPIC}/state",
            "heat_mos_on",
            device_conf,
        ),
        f"{BINARY_BASE_TOPIC}{DEVICE_ID}_fan_mos/config": build_binary_sensor_discovery(
            "Fan MOS",
            "fan_mos_on",
            f"{MOS_TOPIC}/state",
            "fan_mos_on",
            device_conf,
        ),
        f"{BINARY_BASE_TOPIC}{DEVICE_ID}_balancing/config": build_binary_sensor_discovery(
            "Balancing Active",
            "balancing_active",
            f"{CELLS_TOPIC}/state",
            "balancing_active",
            device_conf,
        ),
        f"{BINARY_BASE_TOPIC}{DEVICE_ID}_alarm_active/config": build_binary_sensor_discovery(
            "Alarm Active",
            "alarm_active",
            f"{STATUS_TOPIC}/state",
            "active_alarm_count",
            device_conf,
            entity_category="diagnostic",
            value_template="{{ 'ON' if (value_json.active_alarm_count | int) > 0 else 'OFF' }}",
        ),
        f"{BINARY_BASE_TOPIC}{DEVICE_ID}_fault_active/config": build_binary_sensor_discovery(
            "Fault Active",
            "fault_active",
            f"{STATUS_TOPIC}/state",
            "active_fault_count",
            device_conf,
            entity_category="diagnostic",
            value_template="{{ 'ON' if (value_json.active_fault_count | int) > 0 else 'OFF' }}",
        ),
    }

    for cell_index in range(1, CELL_COUNT + 1):
        discovery[f"{CELLS_TOPIC}_{cell_index:02d}/config"] = build_sensor_discovery(
            f"Cell Volt {cell_index:02d}",
            f"cell_{cell_index:02d}",
            f"{CELLS_TOPIC}/state",
            f"cell_{cell_index}",
            device_conf,
            unit="V",
            device_class="voltage",
            suggested_display_precision=3,
        )

    for temp_index in range(3, 9):
        discovery[f"{TEMP_TOPIC}_{temp_index}/config"] = build_sensor_discovery(
            f"Temperature {temp_index}",
            f"temperature_{temp_index}",
            f"{TEMP_TOPIC}/state",
            f"temperature_{temp_index}",
            device_conf,
            unit="°C",
            device_class="temperature",
            suggested_display_precision=1,
        )

    if ENABLE_WRITE_COMMANDS:
        discovery[f"{SWITCH_BASE_TOPIC}{DEVICE_ID}_charge_mos_control/config"] = build_switch_discovery(
            "Charge MOS Control",
            "charge_mos_control",
            f"{CONTROL_TOPIC}/state",
            "charge_mos_control",
            CHARGE_MOS_SET_TOPIC,
            device_conf,
            icon="mdi:car-battery",
        )
        discovery[f"{SWITCH_BASE_TOPIC}{DEVICE_ID}_discharge_mos_control/config"] = build_switch_discovery(
            "Discharge MOS Control",
            "discharge_mos_control",
            f"{CONTROL_TOPIC}/state",
            "discharge_mos_control",
            DISCHARGE_MOS_SET_TOPIC,
            device_conf,
            icon="mdi:car-battery",
        )
        discovery[f"{NUMBER_BASE_TOPIC}{DEVICE_ID}_rated_capacity_ah/config"] = build_number_discovery(
            "Rated Capacity",
            "rated_capacity_ah",
            f"{CONTROL_TOPIC}/state",
            "rated_capacity_ah",
            RATED_CAPACITY_SET_TOPIC,
            device_conf,
            unit="Ah",
            min_value=0.0,
            max_value=10000.0,
            step=0.1,
            suggested_display_precision=1,
            icon="mdi:battery-high",
        )
        discovery[f"{NUMBER_BASE_TOPIC}{DEVICE_ID}_actual_capacity_ah/config"] = build_number_discovery(
            "Actual Capacity",
            "actual_capacity_ah",
            f"{CONTROL_TOPIC}/state",
            "actual_capacity_ah",
            ACTUAL_CAPACITY_SET_TOPIC,
            device_conf,
            unit="Ah",
            min_value=0.0,
            max_value=10000.0,
            step=0.1,
            suggested_display_precision=1,
            icon="mdi:battery-70",
        )
        discovery[f"{NUMBER_BASE_TOPIC}{DEVICE_ID}_max_charge_current_level_1/config"] = build_number_discovery(
            "Max Charge Current L1",
            "max_charge_current_level_1",
            f"{CONTROL_TOPIC}/state",
            "max_charge_current_level_1",
            MAX_CHARGE_CURRENT_LEVEL_1_SET_TOPIC,
            device_conf,
            unit="A",
            min_value=0.1,
            max_value=3000.0,
            step=0.1,
            suggested_display_precision=1,
            icon="mdi:current-dc",
        )
        discovery[f"{NUMBER_BASE_TOPIC}{DEVICE_ID}_max_charge_current_level_2/config"] = build_number_discovery(
            "Max Charge Current L2",
            "max_charge_current_level_2",
            f"{CONTROL_TOPIC}/state",
            "max_charge_current_level_2",
            MAX_CHARGE_CURRENT_LEVEL_2_SET_TOPIC,
            device_conf,
            unit="A",
            min_value=0.1,
            max_value=3000.0,
            step=0.1,
            suggested_display_precision=1,
            icon="mdi:current-dc",
        )
        discovery[
            f"{NUMBER_BASE_TOPIC}{DEVICE_ID}_max_discharge_current_level_1/config"
        ] = build_number_discovery(
            "Max Discharge Current L1",
            "max_discharge_current_level_1",
            f"{CONTROL_TOPIC}/state",
            "max_discharge_current_level_1",
            MAX_DISCHARGE_CURRENT_LEVEL_1_SET_TOPIC,
            device_conf,
            unit="A",
            min_value=0.1,
            max_value=3000.0,
            step=0.1,
            suggested_display_precision=1,
            icon="mdi:current-dc",
        )
        discovery[
            f"{NUMBER_BASE_TOPIC}{DEVICE_ID}_max_discharge_current_level_2/config"
        ] = build_number_discovery(
            "Max Discharge Current L2",
            "max_discharge_current_level_2",
            f"{CONTROL_TOPIC}/state",
            "max_discharge_current_level_2",
            MAX_DISCHARGE_CURRENT_LEVEL_2_SET_TOPIC,
            device_conf,
            unit="A",
            min_value=0.1,
            max_value=3000.0,
            step=0.1,
            suggested_display_precision=1,
            icon="mdi:current-dc",
        )

    deprecated_discovery_topics = [
        f"{STATE_TOPIC}_afe_current/config",
        f"{STATE_TOPIC}_afe_factor/config",
        f"{STATE_TOPIC}_afe_offset/config",
        f"{STATE_TOPIC}_afe_adc/config",
        f"{STATE_TOPIC}_do_state/config",
        f"{STATE_TOPIC}_di_state/config",
        f"{STATE_TOPIC}_serial_port_type/config",
        f"{MOS_TOPIC}/config",
    ]

    # Remove deprecated entities from Home Assistant by clearing retained discovery config.
    for topic in deprecated_discovery_topics:
        try:
            info = client.publish(topic, payload="", qos=0, retain=True)
            log.debug("MQTT clear discovery topic=%s rc=%s mid=%s", topic, info.rc, info.mid)
        except Exception:
            log.exception("Failed to clear deprecated discovery topic=%s", topic)

    log.info("Publishing MQTT discovery for %d entities", len(discovery))
    for topic, payload in discovery.items():
        publish(topic, payload, retain=True)


class ModbusTcpClient:
    def __init__(self, host: str, port: int, timeout: float = 3):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.tx_id = 1
        self._lock = threading.Lock()

    def read_holding_registers(self, unit_id: int, start: int, count: int) -> Optional[bytes]:
        pdu = struct.pack(">BHH", 0x03, start, count)
        frame = self._exchange(unit_id, pdu)
        if not frame:
            return None
        payload = frame[7:]
        if not payload:
            log.warning("Read response payload is empty")
            return None
        if payload[0] == 0x83:
            exception_code = payload[1] if len(payload) > 1 else None
            log.warning("Modbus exception on read (0x03): code=%s", exception_code)
            return None
        if payload[0] != 0x03:
            log.warning("Unexpected function in read response: 0x%02X", payload[0])
            return None
        return frame

    def write_single_register(self, unit_id: int, register: int, value: int) -> bool:
        pdu = struct.pack(">BHH", 0x06, register, value)
        frame = self._exchange(unit_id, pdu)
        if not frame:
            return False

        payload = frame[7:]
        if len(payload) < 5:
            log.warning("Short write response for 0x06: %s", hexdump(payload))
            return False
        if payload[0] == 0x86:
            exception_code = payload[1]
            log.warning("Modbus exception on write single (0x06): code=%s", exception_code)
            return False
        if payload[0] != 0x06:
            log.warning("Unexpected function in write single response: 0x%02X", payload[0])
            return False

        rx_register, rx_value = struct.unpack(">HH", payload[1:5])
        if rx_register != register or rx_value != value:
            log.warning(
                "Write single echo mismatch: register tx=%s rx=%s value tx=%s rx=%s",
                register,
                rx_register,
                value,
                rx_value,
            )
            return False
        return True

    def write_multiple_registers(self, unit_id: int, start_register: int, values: List[int]) -> bool:
        if not values:
            log.warning("write_multiple_registers called without values")
            return False

        count = len(values)
        data = b"".join(struct.pack(">H", value) for value in values)
        pdu = struct.pack(">BHHB", 0x10, start_register, count, len(data)) + data
        frame = self._exchange(unit_id, pdu)
        if not frame:
            return False

        payload = frame[7:]
        if len(payload) < 5:
            log.warning("Short write response for 0x10: %s", hexdump(payload))
            return False
        if payload[0] == 0x90:
            exception_code = payload[1]
            log.warning("Modbus exception on write multiple (0x10): code=%s", exception_code)
            return False
        if payload[0] != 0x10:
            log.warning("Unexpected function in write multiple response: 0x%02X", payload[0])
            return False

        rx_start, rx_count = struct.unpack(">HH", payload[1:5])
        if rx_start != start_register or rx_count != count:
            log.warning(
                "Write multiple echo mismatch: start tx=%s rx=%s count tx=%s rx=%s",
                start_register,
                rx_start,
                count,
                rx_count,
            )
            return False
        return True

    def _exchange(self, unit_id: int, pdu: bytes) -> Optional[bytes]:
        with self._lock:
            tx_id = self.tx_id & 0xFFFF
            self.tx_id += 1

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

                rx_tx_id, proto_id, length, rx_unit = struct.unpack(">HHHB", header)
                if rx_tx_id != tx_id:
                    log.warning("Unexpected transaction id: tx=%s rx=%s", tx_id, rx_tx_id)
                if proto_id != 0:
                    log.warning("Unexpected proto_id=%s", proto_id)
                if rx_unit != unit_id:
                    log.warning("Unexpected unit id: tx=%s rx=%s", unit_id, rx_unit)
                if length <= 0:
                    log.warning("Invalid MBAP length=%s", length)
                    return None

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


def publish_write_result(payload: dict):
    publish(WRITE_RESULT_TOPIC, payload, retain=False)


def parse_write_command_payload(payload_bytes: bytes) -> Tuple[Optional[dict], Optional[str]]:
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception:
        return None, "invalid JSON payload"

    if not isinstance(payload, dict):
        return None, "payload must be a JSON object"

    register = payload.get("register")
    if isinstance(register, bool) or not isinstance(register, int):
        return None, "field 'register' must be an integer"
    if not 0 <= register <= 65535:
        return None, "field 'register' must be between 0 and 65535"

    values_field = payload.get("values")
    value_field = payload.get("value")
    if values_field is not None and value_field is not None:
        return None, "use either 'value' or 'values', not both"

    if values_field is not None:
        if not isinstance(values_field, list) or not values_field:
            return None, "field 'values' must be a non-empty integer list"
        if len(values_field) > 123:
            return None, "field 'values' supports at most 123 registers"

        values: List[int] = []
        for item in values_field:
            if isinstance(item, bool) or not isinstance(item, int):
                return None, "all items in 'values' must be integers"
            if not 0 <= item <= 65535:
                return None, "all items in 'values' must be between 0 and 65535"
            values.append(item)

        for offset in range(len(values)):
            target_register = register + offset
            if target_register not in ALLOWED_WRITE_REGISTERS:
                return None, f"register {target_register} is not allowed for writes"

        return {
            "register": register,
            "values": values,
            "multi_write": True,
            "request_id": payload.get("request_id"),
        }, None

    if isinstance(value_field, bool) or not isinstance(value_field, int):
        return None, "field 'value' must be an integer when 'values' is not provided"
    if not 0 <= value_field <= 65535:
        return None, "field 'value' must be between 0 and 65535"
    if register not in ALLOWED_WRITE_REGISTERS:
        return None, f"register {register} is not allowed for writes"

    return {
        "register": register,
        "values": [value_field],
        "multi_write": False,
        "request_id": payload.get("request_id"),
    }, None


def handle_mqtt_write_command(payload_bytes: bytes):
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()

    if modbus is None:
        publish_write_result(
            {
                "ok": False,
                "error": "modbus client is not initialized yet",
                "timestamp": timestamp,
            }
        )
        return

    command, error = parse_write_command_payload(payload_bytes)
    if error:
        log.warning("Rejected write command: %s", error)
        publish_write_result(
            {
                "ok": False,
                "error": error,
                "timestamp": timestamp,
            }
        )
        return

    register = command["register"]
    values = command["values"]
    request_id = command["request_id"]

    if command["multi_write"]:
        log.info("Write command 0x10 register=%s values=%s", register, values)
        ok = modbus.write_multiple_registers(MODBUS_UNIT_ID, register, values)
        function_code = "0x10"
    else:
        log.info("Write command 0x06 register=%s value=%s", register, values[0])
        ok = modbus.write_single_register(MODBUS_UNIT_ID, register, values[0])
        function_code = "0x06"

    response: Dict[str, Any] = {
        "ok": ok,
        "function": function_code,
        "register": register,
        "values": values,
        "timestamp": timestamp,
    }
    if request_id is not None:
        response["request_id"] = request_id
    if not ok:
        response["error"] = "write failed or was rejected by the BMS"
    publish_write_result(response)
    if ok:
        refresh_control_state()


def parse_modbus_read_words(frame: bytes, expected_count: int) -> Optional[List[int]]:
    if not frame or len(frame) < 9:
        return None
    payload = frame[7:]
    if len(payload) < 2:
        return None
    if payload[0] != 0x03:
        return None
    byte_count = payload[1]
    expected_bytes = expected_count * 2
    if byte_count != expected_bytes:
        return None
    data = payload[2:]
    if len(data) < expected_bytes:
        return None
    words: List[int] = []
    for i in range(expected_count):
        pos = i * 2
        words.append(int.from_bytes(data[pos : pos + 2], byteorder="big", signed=False))
    return words


def decode_capacity_ah(high_word: int, low_word: int) -> float:
    raw = (high_word << 16) | low_word
    return round(raw / 1000.0, 1)


def decode_max_charge_current(raw_value: int) -> Optional[float]:
    if 0 <= raw_value <= 30000:
        return round((30000.0 - raw_value) / 10.0, 1)
    return None


def decode_max_discharge_current(raw_value: int) -> Optional[float]:
    if 30000 <= raw_value <= 60000:
        return round((raw_value - 30000.0) / 10.0, 1)
    return None


def encode_capacity_words(value_ah: float) -> Optional[List[int]]:
    if value_ah < 0:
        return None
    raw = int(round(value_ah * 1000.0))
    if not 0 <= raw <= 0xFFFFFFFF:
        return None
    return [(raw >> 16) & 0xFFFF, raw & 0xFFFF]


def encode_max_charge_current(value_a: float) -> Optional[int]:
    raw = int(round(30000.0 - value_a * 10.0))
    if not 0 <= raw < 30000:
        return None
    return raw


def encode_max_discharge_current(value_a: float) -> Optional[int]:
    raw = int(round(30000.0 + value_a * 10.0))
    if not 30000 < raw <= 60000:
        return None
    return raw


def parse_switch_payload(payload_bytes: bytes) -> Optional[bool]:
    text = payload_bytes.decode("utf-8", errors="ignore").strip().upper()
    if text in {"ON", "1", "TRUE"}:
        return True
    if text in {"OFF", "0", "FALSE"}:
        return False
    return None


def parse_float_payload(payload_bytes: bytes) -> Optional[float]:
    text = payload_bytes.decode("utf-8", errors="ignore").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _profile_order() -> List[str]:
    if active_control_profile == "base":
        return ["base", "offset"]
    return ["offset", "base"]


def _register_candidates(offset_register: int, base_register: int) -> List[int]:
    if active_control_profile == "base":
        return [base_register, offset_register]
    return [offset_register, base_register]


def _update_active_control_profile(profile: str):
    global active_control_profile
    if profile != active_control_profile:
        log.info("Control register profile switched to: %s", profile)
        active_control_profile = profile


def _read_control_state_for_profile(profile: str) -> Optional[dict]:
    if modbus is None:
        return None

    if profile == "offset":
        start = CONTROL_REG_START
        count = CONTROL_REG_COUNT
        reg_charge = REG_CHARGE_MOS_CONTROL
        reg_discharge = REG_DISCHARGE_MOS_CONTROL
        reg_rated_high = REG_RATED_CAPACITY_HIGH
        reg_rated_low = REG_RATED_CAPACITY_LOW
        reg_actual_high = REG_ACTUAL_CAPACITY_HIGH
        reg_actual_low = REG_ACTUAL_CAPACITY_LOW
        reg_chg_l1 = REG_MAX_CHARGE_CURRENT_LEVEL_1
        reg_chg_l2 = REG_MAX_CHARGE_CURRENT_LEVEL_2
        reg_dsg_l1 = REG_MAX_DISCHARGE_CURRENT_LEVEL_1
        reg_dsg_l2 = REG_MAX_DISCHARGE_CURRENT_LEVEL_2
    else:
        start = CONTROL_REG_START_BASE
        count = CONTROL_REG_COUNT_BASE
        reg_charge = REG_CHARGE_MOS_CONTROL_BASE
        reg_discharge = REG_DISCHARGE_MOS_CONTROL_BASE
        reg_rated_high = REG_RATED_CAPACITY_HIGH_BASE
        reg_rated_low = REG_RATED_CAPACITY_LOW_BASE
        reg_actual_high = REG_ACTUAL_CAPACITY_HIGH_BASE
        reg_actual_low = REG_ACTUAL_CAPACITY_LOW_BASE
        reg_chg_l1 = REG_MAX_CHARGE_CURRENT_LEVEL_1_BASE
        reg_chg_l2 = REG_MAX_CHARGE_CURRENT_LEVEL_2_BASE
        reg_dsg_l1 = REG_MAX_DISCHARGE_CURRENT_LEVEL_1_BASE
        reg_dsg_l2 = REG_MAX_DISCHARGE_CURRENT_LEVEL_2_BASE

    frame = modbus.read_holding_registers(MODBUS_UNIT_ID, start, count)
    if not frame:
        return None
    words = parse_modbus_read_words(frame, count)
    if words is None:
        return None

    def reg_value(register: int) -> int:
        return words[register - start]

    charge_raw = reg_value(reg_charge)
    discharge_raw = reg_value(reg_discharge)
    if charge_raw not in (0, 1) or discharge_raw not in (0, 1):
        return None

    return {
        "charge_mos_control": charge_raw == 1,
        "discharge_mos_control": discharge_raw == 1,
        "rated_capacity_ah": decode_capacity_ah(
            reg_value(reg_rated_high),
            reg_value(reg_rated_low),
        ),
        "actual_capacity_ah": decode_capacity_ah(
            reg_value(reg_actual_high),
            reg_value(reg_actual_low),
        ),
        "max_charge_current_level_1": decode_max_charge_current(reg_value(reg_chg_l1)),
        "max_charge_current_level_2": decode_max_charge_current(reg_value(reg_chg_l2)),
        "max_discharge_current_level_1": decode_max_discharge_current(reg_value(reg_dsg_l1)),
        "max_discharge_current_level_2": decode_max_discharge_current(reg_value(reg_dsg_l2)),
    }


def _write_single_with_fallback(offset_register: int, base_register: int, value: int) -> Tuple[bool, Optional[int]]:
    if modbus is None:
        return False, None

    candidates = _register_candidates(offset_register, base_register)
    first_allowed: Optional[int] = None
    for register in candidates:
        if register not in ALLOWED_WRITE_REGISTERS:
            continue
        if first_allowed is None:
            first_allowed = register
        if modbus.write_single_register(MODBUS_UNIT_ID, register, value):
            _update_active_control_profile("offset" if register == offset_register else "base")
            return True, register

    return False, first_allowed


def _write_multi_with_fallback(
    offset_start_register: int, base_start_register: int, values: List[int]
) -> Tuple[bool, Optional[int]]:
    if modbus is None:
        return False, None

    candidates = _register_candidates(offset_start_register, base_start_register)
    first_allowed: Optional[int] = None
    for start_register in candidates:
        if any((start_register + idx) not in ALLOWED_WRITE_REGISTERS for idx in range(len(values))):
            continue
        if first_allowed is None:
            first_allowed = start_register
        if modbus.write_multiple_registers(MODBUS_UNIT_ID, start_register, values):
            _update_active_control_profile("offset" if start_register == offset_start_register else "base")
            return True, start_register

    return False, first_allowed


def read_control_state() -> Optional[dict]:
    for profile in _profile_order():
        state = _read_control_state_for_profile(profile)
        if state is not None:
            _update_active_control_profile(profile)
            return state
    return None


def publish_control_state(control_state: dict):
    publish(f"{CONTROL_TOPIC}/state", control_state)


def refresh_control_state():
    if not ENABLE_WRITE_COMMANDS:
        return
    state = read_control_state()
    if state is not None:
        publish_control_state(state)


def handle_simple_control_command(topic: str, payload_bytes: bytes):
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    if modbus is None:
        publish_write_result({"ok": False, "error": "modbus client is not initialized yet", "timestamp": timestamp})
        return

    ok = False
    function_code = "0x06"
    register: Optional[int] = None
    values: List[int] = []
    error: Optional[str] = None

    if topic == CHARGE_MOS_SET_TOPIC:
        switch_value = parse_switch_payload(payload_bytes)
        if switch_value is None:
            error = "invalid payload for charge_mos_control (expected ON/OFF)"
        else:
            values = [1 if switch_value else 0]
            ok, register = _write_single_with_fallback(
                REG_CHARGE_MOS_CONTROL,
                REG_CHARGE_MOS_CONTROL_BASE,
                values[0],
            )
            if register is None:
                error = "no allowed register for charge_mos_control write"
    elif topic == DISCHARGE_MOS_SET_TOPIC:
        switch_value = parse_switch_payload(payload_bytes)
        if switch_value is None:
            error = "invalid payload for discharge_mos_control (expected ON/OFF)"
        else:
            values = [1 if switch_value else 0]
            ok, register = _write_single_with_fallback(
                REG_DISCHARGE_MOS_CONTROL,
                REG_DISCHARGE_MOS_CONTROL_BASE,
                values[0],
            )
            if register is None:
                error = "no allowed register for discharge_mos_control write"
    elif topic == RATED_CAPACITY_SET_TOPIC:
        function_code = "0x10"
        value_ah = parse_float_payload(payload_bytes)
        words = encode_capacity_words(value_ah) if value_ah is not None else None
        if words is None:
            error = "invalid payload for rated_capacity_ah (expected Ah >= 0)"
        else:
            values = words
            ok, register = _write_multi_with_fallback(
                REG_RATED_CAPACITY_HIGH,
                REG_RATED_CAPACITY_HIGH_BASE,
                values,
            )
            if register is None:
                error = "no allowed register for rated_capacity_ah write"
    elif topic == ACTUAL_CAPACITY_SET_TOPIC:
        function_code = "0x10"
        value_ah = parse_float_payload(payload_bytes)
        words = encode_capacity_words(value_ah) if value_ah is not None else None
        if words is None:
            error = "invalid payload for actual_capacity_ah (expected Ah >= 0)"
        else:
            values = words
            ok, register = _write_multi_with_fallback(
                REG_ACTUAL_CAPACITY_HIGH,
                REG_ACTUAL_CAPACITY_HIGH_BASE,
                values,
            )
            if register is None:
                error = "no allowed register for actual_capacity_ah write"
    elif topic == MAX_CHARGE_CURRENT_LEVEL_1_SET_TOPIC:
        value_a = parse_float_payload(payload_bytes)
        raw = encode_max_charge_current(value_a) if value_a is not None else None
        if raw is None:
            error = "invalid payload for max_charge_current_level_1 (expected A between 0.1 and 3000)"
        else:
            values = [raw]
            ok, register = _write_single_with_fallback(
                REG_MAX_CHARGE_CURRENT_LEVEL_1,
                REG_MAX_CHARGE_CURRENT_LEVEL_1_BASE,
                raw,
            )
            if register is None:
                error = "no allowed register for max_charge_current_level_1 write"
    elif topic == MAX_CHARGE_CURRENT_LEVEL_2_SET_TOPIC:
        value_a = parse_float_payload(payload_bytes)
        raw = encode_max_charge_current(value_a) if value_a is not None else None
        if raw is None:
            error = "invalid payload for max_charge_current_level_2 (expected A between 0.1 and 3000)"
        else:
            values = [raw]
            ok, register = _write_single_with_fallback(
                REG_MAX_CHARGE_CURRENT_LEVEL_2,
                REG_MAX_CHARGE_CURRENT_LEVEL_2_BASE,
                raw,
            )
            if register is None:
                error = "no allowed register for max_charge_current_level_2 write"
    elif topic == MAX_DISCHARGE_CURRENT_LEVEL_1_SET_TOPIC:
        value_a = parse_float_payload(payload_bytes)
        raw = encode_max_discharge_current(value_a) if value_a is not None else None
        if raw is None:
            error = "invalid payload for max_discharge_current_level_1 (expected A between 0.1 and 3000)"
        else:
            values = [raw]
            ok, register = _write_single_with_fallback(
                REG_MAX_DISCHARGE_CURRENT_LEVEL_1,
                REG_MAX_DISCHARGE_CURRENT_LEVEL_1_BASE,
                raw,
            )
            if register is None:
                error = "no allowed register for max_discharge_current_level_1 write"
    elif topic == MAX_DISCHARGE_CURRENT_LEVEL_2_SET_TOPIC:
        value_a = parse_float_payload(payload_bytes)
        raw = encode_max_discharge_current(value_a) if value_a is not None else None
        if raw is None:
            error = "invalid payload for max_discharge_current_level_2 (expected A between 0.1 and 3000)"
        else:
            values = [raw]
            ok, register = _write_single_with_fallback(
                REG_MAX_DISCHARGE_CURRENT_LEVEL_2,
                REG_MAX_DISCHARGE_CURRENT_LEVEL_2_BASE,
                raw,
            )
            if register is None:
                error = "no allowed register for max_discharge_current_level_2 write"
    else:
        return

    response: Dict[str, Any] = {
        "ok": ok,
        "function": function_code,
        "register": register,
        "values": values,
        "timestamp": timestamp,
    }
    if error is not None:
        response["ok"] = False
        response["error"] = error
    elif not ok:
        response["error"] = "write failed or was rejected by the BMS"
    publish_write_result(response)
    if response["ok"]:
        refresh_control_state()


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


def s16(value: int) -> int:
    return value - 65536 if value >= 32768 else value


def reg_u16(block: bytes, register: int) -> int:
    return u16(block, register * 2)


def parse_cell_voltages(block: bytes, cell_count: int) -> List[float]:
    """The first registers are cell voltages in mV according to BMSTool."""
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


def parse_signed_current(raw_value: int) -> Optional[float]:
    if raw_value == 65535:
        return None
    return round((raw_value - 30000) / 10.0, 1)


def parse_rtc(block: bytes) -> Optional[str]:
    year = block[194] + 2000
    month = block[195]
    day = block[196]
    hour = block[197]
    minute = block[198]
    second = block[199]
    if (year, month, day, hour, minute, second) == (2000, 0, 0, 0, 0, 0):
        return None
    try:
        return dt.datetime(year, month, day, hour, minute, second).isoformat(sep=" ")
    except ValueError:
        return None


def decode_alarm_bytes(error_bytes: List[int]) -> List[str]:
    active_alarms: List[str] = []
    for byte_index, value in enumerate(error_bytes[:8]):
        if byte_index == 2:
            if (value & 0x03) == 0x03:
                active_alarms.append(SPECIAL_ALARM_TEXTS[0])
            if (value & 0x0C) == 0x0C:
                active_alarms.append(SPECIAL_ALARM_TEXTS[1])
        if byte_index == 3 and (value & 0x03) == 0x03:
            active_alarms.append(SPECIAL_ALARM_TEXTS[2])

        for bit_index in range(8):
            if ((value >> bit_index) & 1) != 1:
                continue
            label = ALARM_TEXTS[byte_index * 8 + bit_index]
            if label not in active_alarms:
                active_alarms.append(label)
    return active_alarms


def decode_new_fault_bytes(new_fault_bytes: List[int]) -> List[str]:
    active_faults: List[str] = []

    # Bytes 0..10 contain two 3-bit level faults and two 1-bit status faults.
    for byte_index, value in enumerate(new_fault_bytes[:11]):
        low_level = value & 0x07
        low_group_index = 2 * byte_index
        if low_level > 0 and low_group_index < len(FAULT_LEVEL_BASE_TEXTS):
            label = f"{FAULT_LEVEL_BASE_TEXTS[low_group_index]} lv{low_level}"
            if label not in active_faults:
                active_faults.append(label)

        high_level = (value >> 3) & 0x07
        high_group_index = low_group_index + 1
        if high_level > 0 and high_group_index < len(FAULT_LEVEL_BASE_TEXTS):
            label = f"{FAULT_LEVEL_BASE_TEXTS[high_group_index]} lv{high_level}"
            if label not in active_faults:
                active_faults.append(label)

        if ((value >> 6) & 1) == 1:
            bit_label_index = 2 * byte_index
            if bit_label_index < len(FAULT_BIT_TEXTS):
                label = FAULT_BIT_TEXTS[bit_label_index]
                if label not in active_faults:
                    active_faults.append(label)

        if ((value >> 7) & 1) == 1:
            bit_label_index = 2 * byte_index + 1
            if bit_label_index < len(FAULT_BIT_TEXTS):
                label = FAULT_BIT_TEXTS[bit_label_index]
                if label not in active_faults:
                    active_faults.append(label)

    # Bytes 11..13 are pure bit fields mapped to FaultBit[22..].
    for byte_index, value in enumerate(new_fault_bytes[11:14], start=11):
        for bit_index in range(8):
            if ((value >> bit_index) & 1) != 1:
                continue
            bit_label_index = (byte_index - 11) * 8 + bit_index + 22
            if bit_label_index >= len(FAULT_BIT_TEXTS):
                continue
            label = FAULT_BIT_TEXTS[bit_label_index]
            if label not in active_faults:
                active_faults.append(label)

    return active_faults


def parse_balance_active_cells(block: bytes, cell_count: int) -> List[int]:
    active_cells: List[int] = []
    balance_bytes = list(block[158:164])
    for group_index, group_value in enumerate(balance_bytes):
        for bit_index in range(8):
            cell_number = group_index * 8 + bit_index + 1
            if cell_number > cell_count:
                return active_cells
            if ((group_value >> bit_index) & 1) == 1:
                active_cells.append(cell_number)
    return active_cells


def parse_main_metrics(block: bytes, cells_payload: Optional[dict] = None) -> dict:
    cell_temperatures = [parse_temperature(reg_u16(block, 48 + idx)) for idx in range(8)]
    voltage_raw = reg_u16(block, 56)
    current_raw = reg_u16(block, 57)
    soc_raw = reg_u16(block, 58)
    detected_cell_count = reg_u16(block, 60)
    detected_ntc_count = reg_u16(block, 61)

    voltage = round(voltage_raw / 10.0, 1) if 0 < voltage_raw < 1000 else None
    if voltage is None and cells_payload is not None:
        voltage = round(cells_payload["sum"], 1)

    current = parse_signed_current(current_raw)
    soc = round(soc_raw / 10.0, 1) if 0 < soc_raw <= 1000 else None
    direct_power = reg_u16(block, 88)
    power = float(direct_power) if direct_power != 65535 else None
    if power is None and voltage is not None and current is not None:
        power = round(voltage * current, 1)

    remaining_charge_ah = None
    if soc is not None and NOMINAL_CAPACITY_AH > 0:
        remaining_charge_ah = round(NOMINAL_CAPACITY_AH * soc / 100.0, 2)

    max_temperature = parse_temperature(reg_u16(block, 67))
    min_temperature = parse_temperature(reg_u16(block, 69))
    rtc = parse_rtc(block)
    error_words = [reg_u16(block, register) for register in range(102, 106)]
    error_bytes: List[int] = []
    for word in error_words:
        error_bytes.extend([(word >> 8) & 0xFF, word & 0xFF])
    active_alarms = decode_alarm_bytes(error_bytes)
    new_fault_bytes = list(block[218:232])
    active_faults = decode_new_fault_bytes(new_fault_bytes)

    effective_cell_count = CELL_COUNT
    if 0 < detected_cell_count <= 48:
        effective_cell_count = min(CELL_COUNT, detected_cell_count)
    balance_active_cells = parse_balance_active_cells(block, effective_cell_count)

    pwm_voltage_raw = reg_u16(block, 121)
    if pwm_voltage_raw >= 32768:
        pwm_voltage_raw -= 32768

    return {
        "voltage": voltage,
        "voltage_raw": voltage_raw,
        "current": current,
        "current_raw": current_raw,
        "soc": soc,
        "soc_raw": soc_raw,
        "power": power,
        "power_raw": direct_power,
        "remaining_charge_ah": remaining_charge_ah,
        "remaining_capacity_ah": round(reg_u16(block, 75) / 10.0, 2) if reg_u16(block, 75) != 65535 else None,
        "cycle_time": reg_u16(block, 76) if reg_u16(block, 76) != 65535 else None,
        "bms_life": reg_u16(block, 59) if reg_u16(block, 59) != 65535 else None,
        "detected_cell_count": detected_cell_count if 0 < detected_cell_count <= 48 else None,
        "detected_ntc_count": detected_ntc_count if 0 < detected_ntc_count <= 8 else None,
        "temperature_1": cell_temperatures[0],
        "temperature_2": cell_temperatures[1],
        "temperature_3": cell_temperatures[2],
        "temperature_4": cell_temperatures[3],
        "temperature_5": cell_temperatures[4],
        "temperature_6": cell_temperatures[5],
        "temperature_7": cell_temperatures[6],
        "temperature_8": cell_temperatures[7],
        "temperature_raw": [reg_u16(block, 48 + idx) for idx in range(8)],
        "max_temperature": max_temperature,
        "max_temperature_sensor": reg_u16(block, 68) if reg_u16(block, 68) != 65535 else None,
        "min_temperature": min_temperature,
        "min_temperature_sensor": reg_u16(block, 70) if reg_u16(block, 70) != 65535 else None,
        "temperature_diff": float(reg_u16(block, 71)) if reg_u16(block, 71) != 65535 else None,
        "battery_status": reg_u16(block, 72) if reg_u16(block, 72) != 65535 else None,
        "charge_detect": reg_u16(block, 73) if reg_u16(block, 73) != 65535 else None,
        "load_detect": reg_u16(block, 74) if reg_u16(block, 74) != 65535 else None,
        "balance_status": reg_u16(block, 77) if reg_u16(block, 77) != 65535 else None,
        "balance_current_raw": reg_u16(block, 78) - 30000 if reg_u16(block, 78) != 65535 else None,
        "balance_active_cells": balance_active_cells,
        "balance_active_cell_count": len(balance_active_cells),
        "balancing_active": bool(balance_active_cells) or reg_u16(block, 77) == 1,
        "charge_mos_raw": reg_u16(block, 82),
        "discharge_mos_raw": reg_u16(block, 83),
        "precharge_mos_raw": reg_u16(block, 84),
        "heat_mos_raw": reg_u16(block, 85),
        "fan_mos_raw": reg_u16(block, 86),
        "charge_mos_on": reg_u16(block, 82) == 1,
        "discharge_mos_on": reg_u16(block, 83) == 1,
        "precharge_mos_on": reg_u16(block, 84) == 1,
        "heat_mos_on": reg_u16(block, 85) == 1,
        "fan_mos_on": reg_u16(block, 86) == 1,
        "mos_temperature": parse_temperature(reg_u16(block, 90)),
        "board_temperature": parse_temperature(reg_u16(block, 91)),
        "heat_temperature": parse_temperature(reg_u16(block, 92)),
        "heat_current": float(reg_u16(block, 93)) if reg_u16(block, 93) != 65535 else None,
        "limit_state": reg_u16(block, 95) if reg_u16(block, 95) != 65535 else None,
        "limit_current": parse_signed_current(reg_u16(block, 96)),
        "rtc": rtc,
        "charge_full_time": reg_u16(block, 100) if reg_u16(block, 100) != 65535 else None,
        "do_state": block[202],
        "di_state": block[203],
        "error_words": error_words,
        "error_bytes": error_bytes,
        "active_alarms": active_alarms,
        "active_alarm_count": len(active_alarms),
        "active_faults": active_faults,
        "active_fault_count": len(active_faults),
        "backup_current": parse_signed_current(reg_u16(block, 106)),
        "wakeup_source": reg_u16(block, 107) if reg_u16(block, 107) != 65535 else None,
        "new_fault_bytes": new_fault_bytes,
        "afe_current": parse_signed_current(reg_u16(block, 116)),
        "afe_factor": round(reg_u16(block, 117) / 10000.0, 4) if reg_u16(block, 117) != 65535 else None,
        "afe_offset": round(s16(reg_u16(block, 118)) / 10.0, 1) if reg_u16(block, 118) != 65535 else None,
        "afe_ad": s16(reg_u16(block, 119)) if reg_u16(block, 119) != 65535 else None,
        "pwm_duty": round(reg_u16(block, 120) / 10.0, 1) if reg_u16(block, 120) != 65535 else None,
        "pwm_voltage": round(pwm_voltage_raw / 10.0, 1) if reg_u16(block, 121) != 65535 else None,
        "serial_port_type": reg_u16(block, 126) if reg_u16(block, 126) != 65535 else None,
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
        detected_cell_count = reg_u16(block, 60)
        effective_cell_count = CELL_COUNT
        if 0 < detected_cell_count <= 48:
            effective_cell_count = min(CELL_COUNT, detected_cell_count)

        cells = parse_cell_voltages(block, effective_cell_count)
        if not cells_look_plausible(cells):
            log.warning("Cell voltages look implausible: %s", cells)
            return None

        min_v = min(cells)
        max_v = max(cells)
        payload = {f"cell_{i+1}": v for i, v in enumerate(cells)}
        balance_active_cells = parse_balance_active_cells(block, effective_cell_count)
        payload.update(
            {
                "sum": round(sum(cells), 3),
                "avg": round(sum(cells) / len(cells), 3),
                "min": min_v,
                "minCell": cells.index(min_v) + 1,
                "max": max_v,
                "maxCell": cells.index(max_v) + 1,
                "diff": round(max_v - min_v, 3),
                "detected_cell_count": detected_cell_count if 0 < detected_cell_count <= 48 else None,
                "balance_active_cells": balance_active_cells,
                "balance_active_cell_count": len(balance_active_cells),
                "balancing_active": bool(balance_active_cells) or reg_u16(block, 77) == 1,
            }
        )
        log.debug(
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
    """Publish parsed metrics from the BMSTool register map."""
    try:
        metrics = parse_main_metrics(block, cells_payload)
        state_payload = {
            "voltage": metrics["voltage"],
            "current": metrics["current"],
            "power": metrics["power"],
            "soc": metrics["soc"],
            "remaining_charge_ah": metrics["remaining_charge_ah"],
            "remaining_capacity_ah": metrics["remaining_capacity_ah"],
            "backup_current": metrics["backup_current"],
            "bms_life": metrics["bms_life"],
            "detected_cell_count": metrics["detected_cell_count"],
            "detected_ntc_count": metrics["detected_ntc_count"],
            "cycle_time": metrics["cycle_time"],
            "max_temperature": metrics["max_temperature"],
            "max_temperature_sensor": metrics["max_temperature_sensor"],
            "min_temperature": metrics["min_temperature"],
            "min_temperature_sensor": metrics["min_temperature_sensor"],
            "temperature_diff": metrics["temperature_diff"],
            "mos_temperature": metrics["mos_temperature"],
            "board_temperature": metrics["board_temperature"],
            "heat_temperature": metrics["heat_temperature"],
            "heat_current": metrics["heat_current"],
            "limit_state": metrics["limit_state"],
            "limit_current": metrics["limit_current"],
            "charge_full_time": metrics["charge_full_time"],
            "wakeup_source": metrics["wakeup_source"],
            "pwm_duty": metrics["pwm_duty"],
            "pwm_voltage": metrics["pwm_voltage"],
            "rtc": metrics["rtc"],
            "battery_status": metrics["battery_status"],
            "charge_detect": metrics["charge_detect"],
            "load_detect": metrics["load_detect"],
        }
        log.debug("Parsed metrics: %s", state_payload)
        publish(f"{STATE_TOPIC}/state", state_payload)

        debug_payload = {
            "word_48": reg_u16(block, 48),
            "word_49": reg_u16(block, 49),
            "word_56": reg_u16(block, 56),
            "word_57": reg_u16(block, 57),
            "word_58": reg_u16(block, 58),
            "word_59": reg_u16(block, 59),
            "word_60": reg_u16(block, 60),
            "word_61": reg_u16(block, 61),
            "word_62": reg_u16(block, 62),
            "word_63": reg_u16(block, 63),
            "word_64": reg_u16(block, 64),
            "word_65": reg_u16(block, 65),
            "word_66": reg_u16(block, 66),
            "word_67": reg_u16(block, 67),
            "word_68": reg_u16(block, 68),
            "word_69": reg_u16(block, 69),
            "word_70": reg_u16(block, 70),
            "word_75": reg_u16(block, 75),
            "word_76": reg_u16(block, 76),
            "word_78": reg_u16(block, 78),
            "word_88": reg_u16(block, 88),
            "word_89": reg_u16(block, 89),
            "word_90": reg_u16(block, 90),
            "word_91": reg_u16(block, 91),
            "word_95": reg_u16(block, 95),
            "word_96": reg_u16(block, 96),
            "word_100": reg_u16(block, 100),
            "word_106": reg_u16(block, 106),
            "word_107": reg_u16(block, 107),
            "word_116": reg_u16(block, 116),
            "word_117": reg_u16(block, 117),
            "word_118": reg_u16(block, 118),
            "word_119": reg_u16(block, 119),
            "word_120": reg_u16(block, 120),
            "word_121": reg_u16(block, 121),
            "pack_voltage_from_cells": round(cells_payload["sum"], 1) if cells_payload else None,
            "voltage_raw": metrics["voltage_raw"],
            "current_raw": metrics["current_raw"],
            "soc_raw": metrics["soc_raw"],
            "power_raw": metrics["power_raw"],
        }
        publish(f"{DEBUG_TOPIC}/state", debug_payload)

        temp_payload = {
            "temperature_1": metrics["temperature_1"],
            "temperature_2": metrics["temperature_2"],
            "temperature_3": metrics["temperature_3"],
            "temperature_4": metrics["temperature_4"],
            "temperature_5": metrics["temperature_5"],
            "temperature_6": metrics["temperature_6"],
            "temperature_7": metrics["temperature_7"],
            "temperature_8": metrics["temperature_8"],
            "max_temperature": metrics["max_temperature"],
            "min_temperature": metrics["min_temperature"],
            "temperature_diff": metrics["temperature_diff"],
            "detected_ntc_count": metrics["detected_ntc_count"],
            "raw_1": metrics["temperature_raw"][0],
            "raw_2": metrics["temperature_raw"][1],
            "raw_3": metrics["temperature_raw"][2],
            "raw_4": metrics["temperature_raw"][3],
            "raw_5": metrics["temperature_raw"][4],
            "raw_6": metrics["temperature_raw"][5],
            "raw_7": metrics["temperature_raw"][6],
            "raw_8": metrics["temperature_raw"][7],
        }
        publish(f"{TEMP_TOPIC}/state", temp_payload)

        mos_payload = {
            "value": str(reg_u16(block, 91)),
            "raw_180": reg_u16(block, 90),
            "raw_182": reg_u16(block, 91),
            "raw_184": reg_u16(block, 92),
            "charge_mos_raw": metrics["charge_mos_raw"],
            "discharge_mos_raw": metrics["discharge_mos_raw"],
            "precharge_mos_raw": metrics["precharge_mos_raw"],
            "heat_mos_raw": metrics["heat_mos_raw"],
            "fan_mos_raw": metrics["fan_mos_raw"],
            "charge_mos_on": metrics["charge_mos_on"],
            "discharge_mos_on": metrics["discharge_mos_on"],
            "precharge_mos_on": metrics["precharge_mos_on"],
            "heat_mos_on": metrics["heat_mos_on"],
            "fan_mos_on": metrics["fan_mos_on"],
            "mos_temperature": metrics["mos_temperature"],
            "board_temperature": metrics["board_temperature"],
            "heat_temperature": metrics["heat_temperature"],
        }
        publish(f"{MOS_TOPIC}/state", mos_payload)

        status_payload = {
            "raw_150": reg_u16(block, 75),
            "raw_152": reg_u16(block, 76),
            "raw_154": reg_u16(block, 77),
            "raw_156": reg_u16(block, 78),
            "raw_158": reg_u16(block, 79),
            "raw_160": reg_u16(block, 80),
            "battery_status": metrics["battery_status"],
            "charge_detect": metrics["charge_detect"],
            "load_detect": metrics["load_detect"],
            "do_state": metrics["do_state"],
            "di_state": metrics["di_state"],
            "charge_full_time": metrics["charge_full_time"],
            "wakeup_source": metrics["wakeup_source"],
            "error_words": metrics["error_words"],
            "error_bytes": metrics["error_bytes"],
            "new_fault_bytes": metrics["new_fault_bytes"],
            "active_alarm_count": metrics["active_alarm_count"],
            "active_alarms": metrics["active_alarms"],
            "active_fault_count": metrics["active_fault_count"],
            "active_faults": metrics["active_faults"],
        }
        publish(f"{STATUS_TOPIC}/state", status_payload)

    except Exception:
        log.exception("Failed to publish candidates")


modbus = ModbusTcpClient(CONNECTION_HOST, CONNECTION_PORT, SOCKET_TIMEOUT)

log.info("Connecting to MQTT broker...")
client.connect(MQTT_SERVER)
client.loop_start()

publish_discovery()
if ENABLE_WRITE_COMMANDS:
    refresh_control_state()

try:
    while True:
        log.debug("Polling Daly WNT block via Modbus TCP...")
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

        log.debug("Parsed fixed block with %d bytes", len(block))
        publish_raw(block)
        cells_payload = publish_cells(block)
        publish_candidates(block, cells_payload)
        if ENABLE_WRITE_COMMANDS:
            refresh_control_state()

        time.sleep(POLL_INTERVAL_SECONDS)

finally:
    log.info("Shutting down block monitor")
    try:
        client.loop_stop()
        client.disconnect()
        log.info("MQTT disconnected")
    except Exception:
        log.exception("Failed to disconnect MQTT")
