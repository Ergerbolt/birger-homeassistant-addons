#!/bin/bash
set -euo pipefail

CONFIG_PATH=/data/options.json

export MQTT_SERVER="$(jq --raw-output '.mqtt_server' "$CONFIG_PATH")"
export MQTT_USER="$(jq --raw-output '.mqtt_user' "$CONFIG_PATH")"
export MQTT_PASS="$(jq --raw-output '.mqtt_pass' "$CONFIG_PATH")"
export MQTT_CLIENT_ID="$(jq --raw-output '.mqtt_client_id' "$CONFIG_PATH")"
export MQTT_DISCOVERY_PREFIX="$(jq --raw-output '.mqtt_discovery_prefix' "$CONFIG_PATH")"
export DEVICE="$(jq --raw-output '.device' "$CONFIG_PATH")"
export DEVICE_ID="$(jq --raw-output '.device_id' "$CONFIG_PATH")"
export CELL_COUNT="$(jq --raw-output '.cells_in_series' "$CONFIG_PATH")"
export POLL_INTERVAL_SECONDS="$(jq --raw-output '.poll_interval_seconds' "$CONFIG_PATH")"

exec python3 /monitor.py
