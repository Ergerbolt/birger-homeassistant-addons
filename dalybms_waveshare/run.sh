#!/usr/bin/with-contenv bashio

export DEVICE=$(bashio::config 'device')
export DEVICE_ID=$(bashio::config 'device_id')
export CELL_COUNT=$(bashio::config 'cells_in_series')
export NOMINAL_CAPACITY_AH=$(bashio::config 'nominal_capacity_ah')
export MQTT_SERVER=$(bashio::config 'mqtt_server')
export MQTT_USER=$(bashio::config 'mqtt_user')
export MQTT_PASS=$(bashio::config 'mqtt_pass')
export MQTT_CLIENT_ID=$(bashio::config 'mqtt_client_id')
export MQTT_DISCOVERY_PREFIX=$(bashio::config 'mqtt_discovery_prefix')
export POLL_INTERVAL_SECONDS=$(bashio::config 'poll_interval_seconds')
export MODBUS_PORT=$(bashio::config 'modbus_port')
export MODBUS_UNIT_ID=$(bashio::config 'modbus_unit_id')
export MODBUS_START=$(bashio::config 'modbus_start')
export MODBUS_COUNT=$(bashio::config 'modbus_count')
export SOCKET_TIMEOUT=$(bashio::config 'socket_timeout')
export DEBUG_LOG_ENABLED=$(bashio::config 'debug_logging')

echo "=== Daly WNT Modbus add-on starting ==="
echo "DEVICE=$DEVICE"
echo "DEVICE_ID=$DEVICE_ID"
echo "CELL_COUNT=$CELL_COUNT"
echo "NOMINAL_CAPACITY_AH=$NOMINAL_CAPACITY_AH"
echo "MQTT_SERVER=$MQTT_SERVER"
echo "MQTT_CLIENT_ID=$MQTT_CLIENT_ID"
echo "MQTT_DISCOVERY_PREFIX=$MQTT_DISCOVERY_PREFIX"
echo "POLL_INTERVAL_SECONDS=$POLL_INTERVAL_SECONDS"
echo "MODBUS_PORT=$MODBUS_PORT"
echo "MODBUS_UNIT_ID=$MODBUS_UNIT_ID"
echo "MODBUS_START=$MODBUS_START"
echo "MODBUS_COUNT=$MODBUS_COUNT"
echo "SOCKET_TIMEOUT=$SOCKET_TIMEOUT"
echo "DEBUG_LOG_ENABLED=$DEBUG_LOG_ENABLED"

exec python3 -u /monitor.py
