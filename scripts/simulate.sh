#!/usr/bin/env bash
# Run the device simulator against the local stack.
# Usage: ./scripts/simulate.sh [devices] [rate] [duration]
set -euo pipefail
cd "$(dirname "$0")/.."

DEVICES="${1:-100}"
RATE="${2:-10}"
DURATION="${3:-30}"

set +e
java -jar pulsepipe-simulator/target/pulsepipe-simulator-0.1.0-SNAPSHOT.jar \
  --broker "tcp://localhost:${MQTT_PORT:-1883}" \
  --username "${MQTT_USERNAME:-pulsepipe}" \
  --password "${MQTT_PASSWORD:-change-me-strong-password}" \
  --devices "$DEVICES" --rate "$RATE" --duration "$DURATION"
