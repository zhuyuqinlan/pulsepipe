#!/usr/bin/env bash
# List topics/messages in Kafka for verification.
set -euo pipefail
cd "$(dirname "$0")/../deploy"

# Prevent Git Bash (MSYS) from rewriting container paths like /opt/...
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

echo "=== Topics ==="
docker compose --env-file ../.env exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

echo "=== pulsepipe.events latest offsets per partition ==="
docker compose --env-file ../.env exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic pulsepipe.events --time latest
