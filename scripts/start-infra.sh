#!/usr/bin/env bash
# Start the PulsePipe infrastructure stack (EMQX, Kafka, TimescaleDB, RustFS).
set -euo pipefail
cd "$(dirname "$0")/../deploy"

if [ ! -f ../.env ]; then
  echo ".env not found — creating it from .env.example (edit passwords before production use)"
  cp ../.env.example ../.env
fi

docker compose --env-file ../.env up -d
echo "Waiting for all services to become healthy..."
docker compose --env-file ../.env ps
