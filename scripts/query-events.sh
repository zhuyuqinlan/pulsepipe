#!/usr/bin/env bash
# Query recent events from TimescaleDB.
# Usage: ./scripts/query-events.sh [limit]
set -euo pipefail
cd "$(dirname "$0")/../deploy"

LIMIT="${1:-10}"
docker compose --env-file ../.env exec -T timescaledb psql \
  -U "${POSTGRES_USER:-pulsepipe}" -d "${POSTGRES_DB:-pulsepipe}" \
  -c "SELECT event_id, device_id, event_type, event_timestamp, payload FROM pulsepipe_events ORDER BY event_timestamp DESC LIMIT ${LIMIT};"
