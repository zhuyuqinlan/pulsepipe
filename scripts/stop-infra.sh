#!/usr/bin/env bash
# Stop the PulsePipe infrastructure stack (data volumes are preserved).
set -euo pipefail
cd "$(dirname "$0")/../deploy"
docker compose --env-file ../.env down "$@"
