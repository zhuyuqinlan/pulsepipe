-- Schema used by both the Docker deployment and the Testcontainers-based
-- integration test. Executed once at first container start.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS pulsepipe_events (
    event_id        UUID        NOT NULL,
    device_id       TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    payload         JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hypertable partitioned by time.
SELECT create_hypertable('pulsepipe_events', 'event_timestamp',
                         if_not_exists => TRUE, migrate_data => TRUE);

-- Idempotency: unique index MUST include the partitioning column
-- (event_timestamp) because Hypertables only allow unique constraints that
-- cover all partition dimensions.
CREATE UNIQUE INDEX IF NOT EXISTS ux_pulsepipe_events_id_ts
    ON pulsepipe_events (event_id, event_timestamp);

-- Query index for device + time-window lookups.
CREATE INDEX IF NOT EXISTS idx_pulsepipe_events_device_ts
    ON pulsepipe_events (device_id, event_timestamp DESC);
