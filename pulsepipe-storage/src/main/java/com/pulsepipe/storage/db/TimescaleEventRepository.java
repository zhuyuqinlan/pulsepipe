package com.pulsepipe.storage.db;

import com.pulsepipe.common.model.Event;
import com.pulsepipe.common.pipeline.EventBatchHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.util.List;

/**
 * TimescaleDB event repository.
 *
 * <p>Idempotency: the hypertable is partitioned by {@code event_timestamp}, so
 * a unique index must include the partition column. We use
 * {@code UNIQUE (event_id, event_timestamp)} and {@code ON CONFLICT DO
 * NOTHING} — replayed events (MQTT QoS1 redelivery, Kafka redelivery after
 * crash-before-commit) never create duplicate rows.</p>
 *
 * <p>Writes use a single JDBC batch per call — never row-by-row round trips.</p>
 */
public class TimescaleEventRepository implements EventBatchHandler {

    private static final Logger log = LoggerFactory.getLogger(TimescaleEventRepository.class);

    private static final String INSERT_SQL = """
            INSERT INTO pulsepipe_events
                (event_id, device_id, event_type, event_timestamp, payload, created_at)
            VALUES (?, ?, ?, ?, ?::jsonb, now())
            ON CONFLICT (event_id, event_timestamp) DO NOTHING
            """;

    private final JdbcTemplate jdbc;

    public TimescaleEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public int handleBatch(List<Event> events) {
        if (events == null || events.isEmpty()) {
            return 0;
        }
        int[][] results = jdbc.batchUpdate(INSERT_SQL, events, 1_000, (ps, event) -> {
            ps.setObject(1, java.util.UUID.fromString(event.eventId()));
            ps.setString(2, event.deviceId());
            ps.setString(3, event.eventType() == null ? Event.EVENT_TYPE_TELEMETRY : event.eventType());
            ps.setTimestamp(4, new Timestamp(event.timestamp()));
            ps.setString(5, com.pulsepipe.common.json.EventJson.serializePayload(event.payload()));
        });
        int inserted = 0;
        for (int[] batchResult : results) {
            for (int r : batchResult) {
                // SUCCESS_NO_INFO (-2) counts as inserted: driver ran the batch
                // without per-statement update counts.
                if (r > 0 || r == java.sql.Statement.SUCCESS_NO_INFO) {
                    inserted++;
                }
            }
        }
        log.debug("Persisted batch of {} events ({} new rows)", events.size(), inserted);
        return inserted;
    }

    /** Look up all rows sharing an eventId (normally 0..1). */
    public List<Event> findByEventId(String eventId) {
        return jdbc.query(
                "SELECT event_id, device_id, event_type, event_timestamp, payload FROM pulsepipe_events WHERE event_id = ? ORDER BY event_timestamp DESC LIMIT 10",
                (rs, i) -> mapRow(rs),
                java.util.UUID.fromString(eventId));
    }

    /** Time-window query for a device. Timestamps are epoch millis. */
    public List<Event> findByDeviceAndTimeRange(String deviceId, Long startTime, Long endTime, int limit) {
        StringBuilder sql = new StringBuilder(
                "SELECT event_id, device_id, event_type, event_timestamp, payload FROM pulsepipe_events WHERE device_id = ?");
        List<Object> args = new java.util.ArrayList<>();
        args.add(deviceId);
        if (startTime != null) {
            sql.append(" AND event_timestamp >= ?");
            args.add(new Timestamp(startTime));
        }
        if (endTime != null) {
            sql.append(" AND event_timestamp <= ?");
            args.add(new Timestamp(endTime));
        }
        sql.append(" ORDER BY event_timestamp DESC LIMIT ?");
        args.add(Math.min(Math.max(limit, 1), 10_000));
        return jdbc.query(sql.toString(), (rs, i) -> mapRow(rs), args.toArray());
    }

    public long countAll() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM pulsepipe_events", Long.class);
        return count == null ? 0 : count;
    }

    public boolean dbHealthy() {
        try {
            jdbc.queryForObject("SELECT 1", Integer.class);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private Event mapRow(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new Event(
                rs.getString("event_id"),
                rs.getString("device_id"),
                rs.getString("event_type"),
                rs.getTimestamp("event_timestamp").getTime(),
                com.pulsepipe.common.json.EventJson.toMap(rs.getString("payload")));
    }
}
