package com.pulsepipe.common.model;

import java.util.Map;

/**
 * Unified event model exchanged across the whole pipeline.
 *
 * Wire format (JSON):
 * <pre>
 * {
 *   "eventId": "uuid",
 *   "deviceId": "device-001",
 *   "eventType": "telemetry",
 *   "timestamp": 1793600000000,
 *   "payload": { "temperature": 26.5, "pressure": 101.2 }
 * }
 * </pre>
 */
public record Event(
        String eventId,
        String deviceId,
        String eventType,
        long timestamp,
        Map<String, Object> payload) {

    public static final String EVENT_TYPE_TELEMETRY = "telemetry";
}
