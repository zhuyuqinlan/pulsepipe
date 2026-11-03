package com.pulsepipe.common;

import com.pulsepipe.common.json.EventJson;
import com.pulsepipe.common.model.Event;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventJsonTest {

    @Test
    void roundTripKeepsAllFields() {
        Event original = new Event(UUID.randomUUID().toString(), "device-001",
                Event.EVENT_TYPE_TELEMETRY, 1793600000000L,
                Map.of("temperature", 26.5, "pressure", 101.2));

        String json = EventJson.serialize(original);
        Event parsed = EventJson.deserialize(json);

        assertEquals(original.eventId(), parsed.eventId());
        assertEquals(original.deviceId(), parsed.deviceId());
        assertEquals(original.eventType(), parsed.eventType());
        assertEquals(original.timestamp(), parsed.timestamp());
        assertEquals(original.payload(), parsed.payload());
    }

    @Test
    void wireFormatMatchesSpec() {
        Event event = new Event("11111111-1111-1111-1111-111111111111", "device-001",
                "telemetry", 1793600000000L, Map.of("temperature", 26.5));
        String json = EventJson.serialize(event);

        assertTrue(json.contains("\"eventId\":\"11111111-1111-1111-1111-111111111111\""));
        assertTrue(json.contains("\"deviceId\":\"device-001\""));
        assertTrue(json.contains("\"eventType\":\"telemetry\""));
        assertTrue(json.contains("\"timestamp\":1793600000000"));
        assertTrue(json.contains("\"temperature\":26.5"));
    }

    @Test
    void malformedJsonThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> EventJson.deserialize("not-json"));
    }

    @Test
    void serializePayloadHandlesNull() {
        assertEquals("{}", EventJson.serializePayload(null));
    }
}
