package com.pulsepipe.common.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsepipe.common.model.Event;

import java.util.Map;

/**
 * JSON codec for the unified event model. Kept dependency-light so that every
 * module (including the simulator) can reuse it.
 */
public final class EventJson {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private EventJson() {
    }

    public static String serialize(Event event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize event " + event.eventId(), e);
        }
    }

    public static Event deserialize(String json) {
        try {
            return MAPPER.readValue(json, Event.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to deserialize event payload: " + e.getMessage(), e);
        }
    }

    public static Map<String, Object> toMap(String json) {
        try {
            return MAPPER.readValue(json, new TypeReference<>() {
            });
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse JSON object: " + e.getMessage(), e);
        }
    }

    /** Serialize only the payload map (used for the JSONB column). */
    public static String serializePayload(Map<String, Object> payload) {
        try {
            return MAPPER.writeValueAsString(payload == null ? Map.of() : payload);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize payload", e);
        }
    }
}
