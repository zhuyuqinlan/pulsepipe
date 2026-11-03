package com.pulsepipe.common.pipeline;

import com.pulsepipe.common.model.Event;

/**
 * Outbound port implemented by the Kafka publisher. The MQTT ingest stage
 * depends on this interface only, never on Kafka directly.
 */
public interface EventPublisher {

    /**
     * Asynchronously publish an event. Implementations must never block the
     * caller on broker I/O.
     */
    void publish(Event event);
}
