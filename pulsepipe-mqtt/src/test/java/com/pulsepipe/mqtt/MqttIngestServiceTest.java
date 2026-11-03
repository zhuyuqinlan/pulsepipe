package com.pulsepipe.mqtt;

import com.pulsepipe.common.json.EventJson;
import com.pulsepipe.common.model.Event;
import com.pulsepipe.common.pipeline.EventPublisher;
import com.pulsepipe.mqtt.config.MqttIngestProperties;
import com.pulsepipe.mqtt.ingest.MqttIngestService;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MqttIngestServiceTest {

    private static Event sampleEvent() {
        return new Event(UUID.randomUUID().toString(), "device-001",
                Event.EVENT_TYPE_TELEMETRY, System.currentTimeMillis(),
                Map.of("temperature", 25.0));
    }

    @Test
    void messageArrivedParsesAndPublishesThroughQueue() throws Exception {
        MqttIngestProperties props = new MqttIngestProperties();
        // Connector runs in a background daemon thread and keeps retrying the
        // (absent) broker; harmless for this test, no real connection is made.
        props.setEnabled(true);
        props.setDispatcherThreads(1);

        CountDownLatch received = new CountDownLatch(1);
        EventPublisher countingPublisher = event -> received.countDown();

        MqttIngestService service = new MqttIngestService(props, countingPublisher);
        service.start();
        try {
            service.messageArrived("device/001/telemetry",
                    new MqttMessage(EventJson.serialize(sampleEvent()).getBytes()));
            assertTrue(received.await(5, TimeUnit.SECONDS), "event should reach the publisher");
            MqttIngestService.MqttStats stats = service.stats();
            assertEquals(1, stats.messagesReceived());
            assertEquals(1, stats.enqueued());
            assertEquals(0, stats.parseErrors());
        } finally {
            service.destroy();
        }
    }

    @Test
    void boundedQueueDropsWhenFull() {
        MqttIngestProperties props = new MqttIngestProperties();
        props.setEnabled(false);
        props.setQueueCapacity(2);

        AtomicInteger publishedCount = new AtomicInteger();
        MqttIngestService service = new MqttIngestService(props, event -> publishedCount.incrementAndGet());
        // Do NOT start the service: no dispatcher drains the queue.
        try {
            for (int i = 0; i < 5; i++) {
                service.messageArrived("device/001/telemetry",
                        new MqttMessage(EventJson.serialize(sampleEvent()).getBytes()));
            }
            MqttIngestService.MqttStats stats = service.stats();
            assertEquals(5, stats.messagesReceived());
            assertEquals(2, stats.enqueued());
            assertEquals(3, stats.queueDropped(), "events beyond queue capacity must be counted as dropped");
            assertEquals(0, stats.parseErrors());
        } finally {
            service.destroy();
        }
    }

    @Test
    void malformedPayloadIsCountedNotThrown() {
        MqttIngestProperties props = new MqttIngestProperties();
        props.setEnabled(false);
        MqttIngestService service = new MqttIngestService(props, event -> {
        });
        try {
            service.messageArrived("device/001/telemetry", new MqttMessage("not-json".getBytes()));
            MqttIngestService.MqttStats stats = service.stats();
            assertEquals(1, stats.parseErrors());
            assertEquals(0, stats.enqueued());
        } finally {
            service.destroy();
        }
    }
}
