package com.pulsepipe.kafka;

import com.pulsepipe.common.json.EventJson;
import com.pulsepipe.common.model.Event;
import com.pulsepipe.common.pipeline.EventBatchHandler;
import com.pulsepipe.kafka.config.KafkaPipelineProperties;
import com.pulsepipe.kafka.consumer.IngestBatchListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IngestBatchListenerTest {

    private static Event sampleEvent() {
        return new Event(UUID.randomUUID().toString(), "device-001",
                Event.EVENT_TYPE_TELEMETRY, System.currentTimeMillis(), Map.of("temperature", 25.0));
    }

    private static ConsumerRecord<String, String> record(Event event) {
        return new ConsumerRecord<>("pulsepipe.events", 0, 0, event.eventId(), EventJson.serialize(event));
    }

    private static class RecordingAck implements Acknowledgment {
        int acks;

        @Override
        public void acknowledge() {
            acks++;
        }
    }

    private static KafkaPipelineProperties fastProps() {
        KafkaPipelineProperties props = new KafkaPipelineProperties();
        props.getConsumer().setHandlerMaxRetries(3);
        props.getConsumer().setHandlerRetryBackoffMs(0);
        return props;
    }

    @Test
    void commitsOffsetsOnlyAfterHandlerSucceeds() {
        AtomicInteger handlerCalls = new AtomicInteger();
        List<Event> seen = new ArrayList<>();
        EventBatchHandler handler = batch -> {
            handlerCalls.incrementAndGet();
            seen.addAll(batch);
            return batch.size();
        };
        IngestBatchListener listener = new IngestBatchListener(handler, fastProps());
        RecordingAck ack = new RecordingAck();

        List<ConsumerRecord<String, String>> records = List.of(record(sampleEvent()), record(sampleEvent()));
        listener.onMessage(records, ack);

        assertEquals(1, handlerCalls.get());
        assertEquals(2, seen.size());
        assertEquals(1, ack.acks, "offset must be acknowledged exactly once after a successful write");
        assertEquals(0, listener.stats().droppedBatches());
    }

    @Test
    void retriesFailingHandlerThenDropsPoisonBatch() {
        AtomicInteger handlerCalls = new AtomicInteger();
        EventBatchHandler alwaysFailing = batch -> {
            handlerCalls.incrementAndGet();
            throw new RuntimeException("db down");
        };
        IngestBatchListener listener = new IngestBatchListener(alwaysFailing, fastProps());
        RecordingAck ack = new RecordingAck();

        listener.onMessage(List.of(record(sampleEvent())), ack);

        assertEquals(3, handlerCalls.get(), "handler should be attempted up to maxRetries times");
        assertEquals(2, listener.stats().retries(), "retries happen after failed attempts that allow another try");
        assertEquals(1, listener.stats().droppedBatches());
        assertEquals(1, ack.acks, "poison batch is quarantined and acknowledged to unblock the partition");
    }

    @Test
    void malformedRecordsAreSkippedWithoutBlockingBatch() {
        EventBatchHandler handler = batch -> batch.size();
        IngestBatchListener listener = new IngestBatchListener(handler, fastProps());
        RecordingAck ack = new RecordingAck();

        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        records.add(record(sampleEvent()));
        records.add(new ConsumerRecord<>("pulsepipe.events", 0, 1, "bad-key", "not-json"));

        listener.onMessage(records, ack);

        assertEquals(1, listener.stats().malformed());
        assertEquals(1, listener.stats().eventsProcessed());
        assertEquals(1, ack.acks);
    }

    @Test
    void emptyBatchIsAcknowledgedImmediately() {
        EventBatchHandler handler = batch -> {
            throw new AssertionError("handler must not be called for an empty batch");
        };
        IngestBatchListener listener = new IngestBatchListener(handler, fastProps());
        RecordingAck ack = new RecordingAck();

        listener.onMessage(new ArrayList<>(), ack);

        assertEquals(1, ack.acks);
        assertEquals(1, listener.stats().batchesProcessed());
    }

    @Test
    void duplicateEventsPassThroughToIdempotentHandler() {
        // The pipeline tolerates duplicates: the same event delivered twice is
        // simply handed to the idempotent storage writer.
        Map<String, Integer> insertCounts = new LinkedHashMap<>();
        EventBatchHandler countingHandler = batch -> {
            for (Event e : batch) {
                insertCounts.merge(e.eventId(), 1, Integer::sum);
            }
            return batch.size();
        };
        IngestBatchListener listener = new IngestBatchListener(countingHandler, fastProps());
        RecordingAck ack = new RecordingAck();

        Event duplicated = sampleEvent();
        listener.onMessage(List.of(record(duplicated), record(duplicated)), ack);

        assertEquals(1, insertCounts.size());
        assertEquals(2, insertCounts.get(duplicated.eventId()));
        assertEquals(1, ack.acks);
    }
}
