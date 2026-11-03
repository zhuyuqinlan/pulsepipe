package com.pulsepipe.kafka.consumer;

import com.pulsepipe.common.json.EventJson;
import com.pulsepipe.common.model.Event;
import com.pulsepipe.common.pipeline.EventBatchHandler;
import com.pulsepipe.kafka.config.KafkaPipelineProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Batch listener: parses a polled batch, hands it to the durable storage
 * handler with retries, and only then acknowledges offsets.
 *
 * Offset contract: offsets are committed AFTER the database write succeeds
 * (AckMode.MANUAL_IMMEDIATE + explicit ack). If the JVM dies between write and
 * commit, redelivery may duplicate events — handled by the idempotent writer,
 * never by assuming exactly-once.
 *
 * Poison-pill policy: a batch that keeps failing after {@code maxRetries}
 * attempts is dropped with an ERROR log so a single bad batch cannot block the
 * partition forever. Normal transient failures are retried in-process.
 */
public class IngestBatchListener implements org.springframework.kafka.listener.BatchAcknowledgingMessageListener<String, String> {

    private static final Logger log = LoggerFactory.getLogger(IngestBatchListener.class);

    private final EventBatchHandler handler;
    private final KafkaPipelineProperties props;
    private final AtomicLong batchesProcessed = new AtomicLong();
    private final AtomicLong eventsProcessed = new AtomicLong();
    private final AtomicLong malformed = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong droppedBatches = new AtomicLong();

    public IngestBatchListener(EventBatchHandler handler, KafkaPipelineProperties props) {
        this.handler = handler;
        this.props = props;
    }

    @Override
    public void onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        List<Event> batch = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> record : records) {
            try {
                batch.add(EventJson.deserialize(record.value()));
            } catch (IllegalArgumentException e) {
                malformed.incrementAndGet();
                log.warn("Skipping malformed Kafka record (topic={}, partition={}, offset={}): {}",
                        record.topic(), record.partition(), record.offset(), e.getMessage());
            }
        }

        if (batch.isEmpty()) {
            acknowledgment.acknowledge();
            batchesProcessed.incrementAndGet();
            return;
        }

        int maxAttempts = Math.max(1, props.getConsumer().getHandlerMaxRetries());
        long backoff = Math.max(0, props.getConsumer().getHandlerRetryBackoffMs());
        Exception lastError = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                handler.handleBatch(batch);
                lastError = null;
                break;
            } catch (Exception e) {
                lastError = e;
                if (attempt < maxAttempts) {
                    retries.incrementAndGet();
                    log.warn("Batch persist failed (attempt {}/{}), retrying in {} ms: {}",
                            attempt, maxAttempts, backoff, e.getMessage());
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        if (lastError != null) {
            // Out of retries: drop the batch rather than blocking the partition forever.
            droppedBatches.incrementAndGet();
            log.error("Batch persist failed after {} attempts, DROPPING {} events (last error: {})",
                    maxAttempts, batch.size(), lastError.getMessage(), lastError);
        }

        // Acknowledge regardless: either the write succeeded, or the batch was
        // explicitly quarantined. Offsets are committed only here.
        acknowledgment.acknowledge();
        batchesProcessed.incrementAndGet();
        eventsProcessed.addAndGet(batch.size());
    }

    public ConsumerStats stats() {
        return new ConsumerStats(batchesProcessed.get(), eventsProcessed.get(), malformed.get(),
                retries.get(), droppedBatches.get());
    }

    public record ConsumerStats(long batchesProcessed, long eventsProcessed, long malformed,
                                long retries, long droppedBatches) {
    }
}
