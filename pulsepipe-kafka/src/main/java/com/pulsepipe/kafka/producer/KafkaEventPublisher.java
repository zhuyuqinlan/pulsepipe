package com.pulsepipe.kafka.producer;

import com.pulsepipe.common.json.EventJson;
import com.pulsepipe.common.model.Event;
import com.pulsepipe.common.pipeline.EventPublisher;
import com.pulsepipe.kafka.config.KafkaPipelineProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async Kafka producer. Events are keyed by {@code eventId} so ordering per
 * event is deterministic across partitions.
 *
 * Reliability settings: {@code acks=all} + idempotent producer + retries. This
 * yields at-least-once semantics into Kafka (never exactly-once end to end);
 * duplicates are removed downstream by the idempotent database writer.
 */
public class KafkaEventPublisher implements EventPublisher, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final Producer<String, String> producer;
    private final String topic;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    public KafkaEventPublisher(KafkaPipelineProperties props) {
        this.topic = props.getTopic();
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(props.getProducer().getBatchSize()));
        p.put(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(props.getProducer().getLingerMs()));
        p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.toString(props.getProducer().getBufferMemory()));
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");
        this.producer = new KafkaProducer<>(p);
    }

    @Override
    public void publish(Event event) {
        String json = EventJson.serialize(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.eventId(), json);
        // Fully async: callback threads never block ingest.
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                failed.incrementAndGet();
                log.error("Failed to deliver event {} to Kafka topic {}: {}", event.eventId(), topic, exception.getMessage());
            } else {
                sent.incrementAndGet();
            }
        });
    }

    public PublisherStats stats() {
        return new PublisherStats(sent.get(), failed.get());
    }

    @Override
    public void close() {
        // Graceful drain: in-flight sends may complete within the timeout.
        producer.close(java.time.Duration.ofSeconds(10));
    }

    public record PublisherStats(long sent, long failed) {
    }
}
