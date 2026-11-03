package com.pulsepipe.api.web;

import com.pulsepipe.api.health.PipelineHealth;
import com.pulsepipe.kafka.consumer.IngestBatchListener;
import com.pulsepipe.kafka.producer.KafkaEventPublisher;
import com.pulsepipe.mqtt.ingest.MqttIngestService;
import com.pulsepipe.storage.db.TimescaleEventRepository;
import com.pulsepipe.storage.rustfs.RustfsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Basic health endpoint summarising every pipeline stage.
 */
@RestController
@RequestMapping("/api")
public class HealthController {

    private final MqttIngestService mqtt;
    private final KafkaEventPublisher publisher;
    private final IngestBatchListener consumerListener;
    private final TimescaleEventRepository repository;
    private final RustfsService rustfs;

    public HealthController(MqttIngestService mqtt, KafkaEventPublisher publisher,
                            IngestBatchListener consumerListener, TimescaleEventRepository repository,
                            RustfsService rustfs) {
        this.mqtt = mqtt;
        this.publisher = publisher;
        this.consumerListener = consumerListener;
        this.repository = repository;
        this.rustfs = rustfs;
    }

    @GetMapping("/health")
    public PipelineHealth health() {
        boolean dbOk = repository.dbHealthy();
        PipelineHealth health = new PipelineHealth();
        health.setStatus(dbOk && mqtt.stats().connected() ? "UP" : "DEGRADED");
        health.setMqtt(mqtt.stats());
        health.setKafkaProducer(publisher.stats());
        health.setKafkaConsumer(consumerListener.stats());
        health.setDatabase(dbOk ? "UP" : "DOWN");
        health.setRustfs(rustfsStatus());
        return health;
    }

    private Map<String, Object> rustfsStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        if (!rustfs.isConfigured()) {
            status.put("status", "NOT_CONFIGURED");
            return status;
        }
        try {
            boolean exists = rustfs.bucketExists(false);
            status.put("status", exists ? "UP" : "BUCKET_MISSING");
            status.put("endpoint", rustfs.getEndpoint());
            status.put("bucket", rustfs.getBucket());
        } catch (Exception e) {
            status.put("status", "DOWN");
            status.put("error", e.getMessage());
        }
        return status;
    }
}
