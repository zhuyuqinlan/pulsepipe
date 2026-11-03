package com.pulsepipe.api.health;

import com.pulsepipe.kafka.consumer.IngestBatchListener.ConsumerStats;
import com.pulsepipe.kafka.producer.KafkaEventPublisher.PublisherStats;
import com.pulsepipe.mqtt.ingest.MqttIngestService.MqttStats;

import java.util.Map;

/** Aggregated pipeline health snapshot returned by {@code GET /api/health}. */
public class PipelineHealth {

    private String status;
    private MqttStats mqtt;
    private PublisherStats kafkaProducer;
    private ConsumerStats kafkaConsumer;
    private String database;
    private Map<String, Object> rustfs;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public MqttStats getMqtt() {
        return mqtt;
    }

    public void setMqtt(MqttStats mqtt) {
        this.mqtt = mqtt;
    }

    public PublisherStats getKafkaProducer() {
        return kafkaProducer;
    }

    public void setKafkaProducer(PublisherStats kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public ConsumerStats getKafkaConsumer() {
        return kafkaConsumer;
    }

    public void setKafkaConsumer(ConsumerStats kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public Map<String, Object> getRustfs() {
        return rustfs;
    }

    public void setRustfs(Map<String, Object> rustfs) {
        this.rustfs = rustfs;
    }
}
