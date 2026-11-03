package com.pulsepipe.api.config;

import com.pulsepipe.common.pipeline.EventPublisher;
import com.pulsepipe.kafka.config.KafkaPipelineProperties;
import com.pulsepipe.kafka.consumer.IngestBatchListener;
import com.pulsepipe.kafka.consumer.KafkaConsumerContainerFactory;
import com.pulsepipe.kafka.producer.KafkaEventPublisher;
import com.pulsepipe.mqtt.config.MqttIngestProperties;
import com.pulsepipe.mqtt.ingest.MqttIngestService;
import com.pulsepipe.storage.db.TimescaleEventRepository;
import com.pulsepipe.storage.rustfs.RustfsProperties;
import com.pulsepipe.storage.rustfs.RustfsService;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

/**
 * Wires the pipeline stages together:
 * <pre>
 * MqttIngestService --(bounded queue)--> KafkaEventPublisher --> [Kafka]
 * [Kafka] --> ConcurrentMessageListenerContainer --> IngestBatchListener
 *                                                   --> TimescaleEventRepository
 * </pre>
 */
@Configuration
@EnableConfigurationProperties({MqttIngestProperties.class, KafkaPipelineProperties.class, RustfsProperties.class})
public class PulsePipeConfig {

    private static final Logger log = LoggerFactory.getLogger(PulsePipeConfig.class);

    @Bean
    public KafkaEventPublisher kafkaEventPublisher(KafkaPipelineProperties props) {
        return new KafkaEventPublisher(props);
    }

    @Bean
    public MqttIngestService mqttIngestService(MqttIngestProperties mqttProps, EventPublisher publisher) {
        if (!mqttProps.isEnabled()) {
            log.warn("MQTT ingest stage disabled by configuration");
        }
        return new MqttIngestService(mqttProps, publisher);
    }

    @Bean
    public TimescaleEventRepository timescaleEventRepository(JdbcTemplate jdbcTemplate) {
        return new TimescaleEventRepository(jdbcTemplate);
    }

    @Bean
    public IngestBatchListener ingestBatchListener(TimescaleEventRepository repository,
                                                   KafkaPipelineProperties props) {
        return new IngestBatchListener(repository, props);
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, String> kafkaConsumerContainer(
            KafkaPipelineProperties props, IngestBatchListener listener) {
        return new KafkaConsumerContainerFactory(props).create(listener);
    }

    @Bean
    public KafkaAdmin kafkaAdmin(KafkaPipelineProperties props) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        return new KafkaAdmin(configs);
    }

    /** Declares (idempotently) the pipeline topic at startup. */
    @Bean
    public NewTopic eventsTopic(KafkaPipelineProperties props) {
        return new NewTopic(props.getTopic(), props.getTopicPartitions(), props.getTopicReplicationFactor());
    }

    @Bean(destroyMethod = "close")
    public RustfsService rustfsService(RustfsProperties props) {
        return new RustfsService(props);
    }
}
