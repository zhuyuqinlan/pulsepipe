package com.pulsepipe.kafka.consumer;

import com.pulsepipe.common.pipeline.EventBatchHandler;
import com.pulsepipe.kafka.config.KafkaPipelineProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the consumer container. Key guarantees:
 * <ul>
 *   <li>{@code enable.auto.commit=false} — offsets are committed only after
 *       the storage handler succeeds (AckMode.MANUAL_IMMEDIATE).</li>
 *   <li>Batch consumption via {@code max.poll.records}.</li>
 *   <li>Consumer group semantics allow horizontal scale-out.</li>
 * </ul>
 */
public class KafkaConsumerContainerFactory {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerContainerFactory.class);

    private final KafkaPipelineProperties props;

    public KafkaConsumerContainerFactory(KafkaPipelineProperties props) {
        this.props = props;
    }

    public ConcurrentMessageListenerContainer<String, String> create(IngestBatchListener listener) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, props.getConsumer().getGroupId());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, props.getConsumer().getMaxPollRecords());
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);

        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps);

        ContainerProperties containerProps = new ContainerProperties(props.getTopic());
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        containerProps.setMessageListener(listener);

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
        container.setConcurrency(props.getConsumer().getConcurrency());

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("kafka-ingest-");
        executor.setCorePoolSize(props.getConsumer().getConcurrency());
        executor.setMaxPoolSize(props.getConsumer().getConcurrency());
        executor.initialize();
        container.getContainerProperties().setListenerTaskExecutor(executor);
        log.info("Kafka consumer container configured: topic={}, group={}, concurrency={}, maxPollRecords={}",
                props.getTopic(), props.getConsumer().getGroupId(),
                props.getConsumer().getConcurrency(), props.getConsumer().getMaxPollRecords());
        return container;
    }
}
