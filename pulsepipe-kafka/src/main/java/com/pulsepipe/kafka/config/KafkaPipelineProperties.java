package com.pulsepipe.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Kafka pipeline configuration bound from {@code pulsepipe.kafka.*}.
 */
@ConfigurationProperties(prefix = "pulsepipe.kafka")
public class KafkaPipelineProperties {

    private String bootstrapServers = "localhost:29092";
    private String topic = "pulsepipe.events";
    private int topicPartitions = 3;
    private short topicReplicationFactor = 1;

    private final Producer producer = new Producer();
    private final Consumer consumer = new Consumer();

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getTopicPartitions() {
        return topicPartitions;
    }

    public void setTopicPartitions(int topicPartitions) {
        this.topicPartitions = topicPartitions;
    }

    public short getTopicReplicationFactor() {
        return topicReplicationFactor;
    }

    public void setTopicReplicationFactor(short topicReplicationFactor) {
        this.topicReplicationFactor = topicReplicationFactor;
    }

    public Producer getProducer() {
        return producer;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public static class Producer {
        /** Batch size in bytes for micro-batching sends. */
        private int batchSize = 65_536;
        /** Linger window in ms so sends get batched. */
        private int lingerMs = 10;
        /** Send buffer capacity in records. */
        private int bufferMemory = 33_554_432;

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getLingerMs() {
            return lingerMs;
        }

        public void setLingerMs(int lingerMs) {
            this.lingerMs = lingerMs;
        }

        public int getBufferMemory() {
            return bufferMemory;
        }

        public void setBufferMemory(int bufferMemory) {
            this.bufferMemory = bufferMemory;
        }
    }

    public static class Consumer {
        private String groupId = "pulsepipe-consumer";
        private int concurrency = 3;
        private int maxPollRecords = 1_000;
        /** Retry attempts for a failing DB batch before the batch is dropped (poison-pill guard). */
        private int handlerMaxRetries = 3;
        /** Backoff between handler retries in ms. */
        private long handlerRetryBackoffMs = 1_000;

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public int getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(int concurrency) {
            this.concurrency = concurrency;
        }

        public int getMaxPollRecords() {
            return maxPollRecords;
        }

        public void setMaxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
        }

        public int getHandlerMaxRetries() {
            return handlerMaxRetries;
        }

        public void setHandlerMaxRetries(int handlerMaxRetries) {
            this.handlerMaxRetries = handlerMaxRetries;
        }

        public long getHandlerRetryBackoffMs() {
            return handlerRetryBackoffMs;
        }

        public void setHandlerRetryBackoffMs(long handlerRetryBackoffMs) {
            this.handlerRetryBackoffMs = handlerRetryBackoffMs;
        }
    }
}
