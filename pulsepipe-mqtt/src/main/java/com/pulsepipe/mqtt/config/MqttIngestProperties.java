package com.pulsepipe.mqtt.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * MQTT ingest configuration, bound from the {@code pulsepipe.mqtt.*} property
 * namespace.
 */
@ConfigurationProperties(prefix = "pulsepipe.mqtt")
public class MqttIngestProperties {

    /** Enable the MQTT ingest stage (disable for tests / API-only runs). */
    private boolean enabled = true;
    /** Broker URL, e.g. tcp://localhost:1883 */
    private String broker = "tcp://localhost:1883";
    private String clientId = "pulsepipe-ingest";
    private String username;
    private String password;
    /** Topic filter subscribed for device events. */
    private String topicFilter = "device/+/telemetry";
    /** MQTT QoS used for the subscription. */
    private int qos = 1;
    /** Durable session so missed messages are re-delivered after reconnect. */
    private boolean cleanSession = false;
    /** Bounded queue between MQTT callback threads and Kafka dispatchers. */
    private int queueCapacity = 10_000;
    /** Threads moving events from the queue to the Kafka producer. */
    private int dispatcherThreads = 4;
    private int connectionTimeoutSeconds = 15;
    private int keepAliveSeconds = 30;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTopicFilter() {
        return topicFilter;
    }

    public void setTopicFilter(String topicFilter) {
        this.topicFilter = topicFilter;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getDispatcherThreads() {
        return dispatcherThreads;
    }

    public void setDispatcherThreads(int dispatcherThreads) {
        this.dispatcherThreads = dispatcherThreads;
    }

    public int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    public void setConnectionTimeoutSeconds(int connectionTimeoutSeconds) {
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
    }

    public int getKeepAliveSeconds() {
        return keepAliveSeconds;
    }

    public void setKeepAliveSeconds(int keepAliveSeconds) {
        this.keepAliveSeconds = keepAliveSeconds;
    }
}
