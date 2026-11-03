package com.pulsepipe.mqtt.ingest;

import com.pulsepipe.common.json.EventJson;
import com.pulsepipe.common.model.Event;
import com.pulsepipe.common.pipeline.EventPublisher;
import com.pulsepipe.mqtt.config.MqttIngestProperties;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Ingests device events from EMQX and hands them over to Kafka.
 *
 * Threading contract:
 * <ul>
 *   <li>MQTT callback threads ONLY parse and enqueue — no broker I/O and no
 *       downstream (database) work happens here.</li>
 *   <li>The hand-off queue is bounded; when Kafka or the broker is slow the
 *       queue fills up and oldest-in-time events are NOT dropped silently —
 *       instead {@code offer()} fails and the event is counted as dropped so
 *       backpressure stays visible.</li>
 *   <li>Dispatcher threads take events from the queue and publish
 *       asynchronously to Kafka.</li>
 * </ul>
 */
public class MqttIngestService implements InitializingBean, DisposableBean, MqttCallbackExtended {

    private static final Logger log = LoggerFactory.getLogger(MqttIngestService.class);

    private final MqttIngestProperties props;
    private final EventPublisher publisher;
    private final BlockingQueue<Event> queue;
    private final ReentrantLock connectLock = new ReentrantLock();

    private MqttAsyncClient client;
    private ExecutorService dispatcher;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean connected = false;

    // Simple in-memory stats surfaced via /api/health.
    private final AtomicLong messagesReceived = new AtomicLong();
    private final AtomicLong parseErrors = new AtomicLong();
    private final AtomicLong enqueued = new AtomicLong();
    private final AtomicLong queueDropped = new AtomicLong();
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong publishErrors = new AtomicLong();

    public MqttIngestService(MqttIngestProperties props, EventPublisher publisher) {
        this.props = props;
        this.publisher = publisher;
        this.queue = new ArrayBlockingQueue<>(props.getQueueCapacity());
    }

    @Override
    public void afterPropertiesSet() {
        start();
    }

    public synchronized void start() {
        if (running.get() || !props.isEnabled()) {
            return;
        }
        dispatcher = Executors.newFixedThreadPool(props.getDispatcherThreads(), r -> {
            Thread t = new Thread(r, "mqtt-to-kafka-dispatcher");
            t.setDaemon(true);
            return t;
        });
        for (int i = 0; i < props.getDispatcherThreads(); i++) {
            dispatcher.submit(this::dispatchLoop);
        }
        // Connect in the background so a not-yet-ready broker does not kill the
        // whole application; we retry until the broker comes up.
        running.set(true);
        Thread connector = new Thread(this::connectWithRetry, "mqtt-connector");
        connector.setDaemon(true);
        connector.start();
    }

    private void connectWithRetry() {
        long backoffMs = 5_000;
        while (running.get()) {
            try {
                connect();
                return;
            } catch (Exception e) {
                log.warn("MQTT connect to {} failed ({}); retrying in {} ms",
                        props.getBroker(), e.getMessage(), backoffMs);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                backoffMs = Math.min(backoffMs * 2, 60_000);
            }
        }
    }

    private void connect() {
        connectLock.lock();
        try {
            if (client != null) {
                try {
                    client.close();
                } catch (MqttException ignored) {
                    // best effort close of a previous client instance
                }
            }
            client = new MqttAsyncClient(props.getBroker(), props.getClientId(), new MemoryPersistence());
            client.setCallback(this);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(props.isCleanSession());
            options.setAutomaticReconnect(true);
            options.setConnectionTimeout(props.getConnectionTimeoutSeconds());
            options.setKeepAliveInterval(props.getKeepAliveSeconds());
            options.setMaxInflight(1_000);
            if (props.getUsername() != null && !props.getUsername().isBlank()) {
                options.setUserName(props.getUsername());
                options.setPassword(props.getPassword() == null ? new char[0] : props.getPassword().toCharArray());
            }

            client.connect(options).waitForCompletion(TimeUnit.SECONDS.toMillis(props.getConnectionTimeoutSeconds() * 2L));
            client.subscribe(props.getTopicFilter(), props.getQos())
                    .waitForCompletion(TimeUnit.SECONDS.toMillis(props.getConnectionTimeoutSeconds()));
            log.info("MQTT connected to {} and subscribed to '{}' (QoS {}, cleanSession={})",
                    props.getBroker(), props.getTopicFilter(), props.getQos(), props.isCleanSession());
        } catch (Exception e) {
            connected = false;
            throw new IllegalStateException("Failed to connect MQTT broker " + props.getBroker(), e);
        } finally {
            connectLock.unlock();
        }
    }

    private void dispatchLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Event event = queue.poll(1, TimeUnit.SECONDS);
                if (event == null) {
                    continue;
                }
                publisher.publish(event);
                published.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                publishErrors.incrementAndGet();
                log.error("Failed to publish event to Kafka", e);
            }
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        // Runs on Paho callback threads: keep it cheap — parse + bounded offer only.
        messagesReceived.incrementAndGet();
        try {
            Event event = EventJson.deserialize(new String(message.getPayload()));
            if (event.eventId() == null || event.deviceId() == null) {
                parseErrors.incrementAndGet();
                return;
            }
            if (queue.offer(event)) {
                enqueued.incrementAndGet();
            } else {
                queueDropped.incrementAndGet();
                // Sampled warning: do not flood logs under sustained backpressure.
                if (queueDropped.get() % 1_000 == 1) {
                    log.warn("MQTT ingest queue is full (capacity={}); dropped {} events so far",
                            props.getQueueCapacity(), queueDropped.get());
                }
            }
        } catch (IllegalArgumentException e) {
            parseErrors.incrementAndGet();
            if (parseErrors.get() % 100 == 1) {
                log.warn("Dropping malformed payload on topic {}: {}", topic, e.getMessage());
            }
        }
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        connected = true;
        if (reconnect) {
            log.warn("MQTT reconnected to {}", serverURI);
        }
        try {
            if (client != null) {
                // Resubscribe defensively; harmless when the session is durable.
                client.subscribe(props.getTopicFilter(), props.getQos());
            }
        } catch (MqttException e) {
            log.error("Failed to (re)subscribe to {}", props.getTopicFilter(), e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        connected = false;
        // Paho automaticReconnect is enabled; log and let the client recover.
        log.warn("MQTT connection lost; automatic reconnect in progress", cause);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // We are a consumer only; nothing to do.
    }

    @Override
    public void destroy() {
        running.set(false);
        if (dispatcher != null) {
            dispatcher.shutdownNow();
        }
        if (client != null && client.isConnected()) {
            try {
                client.disconnect().waitForCompletion(5_000);
            } catch (MqttException e) {
                log.warn("Error while disconnecting MQTT client", e);
            }
        }
        try {
            if (client != null) {
                client.close();
            }
        } catch (MqttException ignored) {
            // ignore close errors during shutdown
        }
    }

    public boolean isConnected() {
        return connected && client != null && client.isConnected();
    }

    public MqttStats stats() {
        return new MqttStats(
                isConnected(),
                messagesReceived.get(),
                parseErrors.get(),
                enqueued.get(),
                queueDropped.get(),
                published.get(),
                publishErrors.get(),
                queue.size());
    }

    /** Snapshot of ingest counters for health reporting. */
    public record MqttStats(
            boolean connected,
            long messagesReceived,
            long parseErrors,
            long enqueued,
            long queueDropped,
            long published,
            long publishErrors,
            int queueSize) {
    }
}
