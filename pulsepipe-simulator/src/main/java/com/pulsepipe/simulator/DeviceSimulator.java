package com.pulsepipe.simulator;

import com.pulsepipe.common.json.EventJson;
import com.pulsepipe.common.model.Event;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Standalone MQTT device simulator.
 *
 * <pre>
 * java -jar pulsepipe-simulator.jar --devices 1000 --rate 10 --duration 60
 * </pre>
 *
 * Simulates N devices, each publishing {@code rate} telemetry messages per
 * second for {@code duration} seconds, then prints connection and delivery
 * statistics. Uses MQTT QoS 1 and measures real connect/publish outcomes via
 * Paho callbacks — no fake counters.
 */
public final class DeviceSimulator {

    private static final String TOPIC_TEMPLATE = "device/%s/telemetry";

    private final AtomicLong connectionsEstablished = new AtomicLong();
    private final AtomicLong connectionFailures = new AtomicLong();
    private final AtomicLong messagesAttempted = new AtomicLong();
    private final AtomicLong sendSuccess = new AtomicLong();
    private final AtomicLong sendFailure = new AtomicLong();
    private final Map<String, MqttAsyncClient> clients = new ConcurrentHashMap<>();
    private final ReentrantLock statsLock = new ReentrantLock();

    private int devices = 1000;
    private int rate = 10;
    private int durationSeconds = 60;
    private String broker = "tcp://localhost:1883";
    private String username;
    private String password;
    private int qos = 1;

    public static void main(String[] args) throws Exception {
        DeviceSimulator sim = new DeviceSimulator();
        sim.parseArgs(args);
        sim.run();
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i].toLowerCase(Locale.ROOT)) {
                case "--devices" -> devices = Integer.parseInt(args[++i]);
                case "--rate" -> rate = Integer.parseInt(args[++i]);
                case "--duration" -> durationSeconds = Integer.parseInt(args[++i]);
                case "--broker" -> broker = args[++i];
                case "--username" -> username = args[++i];
                case "--password" -> password = args[++i];
                case "--qos" -> qos = Integer.parseInt(args[++i]);
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        if (devices < 1 || rate < 1 || durationSeconds < 1) {
            throw new IllegalArgumentException("--devices/--rate/--duration must be >= 1");
        }
    }

    private void run() throws Exception {
        System.out.printf("PulsePipe simulator: devices=%d, rate=%d msg/s per device, duration=%ds, broker=%s (QoS %d)%n",
                devices, rate, durationSeconds, broker, qos);

        long start = System.currentTimeMillis();

        // --- Phase 1: connect all devices concurrently -----------------------
        CountDownLatch connectLatch = new CountDownLatch(devices);
        ExecutorService connectPool = Executors.newFixedThreadPool(Math.min(devices, 64));
        for (int i = 0; i < devices; i++) {
            final String deviceId = String.format("sim-%05d", i);
            connectPool.submit(() -> {
                try {
                    connectDevice(deviceId);
                    connectionsEstablished.incrementAndGet();
                } catch (Exception e) {
                    connectionFailures.incrementAndGet();
                    System.err.printf("Connect failed for %s: %s%n", deviceId, e.getMessage());
                } finally {
                    connectLatch.countDown();
                }
            });
        }
        connectLatch.await();
        connectPool.shutdown();

        // --- Phase 2: publish telemetry at the requested rate ----------------
        long sendStart = System.currentTimeMillis();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(Math.min(rate * 2, 32));
        long intervalMs = Math.max(1, 1000L / rate);
        for (int i = 0; i < devices; i++) {
            final String deviceId = String.format("sim-%05d", i);
            // Slight initial jitter so devices are not perfectly aligned.
            scheduler.scheduleAtFixedRate(() -> publishTelemetry(deviceId),
                    ThreadLocalRandom.current().nextLong(0, intervalMs), intervalMs, TimeUnit.MILLISECONDS);
        }

        long sendEnd = sendStart + durationSeconds * 1000L;
        long reportInterval = Math.max(5, durationSeconds / 6);
        while (System.currentTimeMillis() < sendEnd) {
            Thread.sleep(Math.min(reportInterval * 1000L, sendEnd - System.currentTimeMillis()));
            System.out.printf("[t+%ds] connected=%d, sent=%d, sendFailures=%d%n",
                    (System.currentTimeMillis() - sendStart) / 1000,
                    connectionsEstablished.get(), sendSuccess.get(), sendFailure.get());
        }
        scheduler.shutdownNow();
        long elapsedMs = System.currentTimeMillis() - sendStart;

        // --- Phase 3: disconnect and report ----------------------------------
        for (MqttAsyncClient client : clients.values()) {
            try {
                if (client.isConnected()) {
                    client.disconnect().waitForCompletion(2_000);
                }
                client.close();
            } catch (MqttException ignored) {
                // best-effort disconnect during shutdown
            }
        }

        double throughput = elapsedMs > 0 ? sendSuccess.get() * 1000.0 / elapsedMs : 0;
        System.out.println("\n================ SIMULATION REPORT ================");
        System.out.printf("Devices configured      : %d%n", devices);
        System.out.printf("Connections established : %d%n", connectionsEstablished.get());
        System.out.printf("Connection failures     : %d%n", connectionFailures.get());
        System.out.printf("Messages attempted      : %d%n", messagesAttempted.get());
        System.out.printf("Send succeeded (QoS %d) : %d%n", qos, sendSuccess.get());
        System.out.printf("Send failed             : %d%n", sendFailure.get());
        System.out.printf("Actual send duration    : %.1f s%n", elapsedMs / 1000.0);
        System.out.printf("Achieved throughput     : %.1f msg/s%n", throughput);
        System.out.println("====================================================");
    }

    private void connectDevice(String deviceId) throws Exception {
        String clientId = "sim-" + deviceId;
        MqttAsyncClient client = new MqttAsyncClient(broker, clientId, new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        options.setKeepAliveInterval(60);
        options.setAutomaticReconnect(true);
        if (username != null && !username.isBlank()) {
            options.setUserName(username);
            options.setPassword(password == null ? new char[0] : password.toCharArray());
        }
        IMqttToken token = client.connect(options);
        token.waitForCompletion(15_000);
        clients.put(deviceId, client);
    }

    private void publishTelemetry(String deviceId) {
        MqttAsyncClient client = clients.get(deviceId);
        if (client == null || !client.isConnected()) {
            return;
        }
        Random rnd = ThreadLocalRandom.current();
        long now = System.currentTimeMillis();
        Event event = new Event(
                UUID.randomUUID().toString(),
                deviceId,
                Event.EVENT_TYPE_TELEMETRY,
                now,
                Map.of(
                        "temperature", Math.round((20 + rnd.nextDouble() * 20) * 10) / 10.0,
                        "pressure", Math.round((95 + rnd.nextDouble() * 15) * 10) / 10.0,
                        "seq", System.nanoTime()));
        try {
            MqttMessage message = new MqttMessage(EventJson.serialize(event).getBytes());
            message.setQos(qos);
            messagesAttempted.incrementAndGet();
            String topic = String.format(TOPIC_TEMPLATE, deviceId);
            client.publish(topic, message, null, new org.eclipse.paho.client.mqttv3.IMqttActionListener() {
                @Override
                public void onSuccess(org.eclipse.paho.client.mqttv3.IMqttToken asyncActionToken) {
                    sendSuccess.incrementAndGet();
                }

                @Override
                public void onFailure(org.eclipse.paho.client.mqttv3.IMqttToken asyncActionToken, Throwable exception) {
                    sendFailure.incrementAndGet();
                }
            });
        } catch (Exception e) {
            sendFailure.incrementAndGet();
        }
    }
}
