package com.pulsepipe.storage;

import com.pulsepipe.common.json.EventJson;
import com.pulsepipe.common.model.Event;
import com.pulsepipe.storage.db.TimescaleEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the documented acceptance criteria against real infrastructure:
 * <ul>
 *   <li>TimescaleDB part runs in a Testcontainers TimescaleDB instance
 *       (requires a local Docker engine).</li>
 *   <li>RustFS part runs only when PULSEPIPE_INTEGRATION=true and a live
 *       RustFS is reachable, because bucket creation is done by the
 *       deployment's init container.</li>
 * </ul>
 */
@Testcontainers(disabledWithoutDocker = true)
class StorageIntegrationTest {

    // ----------------------- TimescaleDB (Testcontainers) -----------------------

    @Container
    static final org.testcontainers.containers.PostgreSQLContainer<?> TIMESCALE =
            new org.testcontainers.containers.PostgreSQLContainer<>(
                    DockerImageName.parse("timescale/timescaledb:2.17.2-pg16").asCompatibleSubstituteFor("postgres"))
                    .withInitScript("sql/init_test.sql")
                    .waitingFor(Wait.forListeningPort());

    private TimescaleEventRepository repository;
    private JdbcTemplate jdbc;

    @BeforeEach
    void setUpDb() {
        org.springframework.jdbc.datasource.DriverManagerDataSource dataSource =
                new org.springframework.jdbc.datasource.DriverManagerDataSource(
                        TIMESCALE.getJdbcUrl(), TIMESCALE.getUsername(), TIMESCALE.getPassword());
        dataSource.setDriverClassName(TIMESCALE.getDriverClassName());
        jdbc = new JdbcTemplate(dataSource);
        repository = new TimescaleEventRepository(jdbc);
    }

    private static Event sampleEvent(String deviceId, long ts) {
        return new Event(UUID.randomUUID().toString(), deviceId,
                Event.EVENT_TYPE_TELEMETRY, ts, Map.of("temperature", 26.5, "pressure", 101.2));
    }

    @Test
    void pulsepipeEventsIsAHypertable() {
        Integer hypertables = jdbc.queryForObject(
                "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'pulsepipe_events'",
                Integer.class);
        assertEquals(1, hypertables);
    }

    @Test
    void batchWritePersistsAllEvents() {
        long base = System.currentTimeMillis();
        List<Event> batch = List.of(
                sampleEvent("device-001", base),
                sampleEvent("device-001", base + 1),
                sampleEvent("device-002", base + 2));

        int inserted = repository.handleBatch(batch);

        assertEquals(3, inserted);
        assertEquals(3, repository.countAll());
    }

    @Test
    void duplicateEventsDoNotCreateDuplicateRows() {
        long base = System.currentTimeMillis();
        // Same eventId AND same timestamp -> same identity for the unique index.
        Event duplicated = new Event(UUID.randomUUID().toString(), "device-dup",
                Event.EVENT_TYPE_TELEMETRY, base, Map.of("temperature", 1.0));
        List<Event> batch = List.of(duplicated, duplicated, duplicated);

        repository.handleBatch(batch);
        int secondPass = repository.handleBatch(batch);

        assertEquals(0, secondPass, "replayed batch must insert nothing");
        Integer dupRows = jdbc.queryForObject(
                "SELECT count(*) FROM pulsepipe_events WHERE device_id = 'device-dup'", Integer.class);
        assertEquals(1, dupRows, "only one row must exist for a duplicated event identity");
    }

    @Test
    void timeWindowQueryByDeviceWorks() {
        long base = System.currentTimeMillis();
        repository.handleBatch(List.of(
                sampleEvent("device-query", base),
                sampleEvent("device-query", base + 10_000),
                sampleEvent("device-query", base + 100_000)));

        List<Event> window = repository.findByDeviceAndTimeRange("device-query", base, base + 20_000, 100);
        assertEquals(2, window.size());
        assertTrue(window.stream().allMatch(e -> e.timestamp() >= base && e.timestamp() <= base + 20_000));
    }

    // ----------------------------- RustFS (live) -----------------------------

    @Test
    @EnabledIfEnvironmentVariable(named = "PULSEPIPE_INTEGRATION", matches = "true")
    void rustfsRoundTripUploadDownload() {
        String endpoint = System.getenv().getOrDefault("RUSTFS_ENDPOINT", "http://localhost:9000");
        String accessKey = System.getenv().getOrDefault("RUSTFS_ACCESS_KEY", "pulsepipe");
        String secretKey = System.getenv().getOrDefault("RUSTFS_SECRET_KEY", "change-me");
        String bucket = System.getenv().getOrDefault("STORAGE_BUCKET", "pulsepipe");

        S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(accessKey, secretKey)))
                .forcePathStyle(true)
                .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .build())
                .build();

        // The bucket must already exist (created by rustfs-bucket-init container).
        boolean exists = s3.listBuckets().buckets().stream()
                .anyMatch(b -> b.name().equals(bucket));
        assertTrue(exists, "bucket '" + bucket + "' must be pre-created by rustfs-bucket-init");

        String key = "integration-test/" + UUID.randomUUID() + ".txt";
        byte[] payload = ("pulsepipe-roundtrip-" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);

        s3.putObject(software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                        .bucket(bucket).key(key).contentType("application/octet-stream").build(),
                software.amazon.awssdk.core.sync.RequestBody.fromBytes(payload));

        byte[] downloaded = s3.getObjectAsBytes(software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                .bucket(bucket).key(key).build()).asByteArray();

        assertArrayEquals(payload, downloaded);

        s3.deleteObject(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
                .bucket(bucket).key(key).build());
        s3.close();
    }
}
