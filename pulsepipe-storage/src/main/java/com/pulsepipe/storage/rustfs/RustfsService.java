package com.pulsepipe.storage.rustfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * S3-compatible access to RustFS via AWS SDK for Java v2.
 *
 * <p>The bucket itself is created by the {@code rustfs-bucket-init} one-shot
 * container (idempotent, {@code rc bucket create --ignore-existing}). This
 * service only checks for its existence — the backend never auto-creates
 * buckets.</p>
 */
public class RustfsService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RustfsService.class);

    private final RustfsProperties props;
    private final S3Client s3;
    private final AtomicBoolean bucketPresent = new AtomicBoolean(false);

    public RustfsService(RustfsProperties props) {
        this.props = props;
        this.s3 = S3Client.builder()
                .endpointOverride(URI.create(props.getEndpoint()))
                .region(Region.US_EAST_1) // RustFS ignores region but SDK requires one
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(props.getAccessKey(), props.getSecretKey())))
                .forcePathStyle(true) // S3-compatible object stores use path-style addressing
                .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .build())
                .build();
    }

    /** @return true if the configured bucket exists (cached after first check). */
    public boolean bucketExists(boolean refresh) {
        if (!refresh && bucketPresent.get()) {
            return true;
        }
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(props.getBucket()).build());
            bucketPresent.set(true);
            return true;
        } catch (NoSuchBucketException e) {
            bucketPresent.set(false);
            return false;
        } catch (Exception e) {
            bucketPresent.set(false);
            throw new IllegalStateException("Failed to check RustFS bucket '" + props.getBucket() + "': " + e.getMessage(), e);
        }
    }

    public void upload(String key, byte[] data, String contentType) {
        s3.putObject(PutObjectRequest.builder()
                        .bucket(props.getBucket())
                        .key(key)
                        .contentType(contentType == null ? "application/octet-stream" : contentType)
                        .build(),
                RequestBody.fromBytes(data));
    }

    public byte[] download(String key) {
        ResponseBytes<GetObjectResponse> bytes =
                s3.getObjectAsBytes(GetObjectRequest.builder().bucket(props.getBucket()).key(key).build());
        return bytes.asByteArray();
    }

    /**
     * Round-trip integrity check: upload a probe object, download it back and
     * compare byte-for-byte.
     *
     * @return the downloaded bytes
     */
    public byte[] roundTripCheck(String key, byte[] payload) {
        upload(key, payload, "application/octet-stream");
        byte[] downloaded = download(key);
        if (!java.util.Arrays.equals(payload, downloaded)) {
            throw new IllegalStateException("RustFS round-trip check failed: downloaded bytes differ from uploaded payload");
        }
        log.info("RustFS round-trip check OK (bucket={}, key={}, {} bytes)", props.getBucket(), key, payload.length);
        return downloaded;
    }

    public boolean isConfigured() {
        return props.getAccessKey() != null && !props.getAccessKey().isBlank();
    }

    public String getBucket() {
        return props.getBucket();
    }

    public String getEndpoint() {
        return props.getEndpoint();
    }

    @Override
    public void close() {
        s3.close();
    }
}
