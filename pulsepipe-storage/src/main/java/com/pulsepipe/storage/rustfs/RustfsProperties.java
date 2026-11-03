package com.pulsepipe.storage.rustfs;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * RustFS (S3-compatible) access configuration bound from {@code pulsepipe.rustfs.*}.
 */
@ConfigurationProperties(prefix = "pulsepipe.rustfs")
public class RustfsProperties {

    /** S3 API endpoint, e.g. http://localhost:9000 */
    private String endpoint = "http://localhost:9000";
    private String accessKey;
    private String secretKey;
    private String bucket = "pulsepipe";
    /** Upload a probe file on startup and verify round-trip integrity. */
    private boolean verifyOnStartup = false;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public boolean isVerifyOnStartup() {
        return verifyOnStartup;
    }

    public void setVerifyOnStartup(boolean verifyOnStartup) {
        this.verifyOnStartup = verifyOnStartup;
    }
}
