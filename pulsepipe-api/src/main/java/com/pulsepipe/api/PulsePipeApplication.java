package com.pulsepipe.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * PulsePipe main application: MQTT ingest -> Kafka -> TimescaleDB + HTTP API.
 */
@SpringBootApplication
public class PulsePipeApplication {

    public static void main(String[] args) {
        SpringApplication.run(PulsePipeApplication.class, args);
    }
}
