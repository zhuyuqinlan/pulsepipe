package com.pulsepipe.api.web;

import com.pulsepipe.common.model.Event;
import com.pulsepipe.storage.db.TimescaleEventRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read-only management/query API. HTTP is not an ingestion protocol.
 */
@RestController
@RequestMapping("/api/events")
public class EventQueryController {

    private final TimescaleEventRepository repository;

    public EventQueryController(TimescaleEventRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/{eventId}")
    public ResponseEntity<List<Event>> byId(@PathVariable String eventId) {
        List<Event> events = repository.findByEventId(eventId);
        if (events.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(events);
    }

    @GetMapping
    public ResponseEntity<List<Event>> query(
            @RequestParam String deviceId,
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime,
            @RequestParam(defaultValue = "100") int limit) {
        if (deviceId.isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        if (limit < 1 || limit > 10_000) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(repository.findByDeviceAndTimeRange(deviceId, startTime, endTime, limit));
    }
}
