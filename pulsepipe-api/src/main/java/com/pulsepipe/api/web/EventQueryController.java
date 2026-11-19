package com.pulsepipe.api.web;

import com.pulsepipe.common.model.Event;
import com.pulsepipe.storage.db.TimescaleEventRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "事件查询", description = "只读管理/查询接口，HTTP 不是事件接入协议")
@RestController
@RequestMapping("/api/events")
public class EventQueryController {

    private final TimescaleEventRepository repository;

    public EventQueryController(TimescaleEventRepository repository) {
        this.repository = repository;
    }

    @Operation(summary = "按事件 ID 查询", description = "返回该事件 ID 的全部记录（at-least-once 可能产生多条相同记录），不存在返回 404")
    @GetMapping("/{eventId}")
    public ResponseEntity<List<Event>> byId(
            @Parameter(description = "事件唯一 ID") @PathVariable String eventId) {
        List<Event> events = repository.findByEventId(eventId);
        if (events.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(events);
    }

    @Operation(summary = "按设备与时间范围查询", description = "必填 deviceId；startTime/endTime 为毫秒时间戳（可选）；limit 1-10000，默认 100")
    @GetMapping
    public ResponseEntity<List<Event>> query(
            @Parameter(description = "设备 ID，必填") @RequestParam String deviceId,
            @Parameter(description = "起始时间（毫秒时间戳，可选）") @RequestParam(required = false) Long startTime,
            @Parameter(description = "结束时间（毫秒时间戳，可选）") @RequestParam(required = false) Long endTime,
            @Parameter(description = "返回条数上限，1-10000") @RequestParam(defaultValue = "100") int limit) {
        if (deviceId.isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        if (limit < 1 || limit > 10_000) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(repository.findByDeviceAndTimeRange(deviceId, startTime, endTime, limit));
    }
}
