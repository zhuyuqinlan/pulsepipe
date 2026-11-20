package com.pulsepipe.api.web;

import com.pulsepipe.api.dto.EventIngestRequest;
import com.pulsepipe.api.vo.EventIngestResponse;
import com.pulsepipe.common.model.Event;
import com.pulsepipe.common.pipeline.EventPublisher;
import com.pulsepipe.storage.db.TimescaleEventRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 管理/查询 API。POST 用于管理侧手动注入事件（走 Kafka 管道，与 MQTT 接入同一链路），
 * 生产数据接入仍以 MQTT 为主协议。
 */
@Tag(name = "事件", description = "事件查询（只读）与管理侧事件注入")
@RestController
@RequestMapping("/api/events")
public class EventQueryController {

    private final TimescaleEventRepository repository;
    private final EventPublisher publisher;

    public EventQueryController(TimescaleEventRepository repository, EventPublisher publisher) {
        this.repository = repository;
        this.publisher = publisher;
    }

    /**
     * 管理侧手动注入事件：走 EventPublisher（Kafka）管道，复用与 MQTT 接入
     * 完全相同的写入链路，保持 at-least-once 语义。eventId/timestamp/eventType
     * 缺省时自动补齐；deviceId 必填。
     */
    @Operation(summary = "注入事件",
            description = "将事件发布到 Kafka 管道（与 MQTT 接入同一链路，at-least-once 语义，消费端幂等去重）。返回 202 表示已受理；deviceId 缺失返回 400。",
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "注入请求体：deviceId 必填，eventId/timestamp/eventType/payload 可缺省自动补齐",
                    required = true))
    @PostMapping
    public ResponseEntity<EventIngestResponse> create(@Valid @RequestBody EventIngestRequest req) {
        Event event = new Event(
                req.eventId() == null || req.eventId().isBlank() ? UUID.randomUUID().toString() : req.eventId(),
                req.deviceId(),
                req.eventType() == null || req.eventType().isBlank() ? Event.EVENT_TYPE_TELEMETRY : req.eventType(),
                req.timestamp() == null ? System.currentTimeMillis() : req.timestamp(),
                req.payload() == null ? Map.of() : req.payload());
        publisher.publish(event);
        return ResponseEntity.accepted().body(EventIngestResponse.from(event));
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
