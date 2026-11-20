package com.pulsepipe.api.vo;

import com.pulsepipe.common.model.Event;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * 注入事件响应 VO：回显服务端补齐后的事件全字段。
 */
@Schema(description = "注入事件响应（回显补齐后的完整事件）")
public record EventIngestResponse(

        @Schema(description = "事件唯一 ID（服务端补齐后）", example = "3f2c1a9e-8b7d-4c2e-a1f0-9e8d7c6b5a43")
        String eventId,

        @Schema(description = "设备 ID", example = "device-001")
        String deviceId,

        @Schema(description = "事件类型", example = "telemetry")
        String eventType,

        @Schema(description = "事件时间戳（毫秒，服务端补齐后）", example = "1793600000000")
        long timestamp,

        @Schema(description = "事件负载", example = "{\"temperature\": 26.5}")
        Object payload) {

    public static EventIngestResponse from(Event event) {
        return new EventIngestResponse(
                event.eventId(), event.deviceId(), event.eventType(),
                event.timestamp(), event.payload());
    }
}
