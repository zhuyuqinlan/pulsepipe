package com.pulsepipe.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

import java.util.Map;

/**
 * POST /api/events 注入事件请求 DTO。
 * eventId/timestamp/eventType 缺省时由服务端自动补齐。
 */
@Schema(description = "注入事件请求体")
public record EventIngestRequest(

        @Schema(description = "事件唯一 ID（UUID）；缺省时自动生成", example = "3f2c1a9e-8b7d-4c2e-a1f0-9e8d7c6b5a43")
        String eventId,

        @Schema(description = "设备 ID，必填", example = "device-001")
        @NotBlank(message = "deviceId 不能为空")
        String deviceId,

        @Schema(description = "事件类型；缺省为 telemetry", example = "telemetry")
        String eventType,

        @Schema(description = "事件时间戳（毫秒）；缺省为服务器当前时间", example = "1793600000000")
        Long timestamp,

        @Schema(description = "事件负载（任意 JSON 对象）；缺省为空对象", example = "{\"temperature\": 26.5}")
        Map<String, Object> payload) {
}
