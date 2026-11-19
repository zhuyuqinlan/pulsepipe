package com.pulsepipe.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.boot.web.server.WebServer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;

import java.net.InetAddress;

/**
 * PulsePipe main application: MQTT ingest -> Kafka -> TimescaleDB + HTTP API.
 */
@SpringBootApplication
public class PulsePipeApplication {

    private static final Logger log = LoggerFactory.getLogger(PulsePipeApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(PulsePipeApplication.class, args);
    }

    /**
     * 应用就绪后打印对外暴露的服务地址，便于开发时直接点击访问。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void logExposedEndpoints(ApplicationReadyEvent event) {
        ApplicationContext ctx = event.getApplicationContext();
        if (!(ctx instanceof WebServerApplicationContext webCtx)) {
            return; // 非 Web 环境（如测试）不打印
        }
        WebServer server = webCtx.getWebServer();
        int port = server.getPort();
        Environment env = ctx.getEnvironment();
        // context-path（默认 "/"），保证拼接结果无双斜杠
        String ctxPath = env.getProperty("server.servlet.context-path", "/");
        while (ctxPath.startsWith("/")) {
            ctxPath = ctxPath.substring(1);
        }
        if (!ctxPath.isEmpty() && !ctxPath.endsWith("/")) {
            ctxPath = ctxPath + "/";
        }
        // 实际监听地址，未配置则视为绑定所有网卡
        String address = env.getProperty("server.address", "0.0.0.0");
        // 绑定所有网卡时展示回环地址更友好
        String host = "0.0.0.0".equals(address)
                ? InetAddress.getLoopbackAddress().getHostAddress()
                : address;
        String basePath = "http://%s:%d/%s".formatted(host, port, ctxPath);
        log.info("============================================================");
        log.info("  PulsePipe 服务已启动，对外暴露地址:");
        log.info("  本机访问:   {}api/health", basePath);
        log.info("  API 文档:   {}doc.html", basePath);
        log.info("  监听端口:   {} (绑定地址: {})", port, address);
        log.info("============================================================");
    }
}
