# AGENTS.md

面向 OpenCode 会话的仓库指南。回复与代码注释使用中文。

## Makefile 命令

| 命令 | 作用 |
|------|------|
| `make build` | 构建全部模块（跳过测试） |
| `make test` | 运行全部测试 |
| `make dev` | 开发模式启动主应用（先 install 依赖模块再 `spring-boot:run`，自动加载 `.env`；可 `ARGS="--server.port=8080"` 传参） |
| `make run` | 运行已构建的 api jar（自动加载 `.env`） |
| `make infra` | 启动基础设施（首次自动从 `.env.example` 生成 `.env`） |
| `make infra-down` | 停止基础设施（保留数据卷） |
| `make simulate N=100 D=10 I=30` | 运行设备模拟器（台数/速率/时长） |
| `make kafka-check` | 查看 Kafka topic 列表与 `pulsepipe.events` 最新 offset（需主应用已启动创建 topic） |
| `make query-events LIMIT=10` | 查询 TimescaleDB 最近事件 |
| `make clean` | 清理构建产物 |

## 命令

- 构建/测试：`mvn package`、`mvn test`（Java 21 + Maven 3.9+，`mise.toml` 固定 Java 21）
- 单模块测试：`mvn test -pl pulsepipe-storage -am`；单测类：`-Dtest='StorageIntegrationTest#rustfsRoundTripUploadDownload'`
- 基础设施（EMQX/Kafka/TimescaleDB/RustFS）：`make infra`（compose 文件在 `deploy/docker-compose.dev.yml`，首次运行自动从 `.env.example` 生成 `.env`）
- 运行主应用前必须加载凭据：`set -a; source .env; set +a`，然后 `java -jar pulsepipe-api/target/pulsepipe-api-0.1.0-SNAPSHOT.jar`。配置全靠环境变量（application.yml 中 `${VAR:default}`）；若 shell 已有 `SERVER__PORT` 之类变量需 `--server.port=8080` 覆盖
- Docker Compose 命令需带 `--env-file .env -f deploy/docker-compose.dev.yml`（Makefile 已封装）

## 测试前置条件

- `mvn test` 中的 TimescaleDB 测试用 Testcontainers，需要 Docker（或 Podman）
- RustFS 集成测试默认跳过，仅当 `PULSEPIPE_INTEGRATION=true` 且基础设施已启动、`RUSTFS_ENDPOINT`/`RUSTFS_ACCESS_KEY`/`RUSTFS_SECRET_KEY` 已设置时运行（见 `pulsepipe-storage` 的 `StorageIntegrationTest`，用 `@EnabledIfEnvironmentVariable` 门控）
- Kafka EXTERNAL listener 宿主机端口是 **29092**（`KAFKA_EXTERNAL_PORT`），不是 9092；9092 仅容器内部网络使用

## 架构（多模块 Maven，Spring Boot 3.3）

数据流：MQTT (Paho, topic `device/+/telemetry`) → Kafka (`pulsepipe.events`, key=eventId) → TimescaleDB 批量幂等写入；RustFS (S3 兼容) 归档。

- `pulsepipe-common`：统一事件模型、JSON 编解码、管道端口接口（其他模块依赖它）
- `pulsepipe-mqtt` / `pulsepipe-kafka` / `pulsepipe-storage`：接入层库，无 Spring 依赖驱动
- `pulsepipe-api`：Spring Boot 主应用 + HTTP API（`/api/health`、`/api/events`），HTTP 仅管理/查询用，不是事件接入协议
- `pulsepipe-simulator`：设备模拟器（`make simulate N=100 D=10 I=30`）

## 关键约束（改动前必读）

- 可靠性模型是 **at-least-once**，不是 Exactly Once：MQTT QoS 1 + Kafka acks=all + 写库成功后才提交 Offset；重复由 TimescaleDB 唯一索引 `(event_id, event_timestamp)` 幂等去重。改消费/写入逻辑时不要破坏这一链路。详见 `docs/ARCHITECTURE.md`
- MQTT 回调线程只解析入队（有界队列 10000），不做 DB 操作
- 幂等写入靠 JDBC Batch + `ON CONFLICT`，Hypertable 校验在测试中覆盖，勿改表结构除非同步 `deploy/timescaledb` 初始化 SQL
- Docker/Podman 兼容：挂载用 `:z`，RustFS 卷用 `:U`；服务间以 compose 服务名互访，宿主机用映射端口

## 参考文档

- `docs/ARCHITECTURE.md`：架构与可靠性边界（唯一文档，更新行为时同步）
- API 文档：应用启动后 Knife4j/Springdoc
