# PulsePipe

面向工业设备、IoT 和机器人的**实时事件采集与处理平台** MVP。

设备通过 MQTT 上报遥测事件，经过 EMQX、Kafka，最终批量幂等地写入 TimescaleDB；
RustFS（S3 兼容对象存储）用于后续历史事件归档。

## 架构

```text
MQTT 设备 (pulsepipe-simulator / 真实设备)
    │  MQTT QoS 1, topic: device/{deviceId}/telemetry
    ▼
EMQX ──► Java MQTT Client (pulsepipe-api, 自动重连 + 有界队列)
    │        回调线程只解析入队, 不做 DB 操作
    ▼
Kafka (KRaft, topic: pulsepipe.events, key = eventId)
    │        异步生产者 acks=all; 批量消费
    ▼
Java Consumer (写库成功后才提交 Offset, 失败重试)
    ▼
TimescaleDB (Hypertable, JDBC Batch + ON CONFLICT 幂等写入)

RustFS (S3 兼容) ◄── AWS SDK v2 上传/下载归档文件
    └─ 桶由 rustfs-bucket-init 一次性容器幂等创建
```

**可靠性边界**：MQTT QoS 1 + Kafka acks=all + 写库后提交 Offset = **at-least-once**。
重复消息由 TimescaleDB 唯一索引 `(event_id, event_timestamp)` 幂等去重，
**不是端到端 Exactly Once**。详见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 环境要求

- Java 21（Maven 构建）
- Maven 3.9+
- Docker + Docker Compose（或 Podman + Podman Compose）

## 快速开始

### 1. 启动基础设施

```bash
cp .env.example .env        # 按需修改密码
./scripts/start-infra.sh    # docker compose up -d (EMQX/Kafka/TimescaleDB/RustFS)
```

包含的服务：

| 服务 | 端口 | 说明 |
|------|------|------|
| EMQX | 1883 / 18083 | MQTT Broker / Dashboard（admin/public，仅本机调试） |
| Kafka (KRaft) | 29092 | EXTERNAL listener，宿主机 Java 客户端使用 |
| TimescaleDB | 5432 | 库 pulsepipe，用户/密码见 .env |
| RustFS | 9000 / 9001 | S3 API / Web 控制台（凭据见 .env） |
| emqx-auth-init | - | 一次性容器：注册 MQTT 用户（幂等） |
| rustfs-bucket-init | - | 一次性容器：`rc bucket create --ignore-existing` 幂等建桶 |

### 2. 构建并启动 Java 服务（宿主机运行，不入容器）

```bash
mvn package
# 加载 .env 中的凭据后再启动（DB/MQTT/RustFS 密码均来自 .env）
set -a; source .env; set +a
java -jar pulsepipe-api/target/pulsepipe-api-0.1.0-SNAPSHOT.jar
# 注意：若 shell 环境已有 SERVER__PORT 之类变量，用 --server.port=8080 覆盖
```

启动后自动：连接 EMQX 并订阅 `device/+/telemetry` → 创建 `pulsepipe.events` Topic →
启动 Kafka 批量消费者 → 批量写入 TimescaleDB。

### 3. 模拟设备发送事件

```bash
# 1000 台设备, 每台每秒 10 条, 持续 60 秒
java -jar pulsepipe-simulator/target/pulsepipe-simulator-0.1.0-SNAPSHOT.jar \
  --devices 1000 --rate 10 --duration 60

# 或使用脚本（默认 100 台/10 条/30 秒）
./scripts/simulate.sh 100 10 30
```

模拟器输出真实连接数、发送成功/失败数（基于 Paho QoS1 回调统计）。

### 4. 验证链路

**健康与链路状态**：

```bash
curl http://localhost:8080/api/health
```

**Kafka 消息验证**：

```bash
./scripts/verify-kafka.sh
# 或直接查看 topic 增长
docker compose -f deploy/docker-compose.yml exec kafka \
  kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic pulsepipe.events --time latest
```

**TimescaleDB 查询**：

```bash
./scripts/query-events.sh 10
# 或
docker compose -f deploy/docker-compose.yml exec timescaledb psql -U pulsepipe -d pulsepipe \
  -c "SELECT device_id, count(*) FROM pulsepipe_events GROUP BY device_id ORDER BY 1 LIMIT 10;"
```

**HTTP API 查询事件**：

```bash
# 按设备 + 时间窗查询（epoch 毫秒）
curl "http://localhost:8080/api/events?deviceId=sim-00001&startTime=1793600000000&endTime=1993600000000"
# 按 eventId 查询
curl http://localhost:8080/api/events/{eventId}
```

### 5. RustFS 文件测试

- Web 控制台：<http://localhost:9001>（Access Key / Secret Key 见 .env）
- Java 集成测试（需基础设施已启动）：

```bash
PULSEPIPE_INTEGRATION=true RUSTFS_ENDPOINT=http://localhost:9000 \
  RUSTFS_ACCESS_KEY=pulsepipe RUSTFS_SECRET_KEY=<your-secret> \
  mvn test -pl pulsepipe-storage -Dtest='StorageIntegrationTest#rustfsRoundTripUploadDownload' -am
```

## HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/health` | 各链路阶段状态与计数（MQTT/Kafka/DB/RustFS） |
| GET | `/api/events/{eventId}` | 按 eventId 查询 |
| GET | `/api/events?deviceId=&startTime=&endTime=&limit=` | 按设备 + 时间窗查询 |

HTTP 仅用于管理与查询，不作为事件接入协议。

## 测试

```bash
mvn test    # 单元测试 + TimescaleDB 集成测试（Testcontainers, 需要 Docker）
```

覆盖：事件 JSON 编解码、有界队列丢弃与解析容错、Offset 提交时机与重试/毒丸策略、
Hypertable 校验、批量写入、重复消息幂等（Testcontainers 真库）、RustFS 往返校验（联调环境）。

## 项目结构

```text
pulsepipe/
├── pulsepipe-common/     # 统一事件模型、JSON 编解码、管道端口接口
├── pulsepipe-mqtt/       # MQTT 接入（Paho、自动重连、有界队列）
├── pulsepipe-kafka/      # Kafka 异步生产者、批量消费者（手动 Offset）
├── pulsepipe-storage/    # TimescaleDB 批量幂等写入、RustFS S3 访问
├── pulsepipe-api/        # Spring Boot 主应用 + HTTP API
├── pulsepipe-simulator/  # MQTT 设备模拟器
├── deploy/               # docker-compose.yml + TimescaleDB 初始化 SQL
├── scripts/              # 启停、模拟、验证脚本
└── docs/                 # 架构与可靠性边界说明
```

## Docker / Podman 兼容说明

- 绑定挂载使用 `:z`（SELinux 重新标记）。
- RustFS 命名卷使用 `:U`（Podman 自动处理卷目录属主；Docker Compose 亦支持该选项）。
- 所有服务通过内部网络以服务名互访；宿主机访问使用映射端口。
