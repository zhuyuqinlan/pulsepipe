# PulsePipe 架构与可靠性边界

## 数据链路

```text
MQTT 设备 (pulsepipe-simulator / 真实设备)
    │  MQTT QoS 1, topic: device/{deviceId}/telemetry
    ▼
EMQX (用户名密码认证, built_in_database)
    │
    ▼
pulsepipe-api: MqttIngestService (Paho client, 自动重连)
    │  回调线程只做 JSON 解析 + 入队
    │  ArrayBlockingQueue (有界, 默认 10000)
    ▼
KafkaEventPublisher (异步 producer, acks=all, idempotence)
    │  key = eventId, topic = pulsepipe.events
    ▼
Kafka (KRaft 模式)
    │  consumer group: pulsepipe-consumer
    │  批量拉取 max.poll.records=1000
    ▼
IngestBatchListener (AckMode.MANUAL_IMMEDIATE)
    │  失败重试 (默认 3 次, 退避 1s)
    ▼
TimescaleEventRepository (JDBC Batch, ON CONFLICT DO NOTHING)
    │  写库成功后才 ack → 提交 Offset
    ▼
TimescaleDB (Hypertable: pulsepipe_events, 按 event_timestamp 分区)

RustFS (S3 兼容): 由 rustfs-bucket-init 容器幂等建桶,
Java 侧通过 AWS SDK v2 上传/下载归档文件, 后端不自动建桶。
```

## 可靠性边界（明确不承诺端到端 Exactly Once）

| 环节 | 语义 | 说明 |
|------|------|------|
| MQTT 发布/订阅 | QoS 1, at-least-once | 设备→EMQX 与 EMQX→Java 都可能重复投递 |
| EMQX 会话队列 | broker 侧有界队列 | 订阅端消费不过来时 broker 会丢弃（默认 max_mqueue_len=1000，部署中已调至 100000）；压测 10k msg/s 下调优后零丢失 |
| Kafka 生产 | acks=all + 幂等 producer | broker 端不丢；重试可能产生 broker 内重复消息 |
| Offset 提交 | 写库成功后手动提交 (MANUAL_IMMEDIATE) | JVM 崩溃在"写库后、提交前"会导致重新消费 → 重复 |
| 数据库写入 | UNIQUE(event_id, event_timestamp) + ON CONFLICT DO NOTHING | 幂等，重复消费不会产生重复行 |

结论：整条链路是 **at-least-once + 数据库幂等去重**。这不是端到端 Exactly Once；
重复事件可能出现在 Kafka 中，但不会在 TimescaleDB 中产生重复记录。

## 关键设计决策

1. **MQTT 回调线程不做重活**：`messageArrived` 只解析 JSON 并向有界队列 `offer`，
   Kafka 投递与数据库写入都发生在独立线程。队列满时事件被丢弃并计数（背压可见）。
2. **Hypertable 唯一索引**：TimescaleDB 要求唯一约束包含所有分区维度，
   因此幂等键是 `(event_id, event_timestamp)` 而不是单纯的 `event_id`。
   `event_timestamp` 来自事件本身，同一条事件重放时两个字段都不变。
3. **Offset 提交时机**：批量写库成功 → `acknowledge()` → 提交。失败重试 3 次，
   仍失败则丢弃该批（ERROR 日志 + 计数），避免毒丸批次永久阻塞分区。
4. **Kafka Key**：使用 `eventId`，保证同一事件落在同一分区，顺序消费无乱序问题。
5. **RustFS 建桶职责分离**：基础设施层（compose init 容器）负责建桶且幂等；
   Java 后端只检查桶是否存在，不自动创建。
