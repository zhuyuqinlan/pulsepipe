# PulsePipe 常用命令快捷入口

JAVA ?= java
# 优先走 mise 管理的 Java 25 工具链；未装 mise 时退回 PATH 上的 mvn
MISE := $(shell command -v mise >/dev/null 2>&1 && echo mise exec --)
MVN := $(MISE) mvn
ENV_FILE := .env
COMPOSE := docker compose --env-file $(ENV_FILE) -f deploy/docker-compose.dev.yml
SIMULATOR_JAR := pulsepipe-simulator/target/pulsepipe-simulator-0.1.0-SNAPSHOT.jar

# 加载 .env 后执行（运行主应用需要凭据）。注意：call 传入的参数不能含逗号
define run_with_env
set -a; [ -f ./$(ENV_FILE) ] && . ./$(ENV_FILE); set +a; $(1)
endef

.PHONY: help build test clean run infra infra-down simulate kafka-check query-events

help:
	@echo "常用目标:"
	@echo "  make build          - 构建全部模块（跳过测试）"
	@echo "  make test           - 运行全部测试"
	@echo "  make dev           - 开发模式启动主应用（等价 mvn spring-boot:run，自动加载 .env）"
	@echo "  make run           - 运行已构建的 jar（需先 make build）"
	@echo "  make infra          - 启动基础设施（EMQX/Kafka/TimescaleDB/RustFS）"
	@echo "  make infra-down     - 停止基础设施（保留数据卷）"
	@echo "  make simulate N=100 D=10 I=30 - 运行设备模拟器"
	@echo "  make kafka-check    - 查看 Kafka topic 与最新 offset"
	@echo "  make query-events LIMIT=10 - 查询 TimescaleDB 最近事件"
	@echo "  make clean          - 清理构建产物"

build:
	$(MVN) package -DskipTests

test:
	$(MVN) test

clean:
	$(MVN) clean

# 用法: make dev [ARGS="--server.port=8080"]
# 单模块 spring-boot:run 需要兄弟模块已 install；自动加载 .env
# （不用 call，因为模块列表的逗号会被 call 当参数分隔符）
dev:
	set -a; [ -f ./$(ENV_FILE) ] && . ./$(ENV_FILE); set +a; \
	$(MVN) install -DskipTests -pl pulsepipe-common,pulsepipe-mqtt,pulsepipe-kafka,pulsepipe-storage -am -q && \
	$(MVN) spring-boot:run -pl pulsepipe-api $(ARGS)

run:
	$(call run_with_env,$(JAVA) -jar pulsepipe-api/target/pulsepipe-api-0.1.0-SNAPSHOT.jar)

# 启动基础设施；首次运行自动从 .env.example 生成 .env
infra:
	@if [ ! -f $(ENV_FILE) ]; then \
		echo "$(ENV_FILE) not found — creating it from .env.example (edit passwords before production use)"; \
		cp $(ENV_FILE).example $(ENV_FILE); \
	fi
	$(COMPOSE) up -d
	@echo "Waiting for all services to become healthy..."
	@$(COMPOSE) ps

infra-down:
	$(COMPOSE) down

# 用法: make simulate N=100 D=10 I=30
simulate: build
	$(call run_with_env,$(JAVA) -jar $(SIMULATOR_JAR) \
		--broker "tcp://localhost:$${MQTT_PORT:-1883}" \
		--username "$${MQTT_USERNAME:-pulsepipe}" \
		--password "$${MQTT_PASSWORD:-change-me-strong-password}" \
		--devices $(or $(N),100) --rate $(or $(D),10) --duration $(or $(I),30))

# 防止 Git Bash (MSYS) 改写容器内路径
export MSYS_NO_PATHCONV := 1
export MSYS2_ARG_CONV_EXCL := *

# 注意：topic 由主应用自动创建，需先 make dev 再执行本命令
kafka-check:
	@echo "=== Topics ==="
	@$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
	@echo "=== pulsepipe.events latest offsets per partition ==="
	-@$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic pulsepipe.events --time latest

# 用法: make query-events LIMIT=10
query-events:
	$(COMPOSE) exec -T timescaledb psql \
		-U $${POSTGRES_USER:-pulsepipe} -d $${POSTGRES_DB:-pulsepipe} \
		-c "SELECT event_id, device_id, event_type, event_timestamp, payload FROM pulsepipe_events ORDER BY event_timestamp DESC LIMIT $(or $(LIMIT),10);"
