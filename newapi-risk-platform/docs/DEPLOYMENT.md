# 部署手册

## 1. 模式选择

### 本机全家桶

`compose.local.yml` 启动 Gateway、PostgreSQL、Redis、Kafka KRaft 和 Ollama。适合开发、验收、小规模单机部署。

```bash
./scripts/generate-env.sh .env .env.example
vi .env
./scripts/bootstrap-local.sh
```

### 远端基础设施

`compose.remote.yml` 只启动 Gateway，连接远端 PostgreSQL、Redis、Kafka 和审计模型。适合生产。

```bash
cp .env.remote.example .env.remote
chmod 600 .env.remote
vi .env.remote
./scripts/doctor.sh remote
./scripts/bootstrap-remote.sh
```

### Kubernetes / Helm

Chart 不捆绑数据库、Redis、Kafka，默认按 3 副本部署网关：

```bash
cp deploy/helm/newapi-risk-gateway/values-production.example.yaml values-prod.yaml
vi values-prod.yaml
helm upgrade --install risk-gateway deploy/helm/newapi-risk-gateway \
  --namespace risk-system --create-namespace \
  -f values-prod.yaml
```

生产推荐把 Secret 交给 External Secrets/Vault 管理，并设置 `secret.existingSecret`。

## 2. PostgreSQL

最低要求：支持分区表和 `SKIP LOCKED` 的 PostgreSQL。推荐：

- TLS `sslmode=verify-full`；
- 独立用户和数据库；
- Gateway 用户需要建表、建/删日分区、CRUD 权限；
- `DATABASE_MAX_CONNS` 应乘以网关副本数后仍小于数据库可用连接上限；
- 对 `request_traces` 的长期分析应消费 Kafka 到数仓，不要延长热表到多年。

备份和恢复：

```bash
./scripts/backup-pg.sh
./scripts/restore-pg.sh backups/risk-YYYYMMDD.sql.gz
```

恢复前先停止写流量。生产环境应使用数据库服务自身的 PITR/WAL 备份；脚本是便携基线，不替代 PITR。

## 3. Redis

URL 支持：

```text
redis://:password@host:6379/0
rediss://default:password@host:6380/0
```

参数：

- `REDIS_REQUIRED=true`：生产推荐。启动和 readiness 要求 Redis 可用。
- `REDIS_POOL_SIZE`：每个实例连接池上限。
- `REDIS_MIN_IDLE_CONNS`：预热连接数。

当前实现使用标准 Redis Endpoint。云 Redis Cluster/Sentinel 应提供兼容的统一连接端点或代理入口。不要把 Redis 暴露公网。

## 4. Kafka

### 本地 KRaft

Compose 使用单 Broker/controller，仅用于单机。Topic 可自动创建，也可手工执行：

```bash
./scripts/init-kafka.sh
```

### 远端 SASL/TLS

`.env.remote` 示例：

```dotenv
KAFKA_BROKERS=kafka-1:9093,kafka-2:9093,kafka-3:9093
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_USERNAME=risk_gateway
KAFKA_PASSWORD=...
KAFKA_TLS_ENABLED=true
KAFKA_TLS_INSECURE_SKIP_VERIFY=false
KAFKA_CA_CERT_FILE=/run/secrets/kafka/ca.pem
KAFKA_CLIENT_CERT_FILE=
KAFKA_CLIENT_KEY_FILE=
KAFKA_AUTO_CREATE_TOPICS=false
KAFKA_TOPIC_PARTITIONS=24
KAFKA_REPLICATION_FACTOR=3
KAFKA_RETENTION_DAYS=365
```

管理员创建 Topic：

```bash
export KAFKA_BOOTSTRAP='kafka-1:9093,kafka-2:9093,kafka-3:9093'
export KAFKA_COMMAND_CONFIG=/secure/client.properties
./scripts/init-kafka-remote.sh
```

`client.properties` 示例：

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="risk_gateway" password="REDACTED";
ssl.truststore.location=/secure/kafka.truststore.jks
ssl.truststore.password=REDACTED
```

只消费审计 Topic 查看事件：

```bash
./scripts/kafka-consume.sh audit
```

管理台修改 `Kafka retention days` 后会调用 Kafka Admin API。若运行账号没有 AlterConfigs 权限，设置仍会保存，但返回 warning；管理员随后执行脚本应用。

## 5. 审计模型

默认本地：

```dotenv
AUDIT_MODEL_ENABLED=true
AUDIT_MODEL_URL=http://ollama:11434/v1/chat/completions
AUDIT_MODEL_NAME=qwen3.5:4b
AUDIT_FAIL_MODE=rules_only
```

远端只要兼容 OpenAI Chat Completions 即可。模型必须返回严格 JSON 分类结果；客户端会在不支持 `response_format` 时自动降级重试。

失败模式：

- `rules_only`：默认，模型故障时仅依赖确定性规则。
- `allow`：fail-open；吞吐优先但风险更高。
- `block`：fail-closed；合规优先但模型故障会阻断正常请求。

建议为审计模型设置独立容量、超时和熔断监控，不要与被代理的大模型共用同一个配额。

## 6. 反向代理

`deploy/nginx/risk-gateway.conf` 提供 SSE 友好的 Nginx 示例。重点：

- 关闭响应缓冲；
- 足够长的 read/send timeout；
- 不缓存；
- 只信任你控制的代理层后再设置 `TRUST_PROXY_HEADERS=true`；
- 验证代理/CDN 是否透传 HTTP 555。

## 7. 可观测性

```bash
./scripts/deploy.sh observability-up
```

默认只在本机开放 Prometheus、Grafana、Kafka UI。Grafana 密码来自 `.env` 的 `GRAFANA_ADMIN_PASSWORD`。

关键指标：

- `risk_gateway_requests_total`
- `risk_gateway_blocked_total`
- `risk_gateway_normalized_555_total`
- `risk_gateway_request_duration_seconds`
- `risk_gateway_kafka_published_total`
- `risk_gateway_kafka_failures_total`
- `risk_gateway_outbox_pending`

## 8. 发布和回滚

建议镜像使用不可变版本和 commit SHA，不使用 `latest`：

```bash
VERSION=0.1.0 COMMIT=$(git rev-parse --short HEAD) docker compose -f compose.remote.yml build
```

升级：

```bash
./scripts/upgrade.sh remote
```

回滚时切回上一镜像 Tag。数据库迁移目前为向前兼容建表/加索引；仍应在生产升级前完成快照和 staging 演练。
