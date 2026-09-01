# 运维与扩容

## 1. 日常检查

```bash
./scripts/doctor.sh local
./scripts/healthcheck.sh
./scripts/deploy.sh status
./scripts/deploy.sh logs
```

`/healthz` 只说明进程存活；`/readyz` 根据 PostgreSQL、以及被配置为 required 的 Redis/Kafka 判断能否接流量。

## 2. 告警建议

至少配置：

- readiness 连续失败；
- 5 分钟 555 比例异常上升；
- 审计 block 比例突变；
- P95/P99 延迟超目标；
- Kafka publish failure 增长；
- Outbox pending 持续增长或最老事件年龄过大；
- PostgreSQL 连接池耗尽、磁盘/WAL 异常；
- Redis 延迟或内存驱逐；
- 审计模型超时/降级比例；
- Gateway panic/restart。

## 3. 扩容顺序

1. 先确认审计模型是否瓶颈；增加模型副本或 `AUDIT_MODEL_MAX_CONCURRENCY` 前做模型端压测。
2. 增加 Gateway 副本；Redis 负责共享限流和配置通知。
3. 调整 PostgreSQL 总连接预算：`副本数 × DATABASE_MAX_CONNS`。
4. 增加 Kafka Topic 分区和 Broker；分区只能增加，不能直接减少。
5. 按流量调大 `AUDIT_QUEUE_SIZE`，但不要用无限队列掩盖下游长期故障。

## 4. 数据留存

- 热数据：管理台设置 `hot_retention_days`，维护任务按天删除旧分区。
- Kafka：管理台设置 `kafka_retention_days`；`-1` 表示无限保留，生产使用前评估磁盘成本。
- `tracking_records`、Outbox 交付历史会按热窗口清理。
- 需要多年归档时运行独立 Kafka consumer，把事件写对象存储/数仓，并按 `event_id` 去重。

更改留存不会恢复已经删除的数据。

## 5. Outbox 故障处理

Kafka 故障时：

1. 请求和 PostgreSQL 热记录继续；
2. `event_outbox` pending 增长；
3. 投递器指数退避；
4. Kafka 恢复后自动补发；
5. 超过重试上限的事件尝试写 `risk.deadletter`，并标记 deadlettered。

排查：

```bash
curl -fsS http://127.0.0.1:8088/metrics | grep -E 'kafka|outbox'
./scripts/kafka-consume.sh deadletter
```

消费者必须按 `event_id` 幂等，因为在“Kafka 已接收、数据库尚未标记 delivered”时崩溃会造成重复投递。

## 6. 备份

本机：

```bash
./scripts/backup-pg.sh
```

生产：启用 PostgreSQL PITR、定期恢复演练；备份 Kafka Topic 配置和 ACL；保存 Helm values 的非密钥部分。Redis 中的数据可重建，但限流窗口和缓存会丢失。

## 7. 密钥轮换

- `ADMIN_TOKEN`、`TRACKING_TOKEN`：更新 Secret 并滚动重启。
- `AUDIT_MODEL_API_KEY`：可在管理台更新；数据库保存加密值。
- 渠道 managed secret：更新对应路由。
- `MASTER_KEY`：当前不能直接无损替换，因为已有密文依赖旧密钥。轮换前导出/解密后重新加密，或采用外部 KMS 封装。不要直接覆盖并重启。
- `HASH_SECRET`：更换会导致用户/Token/IP 哈希无法与历史记录关联，应按隐私和审计计划执行。

## 8. 压测

```bash
VUS=100 DURATION=2m \
TARGET_URL=http://host.docker.internal:8088/r/default/v1/chat/completions \
UPSTREAM_API_KEY=... \
./scripts/loadtest.sh
```

压测数字只有在真实审计模型、真实渠道延迟、目标 payload 大小和 SSE 比例下才有意义。重点观察吞吐、P95/P99、错误率、模型队列、PG 连接、Outbox 和内存。

## 9. 灾难演练

每次正式发布前至少演练：

- 停 Kafka 10 分钟再恢复，确认 Outbox 回落；
- 停 Redis，确认 required/optional 模式符合预期；
- 审计模型返回超时/坏 JSON，验证 fail mode；
- 渠道在 SSE 中途断开；
- PostgreSQL failover；
- 滚动升级期间持续长连接；
- 恢复一份 PostgreSQL 备份到隔离环境。
