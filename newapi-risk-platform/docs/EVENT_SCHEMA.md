# Kafka 事件协议

所有 Topic 使用 UTF-8 JSON。Envelope：

```json
{
  "version": "1",
  "type": "risk.audit.decision",
  "event_id": "evt_...",
  "timestamp": "2026-09-01T16:00:00Z",
  "data": {}
}
```

## 兼容规则

- `version` 当前为字符串 `1`。
- 新字段只追加，不改变既有字段含义。
- 消费者忽略未知字段。
- `event_id` 是业务幂等键；Kafka 交付为至少一次。
- `timestamp` 是网关生成事件的 UTC 时间，不等于渠道完成时间的唯一事实来源。

## `risk.audit.events`

`type=risk.audit.decision`，`data` 为请求发送渠道前的审计结果：

```json
{
  "request_id":"req_...",
  "trace_id":"trace_...",
  "route_key":"provider-a",
  "model":"gpt-example",
  "result":{
    "decision":"allow",
    "score":0.12,
    "categories":[],
    "reason":"no operational cyber-abuse rule matched",
    "source":"model"
  }
}
```

Payload 是否存在由 `PAYLOAD_MODE` 决定。

## `risk.request.traces`

该 Topic 有两类事件：

- `type=risk.proxy.completed`：完整代理请求追踪，身份字段已是 `user_hash`、`token_hash`、`client_ip_hash`。
- `type=risk.tracking.received`：NewAPI 主动追踪；`user_id` 与 `token_id` 会在进入 Kafka 前转换为 HMAC-SHA256 的 `user_hash`、`token_hash`。

`metadata` 由调用方提供并原样进入 Kafka。NewAPI 不应在 metadata 中放渠道密钥、Authorization、完整手机号、证件号或不必要的个人信息。

## `risk.deadletter`

死信沿用原 Envelope，投递记录中保留失败原因。消费者应告警并把事件送人工/自动补偿流程，不能简单无限重放。

## Topic Key

- 审计事件：`request_id`；
- 主动追踪：`request_id`；
- 死信：原事件 key。

相同 key 在单个 Topic 分区内保持顺序，但跨 Topic 不保证全局顺序。
