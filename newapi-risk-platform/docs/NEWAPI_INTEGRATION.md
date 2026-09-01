# NewAPI 接入手册

## 1. 推荐拓扑

```text
Client -> NewAPI -> Risk Gateway -> Provider
```

网关放在 NewAPI 与每个渠道之间。客户端仍只使用 NewAPI，不直接接触风控管理接口或真实渠道地址。

## 2. 创建路由

管理台或 API 创建：

```bash
curl -X POST 'https://risk.example.com/api/admin/v1/routes' \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{
    "key":"provider-a",
    "name":"Provider A production",
    "base_url":"https://provider-a.example.com",
    "auth_mode":"passthrough",
    "headers":{},
    "model_map":{"gpt-alias":"provider-model-v2"},
    "enabled":true,
    "timeout_ms":600000,
    "normalize_errors":true,
    "normalize_statuses":[401,403,408,409,429,500,502,503,504],
    "normalize_patterns":["model not found","model overloaded"],
    "allow_private_target":false
  }'
```

认证模式：

- `passthrough`：保留 NewAPI 渠道配置发送的 Authorization。最简单。
- `managed`：网关保存渠道密钥并替换 Authorization；数据库只存 AES-GCM 密文。更新时 `managed_secret` 留空表示保留原密钥。
- `none`：删除 Authorization，适合内网免认证服务或通过静态非 Authorization Header 认证的服务。

出于安全原因，`headers` 不允许覆盖 Authorization、Host、Content-Length 和 hop-by-hop Header。

## 3. 修改 NewAPI 渠道

假设网关路由键为 `provider-a`：

```text
NewAPI Channel Base URL = https://risk.example.com/r/provider-a
```

网关保留原始路径和 query：

```text
NewAPI -> POST /r/provider-a/v1/chat/completions?x=1
Gateway -> POST https://provider-a.example.com/v1/chat/completions?x=1
```

默认路由也可直接使用：

```text
https://risk.example.com
```

此时 `/v1/*` 使用 `DEFAULT_ROUTE_KEY`。生产建议使用显式 `/r/{route_key}`，便于渠道级统计、隔离和错误策略。

## 4. 请求标识和用户信息

网关识别以下标头：

| Header | 用途 |
|---|---|
| `X-Request-ID` | 由 NewAPI 生成的请求唯一 ID；不合法时网关重建 |
| `traceparent` | W3C Trace Context；合法 trace-id 会复用 |
| `X-Risk-User-ID` | 推荐的 NewAPI 用户 ID；仅保存 HMAC 哈希 |
| `X-User-ID` | 用户 ID 兼容字段 |
| `New-Api-User` / `X-New-Api-User` | NewAPI 用户兼容字段 |
| `X-Risk-Route` | 可选；通常使用 URL 中的 route key |

返回始终包含：

```text
X-Request-ID: req_...
X-Trace-ID: trace_...
```

NewAPI 最好把这两个字段写入自身日志并返回给客户端，方便跨层查询。

## 5. 主动追踪接口

代理层已自动记录渠道请求。主动接口用于补充 NewAPI 才知道的用户、Token、渠道 ID、计费 Token 和业务元数据。

```http
POST /api/v1/track
Authorization: Bearer <TRACKING_TOKEN>
Content-Type: application/json
```

支持 `event`：`start`、`finish`、`error`、`usage`。同一 `request_id` 重复上报会更新记录；Kafka 中每次上报仍是独立事件。

建议流程：

1. NewAPI 准备调用渠道时上报 `start`；
2. 把相同 `request_id` 放入代理 Header；
3. 完成后上报 `finish` 或 `error`，附 usage；
4. 管理员通过 `GET /api/v1/track/{request_id}` 查询聚合后的最新记录。

## 6. 555 处理

NewAPI 应把 `555` 识别为**不重试同一请求到其他渠道**或按你的商业策略处理。建议区分：

- `error.type=risk_control_error` 且审计阻断：向用户返回安全提示，不重试；
- `error.type=risk_control_error` 且渠道错误归一化：可由 NewAPI 根据 `request_id` 查询追踪中的 `error_class` 决定是否切换渠道；
- SSE 流中 `event:error`：停止继续消费并按 `error.code=555` 处理。

当前统一错误体故意不暴露内部规则、渠道原始错误和供应商密钥，详细原因只保存在管理台追踪中。

## 7. 内网渠道

默认 SSRF 策略阻止 loopback、私网、link-local 和未指定地址。仅当网关确实需要访问内网渠道时：

- 全局设置 `ALLOW_PRIVATE_UPSTREAMS=true`；或
- 对单个路由设置 `allow_private_target=true`。

二者都应配合网络策略限制网关可访问的网段。不要仅依赖应用层 SSRF 规则。

## 8. 兼容性测试清单

- Chat Completions 非流式与流式；
- Responses API 非流式与流式；
- 文件/音频 multipart 透明传输；
- NewAPI 渠道密钥 passthrough/managed；
- 供应商 401、429、5xx；
- HTTP 200 但 body 为 error；
- SSE 中途 error 与断流；
- CDN/Nginx 对 555 的透传；
- 长请求超时与客户端取消；
- 用户/Token 标识是否贯穿日志。
