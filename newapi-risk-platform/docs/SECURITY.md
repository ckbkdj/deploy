# 安全说明

## 威胁模型

本服务位于高权限数据路径，可接触用户 Prompt、渠道密钥、模型输出、用户 ID 和 Token 标识。主要风险包括：

- 管理接口被接管；
- 渠道密钥泄漏；
- SSRF 访问云元数据或内网控制面；
- Prompt/响应中的个人信息被过度留存；
- 审计模型提示注入或错误分类；
- Kafka/Redis/PostgreSQL 暴露公网；
- 错误归一化掩盖真实渠道故障；
- 非标准 555 被中间件错误处理。

## 已实现控制

- 管理与追踪 Token 分离，常量时间比较；
- 生产环境 Secret 长度和配置严格校验；
- 托管渠道 Secret、审计模型 Key 使用 AES-GCM 加密后入库；
- 用户、Token、IP 使用带 Secret 的 HMAC-SHA256；
- 默认阻止私网/loopback/link-local，并在 DNS 解析后再次检查；
- 禁止重定向，避免 Authorization 被带到第二目标；
- 不允许路由自定义 Authorization/Host/hop-by-hop Header；
- 请求/响应有边界限制，JSON 管理接口拒绝未知字段；
- 默认 payload 仅脱敏捕获，也可完全关闭；
- 管理台 CSP、禁止 iframe、no-referrer；
- 数据库 Outbox 避免 Kafka 故障时静默丢事件；
- 日志不记录原始 Authorization 或完整 Prompt。

## 上线必须补充

- 在负载均衡/WAF 层限制管理台来源 IP，并启用企业身份认证；内置 Token 不是完整 RBAC。
- PostgreSQL、Redis、Kafka、审计模型全部使用私网和 TLS；用最小权限账号与 ACL。
- Kubernetes 使用 NetworkPolicy 或云安全组限制网关只能访问批准的渠道和基础设施。
- 管理页面不要直接暴露公网；需要公网时加入 SSO/MFA。
- 定期轮换 Token 和渠道密钥；把 Secret 放 Vault/KMS/External Secrets。
- 对规则误杀/漏检建立人工复核、版本控制、灰度和回滚流程。
- 根据业务法规配置数据最小化、访问审计、删除请求和跨境传输策略。
- 对二进制、图片、音频的内容审计需要独立多模态/文件扫描器；当前非 JSON body 只透明代理和元数据追踪。

## Payload 模式

- `none`：不存请求/响应 body，风险最低。
- `redacted`：保留有限 JSON，递归遮蔽常见密钥、Token、密码字段；无法保证识别所有自由文本隐私。
- `encrypted`：AES-GCM 加密有限 body，并记录摘要。加密不是无限制留存的理由。

## Cyber 规则边界

确定性规则面向明确的操作型滥用，不应被当作完整内容安全系统。小模型输出同样可能被提示注入、语言变体、编码或新型攻击绕过。生产应：

- 将规则与模型版本纳入变更审计；
- 用真实业务样本建立误杀/漏检回归集；
- 对高风险客户和能力增加独立策略；
- 不向终端用户暴露具体命中规则，以降低规避能力；
- 监控攻击者通过长文本、混合语言、Base64/Unicode 混淆绕过。

## 漏洞报告

不要在公开 Issue 中提交密钥、Prompt、真实用户数据或可直接利用的漏洞细节。先通过仓库所有者的私有安全渠道报告，并包含影响版本、复现条件和最小化日志。
