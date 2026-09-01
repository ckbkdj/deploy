package config

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var routeKeyPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)

type Config struct {
	AppEnv                   string
	ListenAddr               string
	PublicBaseURL            string
	DatabaseURL              string
	DatabaseMaxConns         int32
	DatabaseMinConns         int32
	RedisURL                 string
	RedisRequired            bool
	RedisPoolSize            int
	RedisMinIdleConns        int
	KafkaEnabled             bool
	KafkaRequired            bool
	KafkaBrokers             []string
	KafkaClientID            string
	KafkaAuditTopic          string
	KafkaTraceTopic          string
	KafkaDeadLetterTopic     string
	KafkaAutoCreateTopics    bool
	KafkaTopicPartitions     int32
	KafkaReplicationFactor   int16
	KafkaSASLMechanism       string
	KafkaUsername            string
	KafkaPassword            string
	KafkaTLSEnabled          bool
	KafkaTLSInsecure         bool
	KafkaCACertFile          string
	KafkaClientCertFile      string
	KafkaClientKeyFile       string
	AdminToken               string
	TrackingToken            string
	HashSecret               string
	MasterKey                string
	RiskHTTPStatus           int
	DefaultRouteKey          string
	DefaultRouteName         string
	DefaultUpstreamBaseURL   string
	DefaultUpstreamAuthMode  string
	AuditQueueSize           int
	AuditWorkers             int
	AuditModelMaxConcurrency int
	AuditModelEnabled        bool
	AuditModelURL            string
	AuditModelName           string
	AuditModelAPIKey         string
	AuditTimeout             time.Duration
	AuditFailMode            string
	HotRetentionDays         int
	KafkaRetentionDays       int
	PayloadMode              string
	PayloadCaptureBytes      int64
	MaxRequestBodyBytes      int64
	MaxResponseBytes         int64
	RateLimitEnabled         bool
	RateLimitRPS             int
	RateLimitBurst           int
	AllowPrivateUpstreams    bool
	TrustProxyHeaders        bool
	NormalizeStatuses        []int
	NormalizePatterns        []string
	ShutdownTimeout          time.Duration
	CleanupInterval          time.Duration
	SettingsRefreshInterval  time.Duration
	RequestTimeout           time.Duration
	ClientReadTimeout        time.Duration
	ClientIdleTimeout        time.Duration
	MaxHeaderBytes           int
	UpstreamIdleConns        int
	UpstreamIdleConnsPerHost int
	UpstreamMaxConnsPerHost  int
}

func Load() (Config, error) {
	if err := validateRawEnvironment(); err != nil {
		return Config{}, err
	}
	cfg := Config{
		AppEnv:                   strings.ToLower(env("APP_ENV", "production")),
		ListenAddr:               env("LISTEN_ADDR", ":8080"),
		PublicBaseURL:            env("PUBLIC_BASE_URL", "http://localhost:8080"),
		DatabaseURL:              env("DATABASE_URL", "postgres://risk:risk@postgres:5432/risk?sslmode=disable"),
		DatabaseMaxConns:         int32(envInt("DATABASE_MAX_CONNS", 50)),
		DatabaseMinConns:         int32(envInt("DATABASE_MIN_CONNS", 5)),
		RedisURL:                 env("REDIS_URL", "redis://redis:6379/0"),
		RedisRequired:            envBool("REDIS_REQUIRED", false),
		RedisPoolSize:            envInt("REDIS_POOL_SIZE", 256),
		RedisMinIdleConns:        envInt("REDIS_MIN_IDLE_CONNS", 16),
		KafkaEnabled:             envBool("KAFKA_ENABLED", true),
		KafkaRequired:            envBool("KAFKA_REQUIRED", false),
		KafkaBrokers:             envCSV("KAFKA_BROKERS", []string{"kafka:9092"}),
		KafkaClientID:            env("KAFKA_CLIENT_ID", "newapi-risk-gateway"),
		KafkaAuditTopic:          env("KAFKA_AUDIT_TOPIC", "risk.audit.events"),
		KafkaTraceTopic:          env("KAFKA_TRACE_TOPIC", "risk.request.traces"),
		KafkaDeadLetterTopic:     env("KAFKA_DEADLETTER_TOPIC", "risk.deadletter"),
		KafkaAutoCreateTopics:    envBool("KAFKA_AUTO_CREATE_TOPICS", true),
		KafkaTopicPartitions:     int32(envInt("KAFKA_TOPIC_PARTITIONS", 12)),
		KafkaReplicationFactor:   int16(envInt("KAFKA_REPLICATION_FACTOR", 1)),
		KafkaSASLMechanism:       strings.ToUpper(env("KAFKA_SASL_MECHANISM", "")),
		KafkaUsername:            strings.TrimSpace(os.Getenv("KAFKA_USERNAME")),
		KafkaPassword:            os.Getenv("KAFKA_PASSWORD"),
		KafkaTLSEnabled:          envBool("KAFKA_TLS_ENABLED", false),
		KafkaTLSInsecure:         envBool("KAFKA_TLS_INSECURE_SKIP_VERIFY", false),
		KafkaCACertFile:          strings.TrimSpace(os.Getenv("KAFKA_CA_CERT_FILE")),
		KafkaClientCertFile:      strings.TrimSpace(os.Getenv("KAFKA_CLIENT_CERT_FILE")),
		KafkaClientKeyFile:       strings.TrimSpace(os.Getenv("KAFKA_CLIENT_KEY_FILE")),
		AdminToken:               os.Getenv("ADMIN_TOKEN"),
		TrackingToken:            os.Getenv("TRACKING_TOKEN"),
		HashSecret:               os.Getenv("HASH_SECRET"),
		MasterKey:                os.Getenv("MASTER_KEY"),
		RiskHTTPStatus:           envInt("RISK_HTTP_STATUS", 555),
		DefaultRouteKey:          env("DEFAULT_ROUTE_KEY", "default"),
		DefaultRouteName:         env("DEFAULT_ROUTE_NAME", "Default upstream"),
		DefaultUpstreamBaseURL:   strings.TrimSpace(os.Getenv("DEFAULT_UPSTREAM_BASE_URL")),
		DefaultUpstreamAuthMode:  strings.ToLower(env("DEFAULT_UPSTREAM_AUTH_MODE", "passthrough")),
		AuditQueueSize:           envInt("AUDIT_QUEUE_SIZE", 20000),
		AuditWorkers:             envInt("AUDIT_WORKERS", 8),
		AuditModelMaxConcurrency: envInt("AUDIT_MODEL_MAX_CONCURRENCY", 64),
		AuditModelEnabled:        envBool("AUDIT_MODEL_ENABLED", true),
		AuditModelURL:            env("AUDIT_MODEL_URL", "http://ollama:11434/v1/chat/completions"),
		AuditModelName:           env("AUDIT_MODEL_NAME", "qwen3.5:4b"),
		AuditModelAPIKey:         os.Getenv("AUDIT_MODEL_API_KEY"),
		AuditTimeout:             envDuration("AUDIT_TIMEOUT", 1800*time.Millisecond),
		AuditFailMode:            strings.ToLower(env("AUDIT_FAIL_MODE", "rules_only")),
		HotRetentionDays:         envInt("HOT_RETENTION_DAYS", 7),
		KafkaRetentionDays:       envInt("KAFKA_RETENTION_DAYS", 180),
		PayloadMode:              strings.ToLower(env("PAYLOAD_MODE", "redacted")),
		PayloadCaptureBytes:      envInt64("PAYLOAD_CAPTURE_BYTES", 64*1024),
		MaxRequestBodyBytes:      envInt64("MAX_REQUEST_BODY_BYTES", 8*1024*1024),
		MaxResponseBytes:         envInt64("MAX_RESPONSE_BYTES", 32*1024*1024),
		RateLimitEnabled:         envBool("RATE_LIMIT_ENABLED", true),
		RateLimitRPS:             envInt("RATE_LIMIT_RPS", 100),
		RateLimitBurst:           envInt("RATE_LIMIT_BURST", 200),
		AllowPrivateUpstreams:    envBool("ALLOW_PRIVATE_UPSTREAMS", false),
		TrustProxyHeaders:        envBool("TRUST_PROXY_HEADERS", false),
		NormalizeStatuses:        envIntCSV("NORMALIZE_UPSTREAM_STATUSES", []int{401, 403, 408, 409, 429, 500, 502, 503, 504}),
		NormalizePatterns:        envCSV("NORMALIZE_UPSTREAM_PATTERNS", []string{"model not found", "unsupported model", "model overloaded", "insufficient capacity", "upstream timeout", "provider error", "channel error", "模型不存在", "模型过载", "渠道错误"}),
		ShutdownTimeout:          envDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
		CleanupInterval:          envDuration("CLEANUP_INTERVAL", time.Hour),
		SettingsRefreshInterval:  envDuration("SETTINGS_REFRESH_INTERVAL", 30*time.Second),
		RequestTimeout:           envDuration("REQUEST_TIMEOUT", 10*time.Minute),
		ClientReadTimeout:        envDuration("CLIENT_READ_TIMEOUT", 60*time.Second),
		ClientIdleTimeout:        envDuration("CLIENT_IDLE_TIMEOUT", 120*time.Second),
		MaxHeaderBytes:           envInt("MAX_HEADER_BYTES", 1024*1024),
		UpstreamIdleConns:        envInt("UPSTREAM_MAX_IDLE_CONNS", 4096),
		UpstreamIdleConnsPerHost: envInt("UPSTREAM_MAX_IDLE_CONNS_PER_HOST", 1024),
		UpstreamMaxConnsPerHost:  envInt("UPSTREAM_MAX_CONNS_PER_HOST", 0),
	}

	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	if cfg.AppEnv == "development" {
		if cfg.AdminToken == "" {
			cfg.AdminToken = "dev-admin-token-change-me"
		}
		if cfg.TrackingToken == "" {
			cfg.TrackingToken = "dev-tracking-token-change-me"
		}
		if cfg.HashSecret == "" {
			cfg.HashSecret = "dev-hash-secret-change-me"
		}
		if cfg.MasterKey == "" {
			cfg.MasterKey = "dev-master-key-change-me-32chars"
		}
	} else {
		for name, value := range map[string]string{
			"ADMIN_TOKEN":    cfg.AdminToken,
			"TRACKING_TOKEN": cfg.TrackingToken,
			"HASH_SECRET":    cfg.HashSecret,
			"MASTER_KEY":     cfg.MasterKey,
		} {
			if len(value) < 24 {
				return cfg, fmt.Errorf("%s must be at least 24 characters in production", name)
			}
		}
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.AppEnv != "development" && c.AppEnv != "test" && c.AppEnv != "production" {
		return fmt.Errorf("APP_ENV must be development, test or production")
	}
	if strings.TrimSpace(c.ListenAddr) == "" {
		return fmt.Errorf("LISTEN_ADDR is required")
	}
	if err := validateHTTPURL(c.PublicBaseURL, "PUBLIC_BASE_URL"); err != nil {
		return err
	}
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.DatabaseMaxConns < 1 || c.DatabaseMaxConns > 10000 {
		return fmt.Errorf("DATABASE_MAX_CONNS must be 1..10000")
	}
	if c.DatabaseMinConns < 0 || c.DatabaseMinConns > c.DatabaseMaxConns {
		return fmt.Errorf("DATABASE_MIN_CONNS must be 0..DATABASE_MAX_CONNS")
	}
	if c.RedisRequired && strings.TrimSpace(c.RedisURL) == "" {
		return fmt.Errorf("REDIS_URL is required when REDIS_REQUIRED=true")
	}
	if c.RedisPoolSize < 1 || c.RedisPoolSize > 100000 {
		return fmt.Errorf("REDIS_POOL_SIZE must be 1..100000")
	}
	if c.RedisMinIdleConns < 0 || c.RedisMinIdleConns > c.RedisPoolSize {
		return fmt.Errorf("REDIS_MIN_IDLE_CONNS must be 0..REDIS_POOL_SIZE")
	}
	if c.KafkaRequired && !c.KafkaEnabled {
		return fmt.Errorf("KAFKA_REQUIRED=true requires KAFKA_ENABLED=true")
	}
	if c.KafkaEnabled {
		if len(c.KafkaBrokers) == 0 {
			return fmt.Errorf("KAFKA_BROKERS is required when Kafka is enabled")
		}
		for _, broker := range c.KafkaBrokers {
			if strings.TrimSpace(broker) == "" || !strings.Contains(broker, ":") {
				return fmt.Errorf("invalid Kafka broker %q", broker)
			}
		}
		if strings.TrimSpace(c.KafkaClientID) == "" || len(c.KafkaClientID) > 255 {
			return fmt.Errorf("KAFKA_CLIENT_ID must be 1..255 characters")
		}
		topics := []string{c.KafkaAuditTopic, c.KafkaTraceTopic, c.KafkaDeadLetterTopic}
		seen := map[string]struct{}{}
		for _, topic := range topics {
			topic = strings.TrimSpace(topic)
			if topic == "" || len(topic) > 249 {
				return fmt.Errorf("Kafka topic names must be 1..249 characters")
			}
			if _, ok := seen[topic]; ok {
				return fmt.Errorf("Kafka audit, trace and dead-letter topics must be distinct")
			}
			seen[topic] = struct{}{}
		}
		if c.KafkaTopicPartitions < 1 || c.KafkaTopicPartitions > 10000 {
			return fmt.Errorf("KAFKA_TOPIC_PARTITIONS must be 1..10000")
		}
		if c.KafkaReplicationFactor < 1 || c.KafkaReplicationFactor > 32767 {
			return fmt.Errorf("KAFKA_REPLICATION_FACTOR must be 1..32767")
		}
		if c.KafkaRetentionDays < -1 || c.KafkaRetentionDays > 36500 {
			return fmt.Errorf("KAFKA_RETENTION_DAYS must be -1..36500")
		}
		switch c.KafkaSASLMechanism {
		case "", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		default:
			return fmt.Errorf("KAFKA_SASL_MECHANISM must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512")
		}
		if c.KafkaSASLMechanism != "" && (c.KafkaUsername == "" || c.KafkaPassword == "") {
			return fmt.Errorf("KAFKA_USERNAME and KAFKA_PASSWORD are required when SASL is enabled")
		}
		if (c.KafkaClientCertFile == "") != (c.KafkaClientKeyFile == "") {
			return fmt.Errorf("KAFKA_CLIENT_CERT_FILE and KAFKA_CLIENT_KEY_FILE must be configured together")
		}
	}
	if c.RiskHTTPStatus < 400 || c.RiskHTTPStatus > 599 {
		return fmt.Errorf("RISK_HTTP_STATUS must be between 400 and 599")
	}
	if !routeKeyPattern.MatchString(c.DefaultRouteKey) {
		return fmt.Errorf("DEFAULT_ROUTE_KEY is invalid")
	}
	if len(c.DefaultRouteName) > 200 {
		return fmt.Errorf("DEFAULT_ROUTE_NAME must be at most 200 characters")
	}
	if c.DefaultUpstreamBaseURL != "" {
		if err := validateHTTPURL(c.DefaultUpstreamBaseURL, "DEFAULT_UPSTREAM_BASE_URL"); err != nil {
			return err
		}
	}
	switch c.DefaultUpstreamAuthMode {
	case "passthrough", "managed", "none":
	default:
		return fmt.Errorf("DEFAULT_UPSTREAM_AUTH_MODE must be passthrough, managed or none")
	}
	if c.HotRetentionDays < 1 || c.HotRetentionDays > 3650 {
		return fmt.Errorf("HOT_RETENTION_DAYS must be 1..3650")
	}
	if c.AuditWorkers < 1 || c.AuditWorkers > 1024 || c.AuditQueueSize < 100 || c.AuditQueueSize > 10000000 {
		return fmt.Errorf("AUDIT_WORKERS must be 1..1024 and AUDIT_QUEUE_SIZE 100..10000000")
	}
	if c.AuditModelMaxConcurrency < 1 || c.AuditModelMaxConcurrency > 4096 {
		return fmt.Errorf("AUDIT_MODEL_MAX_CONCURRENCY must be 1..4096")
	}
	if c.AuditTimeout < 100*time.Millisecond || c.AuditTimeout > 60*time.Second {
		return fmt.Errorf("AUDIT_TIMEOUT must be between 100ms and 60s")
	}
	switch c.AuditFailMode {
	case "rules_only", "block", "allow":
	default:
		return fmt.Errorf("AUDIT_FAIL_MODE must be rules_only, block or allow")
	}
	if c.AuditModelEnabled {
		if strings.TrimSpace(c.AuditModelName) == "" {
			return fmt.Errorf("AUDIT_MODEL_NAME is required when model audit is enabled")
		}
		if err := validateHTTPURL(c.AuditModelURL, "AUDIT_MODEL_URL"); err != nil {
			return err
		}
	}
	switch c.PayloadMode {
	case "none", "redacted", "encrypted":
	default:
		return fmt.Errorf("PAYLOAD_MODE must be none, redacted or encrypted")
	}
	if c.PayloadCaptureBytes < 0 || c.PayloadCaptureBytes > 16*1024*1024 {
		return fmt.Errorf("PAYLOAD_CAPTURE_BYTES must be 0..16777216")
	}
	if c.MaxRequestBodyBytes < 1024 || c.MaxRequestBodyBytes > 1<<30 {
		return fmt.Errorf("MAX_REQUEST_BODY_BYTES must be 1024..1073741824")
	}
	if c.MaxResponseBytes < 1024 || c.MaxResponseBytes > 2<<30 {
		return fmt.Errorf("MAX_RESPONSE_BYTES must be 1024..2147483648")
	}
	if c.RateLimitEnabled && (c.RateLimitRPS < 1 || c.RateLimitRPS > 10000000 || c.RateLimitBurst < 1 || c.RateLimitBurst > 100000000) {
		return fmt.Errorf("RATE_LIMIT_RPS must be 1..10000000 and RATE_LIMIT_BURST 1..100000000")
	}
	if len(c.NormalizeStatuses) > 100 {
		return fmt.Errorf("NORMALIZE_UPSTREAM_STATUSES supports at most 100 entries")
	}
	for _, status := range c.NormalizeStatuses {
		if status < 100 || status > 599 {
			return fmt.Errorf("NORMALIZE_UPSTREAM_STATUSES contains invalid status %d", status)
		}
	}
	if len(c.NormalizePatterns) > 200 {
		return fmt.Errorf("NORMALIZE_UPSTREAM_PATTERNS supports at most 200 entries")
	}
	for _, pattern := range c.NormalizePatterns {
		if len(pattern) > 512 {
			return fmt.Errorf("NORMALIZE_UPSTREAM_PATTERNS entry exceeds 512 bytes")
		}
	}
	if c.ShutdownTimeout < time.Second || c.ShutdownTimeout > 10*time.Minute {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be between 1s and 10m")
	}
	if c.CleanupInterval < time.Minute || c.CleanupInterval > 24*time.Hour {
		return fmt.Errorf("CLEANUP_INTERVAL must be between 1m and 24h")
	}
	if c.SettingsRefreshInterval < 5*time.Second || c.SettingsRefreshInterval > time.Hour {
		return fmt.Errorf("SETTINGS_REFRESH_INTERVAL must be between 5s and 1h")
	}
	if c.RequestTimeout < time.Second || c.RequestTimeout > 24*time.Hour {
		return fmt.Errorf("REQUEST_TIMEOUT must be between 1s and 24h")
	}
	if c.ClientReadTimeout < time.Second || c.ClientReadTimeout > time.Hour {
		return fmt.Errorf("CLIENT_READ_TIMEOUT must be between 1s and 1h")
	}
	if c.ClientIdleTimeout < time.Second || c.ClientIdleTimeout > time.Hour {
		return fmt.Errorf("CLIENT_IDLE_TIMEOUT must be between 1s and 1h")
	}
	if c.MaxHeaderBytes < 8*1024 || c.MaxHeaderBytes > 16*1024*1024 {
		return fmt.Errorf("MAX_HEADER_BYTES must be 8192..16777216")
	}
	if c.UpstreamIdleConns < 1 || c.UpstreamIdleConns > 1000000 {
		return fmt.Errorf("UPSTREAM_MAX_IDLE_CONNS must be 1..1000000")
	}
	if c.UpstreamIdleConnsPerHost < 1 || c.UpstreamIdleConnsPerHost > c.UpstreamIdleConns {
		return fmt.Errorf("UPSTREAM_MAX_IDLE_CONNS_PER_HOST must be 1..UPSTREAM_MAX_IDLE_CONNS")
	}
	if c.UpstreamMaxConnsPerHost < 0 || c.UpstreamMaxConnsPerHost > 1000000 {
		return fmt.Errorf("UPSTREAM_MAX_CONNS_PER_HOST must be 0..1000000")
	}
	return nil
}

func (c Config) KafkaTLSMinVersion() uint16 { return tls.VersionTLS12 }

func validateHTTPURL(raw, name string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("%s must be an absolute http(s) URL", name)
	}
	if u.User != nil {
		return fmt.Errorf("%s must not contain credentials", name)
	}
	return nil
}

func validateRawEnvironment() error {
	boolKeys := []string{"REDIS_REQUIRED", "KAFKA_ENABLED", "KAFKA_REQUIRED", "KAFKA_AUTO_CREATE_TOPICS", "KAFKA_TLS_ENABLED", "KAFKA_TLS_INSECURE_SKIP_VERIFY", "AUDIT_MODEL_ENABLED", "RATE_LIMIT_ENABLED", "ALLOW_PRIVATE_UPSTREAMS", "TRUST_PROXY_HEADERS"}
	for _, key := range boolKeys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			if _, err := strconv.ParseBool(value); err != nil {
				return fmt.Errorf("%s must be a boolean: %w", key, err)
			}
		}
	}
	intKeys := []string{"DATABASE_MAX_CONNS", "DATABASE_MIN_CONNS", "REDIS_POOL_SIZE", "REDIS_MIN_IDLE_CONNS", "KAFKA_TOPIC_PARTITIONS", "KAFKA_REPLICATION_FACTOR", "RISK_HTTP_STATUS", "AUDIT_QUEUE_SIZE", "AUDIT_WORKERS", "AUDIT_MODEL_MAX_CONCURRENCY", "HOT_RETENTION_DAYS", "KAFKA_RETENTION_DAYS", "RATE_LIMIT_RPS", "RATE_LIMIT_BURST", "MAX_HEADER_BYTES", "UPSTREAM_MAX_IDLE_CONNS", "UPSTREAM_MAX_IDLE_CONNS_PER_HOST", "UPSTREAM_MAX_CONNS_PER_HOST"}
	for _, key := range intKeys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			if _, err := strconv.Atoi(value); err != nil {
				return fmt.Errorf("%s must be an integer: %w", key, err)
			}
		}
	}
	int64Keys := []string{"PAYLOAD_CAPTURE_BYTES", "MAX_REQUEST_BODY_BYTES", "MAX_RESPONSE_BYTES"}
	for _, key := range int64Keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			if _, err := strconv.ParseInt(value, 10, 64); err != nil {
				return fmt.Errorf("%s must be an integer: %w", key, err)
			}
		}
	}
	durationKeys := []string{"AUDIT_TIMEOUT", "SHUTDOWN_TIMEOUT", "CLEANUP_INTERVAL", "SETTINGS_REFRESH_INTERVAL", "REQUEST_TIMEOUT", "CLIENT_READ_TIMEOUT", "CLIENT_IDLE_TIMEOUT"}
	for _, key := range durationKeys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			if _, err := time.ParseDuration(value); err != nil {
				return fmt.Errorf("%s must be a Go duration: %w", key, err)
			}
		}
	}
	if value := strings.TrimSpace(os.Getenv("NORMALIZE_UPSTREAM_STATUSES")); value != "" {
		for _, part := range strings.Split(value, ",") {
			if _, err := strconv.Atoi(strings.TrimSpace(part)); err != nil {
				return fmt.Errorf("NORMALIZE_UPSTREAM_STATUSES contains non-integer value %q", part)
			}
		}
	}
	return nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, _ := strconv.ParseBool(value)
	return parsed
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, _ := strconv.Atoi(value)
	return parsed
}

func envInt64(key string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, _ := time.ParseDuration(value)
	return parsed
}

func envCSV(key string, fallback []string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return append([]string(nil), fallback...)
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		if item := strings.TrimSpace(part); item != "" {
			if _, exists := seen[item]; exists {
				continue
			}
			seen[item] = struct{}{}
			out = append(out, item)
		}
	}
	if len(out) == 0 {
		return append([]string(nil), fallback...)
	}
	return out
}

func envIntCSV(key string, fallback []int) []int {
	parts := envCSV(key, nil)
	if len(parts) == 0 {
		return append([]int(nil), fallback...)
	}
	out := make([]int, 0, len(parts))
	seen := map[int]struct{}{}
	for _, part := range parts {
		value, _ := strconv.Atoi(part)
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
