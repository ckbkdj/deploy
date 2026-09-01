package core

import (
	"encoding/json"
	"time"
)

type Decision string

const (
	DecisionAllow  Decision = "allow"
	DecisionReview Decision = "review"
	DecisionBlock  Decision = "block"
)

type AuditResult struct {
	Decision   Decision `json:"decision"`
	Score      float64  `json:"score"`
	Categories []string `json:"categories,omitempty"`
	Reason     string   `json:"reason"`
	Source     string   `json:"source"`
	RuleHits   []string `json:"rule_hits,omitempty"`
	LatencyMS  int64    `json:"latency_ms"`
	Cached     bool     `json:"cached"`
}

type Route struct {
	ID                 int64           `json:"id"`
	Key                string          `json:"key"`
	Name               string          `json:"name"`
	BaseURL            string          `json:"base_url"`
	AuthMode           string          `json:"auth_mode"`
	ManagedSecret      string          `json:"managed_secret,omitempty"`
	ManagedSecretSet   bool            `json:"managed_secret_set"`
	Headers            json.RawMessage `json:"headers"`
	ModelMap           json.RawMessage `json:"model_map"`
	Enabled            bool            `json:"enabled"`
	TimeoutMS          int             `json:"timeout_ms"`
	NormalizeErrors    bool            `json:"normalize_errors"`
	NormalizeStatuses  []int           `json:"normalize_statuses"`
	NormalizePatterns  []string        `json:"normalize_patterns"`
	AllowPrivateTarget bool            `json:"allow_private_target"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

type RuntimeSettings struct {
	HotRetentionDays    int       `json:"hot_retention_days"`
	KafkaRetentionDays  int       `json:"kafka_retention_days"`
	PayloadMode         string    `json:"payload_mode"`
	ModelAuditEnabled   bool      `json:"model_audit_enabled"`
	AuditModelURL       string    `json:"audit_model_url"`
	AuditModelName      string    `json:"audit_model_name"`
	AuditModelAPIKey    string    `json:"audit_model_api_key,omitempty"`
	AuditModelKeySet    bool      `json:"audit_model_api_key_set"`
	AuditTimeoutMS      int       `json:"audit_timeout_ms"`
	BlockThreshold      float64   `json:"block_threshold"`
	ReviewThreshold     float64   `json:"review_threshold"`
	AuditFailMode       string    `json:"audit_fail_mode"`
	RateLimitEnabled    bool      `json:"rate_limit_enabled"`
	RateLimitRPS        int       `json:"rate_limit_rps"`
	RateLimitBurst      int       `json:"rate_limit_burst"`
	NormalizeStatuses   []int     `json:"normalize_statuses"`
	NormalizePatterns   []string  `json:"normalize_patterns"`
	MaxRequestBodyBytes int64     `json:"max_request_body_bytes"`
	MaxResponseBytes    int64     `json:"max_response_bytes"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type RequestTrace struct {
	RequestID         string          `json:"request_id"`
	TraceID           string          `json:"trace_id"`
	RouteKey          string          `json:"route_key"`
	Method            string          `json:"method"`
	Path              string          `json:"path"`
	Model             string          `json:"model"`
	UserHash          string          `json:"user_hash"`
	TokenHash         string          `json:"token_hash"`
	ClientIPHash      string          `json:"client_ip_hash"`
	AuditDecision     Decision        `json:"audit_decision"`
	AuditScore        float64         `json:"audit_score"`
	AuditCategories   []string        `json:"audit_categories"`
	AuditReason       string          `json:"audit_reason"`
	HTTPStatus        int             `json:"http_status"`
	UpstreamStatus    int             `json:"upstream_status"`
	NormalizedTo555   bool            `json:"normalized_to_555"`
	ErrorClass        string          `json:"error_class"`
	LatencyMS         int64           `json:"latency_ms"`
	UpstreamLatencyMS int64           `json:"upstream_latency_ms"`
	PromptTokens      int64           `json:"prompt_tokens"`
	CompletionTokens  int64           `json:"completion_tokens"`
	TotalTokens       int64           `json:"total_tokens"`
	RequestPayload    json.RawMessage `json:"request_payload,omitempty"`
	ResponsePayload   json.RawMessage `json:"response_payload,omitempty"`
	Metadata          json.RawMessage `json:"metadata,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
}

type TrackEvent struct {
	RequestID        string          `json:"request_id"`
	TraceID          string          `json:"trace_id"`
	Event            string          `json:"event"`
	UserID           string          `json:"user_id,omitempty"`
	TokenID          string          `json:"token_id,omitempty"`
	ChannelID        string          `json:"channel_id,omitempty"`
	RouteKey         string          `json:"route_key,omitempty"`
	Model            string          `json:"model,omitempty"`
	HTTPStatus       int             `json:"http_status,omitempty"`
	UpstreamStatus   int             `json:"upstream_status,omitempty"`
	LatencyMS        int64           `json:"latency_ms,omitempty"`
	PromptTokens     int64           `json:"prompt_tokens,omitempty"`
	CompletionTokens int64           `json:"completion_tokens,omitempty"`
	TotalTokens      int64           `json:"total_tokens,omitempty"`
	ErrorCode        string          `json:"error_code,omitempty"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
	OccurredAt       time.Time       `json:"occurred_at,omitempty"`
}

type TrackingKafkaEvent struct {
	RequestID        string          `json:"request_id"`
	TraceID          string          `json:"trace_id"`
	Event            string          `json:"event"`
	UserHash         string          `json:"user_hash,omitempty"`
	TokenHash        string          `json:"token_hash,omitempty"`
	ChannelID        string          `json:"channel_id,omitempty"`
	RouteKey         string          `json:"route_key,omitempty"`
	Model            string          `json:"model,omitempty"`
	HTTPStatus       int             `json:"http_status,omitempty"`
	UpstreamStatus   int             `json:"upstream_status,omitempty"`
	LatencyMS        int64           `json:"latency_ms,omitempty"`
	PromptTokens     int64           `json:"prompt_tokens,omitempty"`
	CompletionTokens int64           `json:"completion_tokens,omitempty"`
	TotalTokens      int64           `json:"total_tokens,omitempty"`
	ErrorCode        string          `json:"error_code,omitempty"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
	OccurredAt       time.Time       `json:"occurred_at"`
}

type DashboardStats struct {
	Requests24H       int64          `json:"requests_24h"`
	Blocked24H        int64          `json:"blocked_24h"`
	Normalized55524H  int64          `json:"normalized_555_24h"`
	Errors24H         int64          `json:"errors_24h"`
	AverageLatencyMS  float64        `json:"average_latency_ms"`
	P95LatencyMS      float64        `json:"p95_latency_ms"`
	KafkaPublished24H int64          `json:"kafka_published_24h"`
	Deadlettered24H   int64          `json:"deadlettered_24h"`
	OutboxPending     int64          `json:"outbox_pending"`
	TopModels         []NameCount    `json:"top_models"`
	TopRoutes         []NameCount    `json:"top_routes"`
	Recent            []RequestTrace `json:"recent"`
	Storage           StorageHealth  `json:"storage"`
}

type NameCount struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

type StorageHealth struct {
	Postgres bool   `json:"postgres"`
	Redis    bool   `json:"redis"`
	Kafka    bool   `json:"kafka"`
	AsOf     string `json:"as_of"`
}

type OutboxEvent struct {
	ID        int64     `json:"id"`
	EventID   string    `json:"event_id"`
	Topic     string    `json:"topic"`
	Key       string    `json:"key"`
	EventType string    `json:"event_type"`
	Payload   []byte    `json:"payload"`
	Attempts  int       `json:"attempts"`
	CreatedAt time.Time `json:"created_at"`
}

type EventEnvelope struct {
	Version   string          `json:"version"`
	Type      string          `json:"type"`
	EventID   string          `json:"event_id"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}
