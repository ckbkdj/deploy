package storage

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ckbkdj/newapi-risk-gateway/internal/core"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

var (
	routeKeyPattern   = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)
	headerNamePattern = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
)

type Postgres struct {
	pool       *pgxpool.Pool
	cipher     *core.Cipher
	hashSecret string
}

func NewPostgres(ctx context.Context, databaseURL string, maxConns, minConns int32, cipher *core.Cipher, hashSecret string) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = minConns
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	cfg.ConnConfig.RuntimeParams["application_name"] = "newapi-risk-gateway"

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Postgres{pool: pool, cipher: cipher, hashSecret: hashSecret}, nil
}

func (p *Postgres) Close()                         { p.pool.Close() }
func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

func (p *Postgres) Migrate(ctx context.Context, retentionDays int) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()
	const migrationLock int64 = 733491555001
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLock); err != nil {
		return fmt.Errorf("lock database migration: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLock) }()
	if _, err := conn.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply database schema: %w", err)
	}
	return p.ensurePartitionsOn(ctx, conn, retentionDays)
}

func (p *Postgres) EnsurePartitions(ctx context.Context, retentionDays int) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	const partitionLock int64 = 733491555001
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, partitionLock); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, partitionLock) }()
	return p.ensurePartitionsOn(ctx, conn, retentionDays)
}

type sqlExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func (p *Postgres) ensurePartitionsOn(ctx context.Context, execer sqlExecer, retentionDays int) error {
	if retentionDays < 1 {
		retentionDays = 1
	}
	// Only current/future partitions are needed for new writes. Existing daily
	// partitions remain until the retention job drops them, so increasing the
	// retention window does not create thousands of empty historical tables.
	today := time.Now().UTC().Truncate(24 * time.Hour)
	start := today.AddDate(0, 0, -2)
	end := today.AddDate(0, 0, 8)
	for day := start; day.Before(end); day = day.AddDate(0, 0, 1) {
		name := "request_traces_" + day.Format("20060102")
		next := day.AddDate(0, 0, 1)
		// PostgreSQL partition bounds are DDL constants and cannot use bind parameters.
		// The values below are generated internally from time.Time, never user input.
		query := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s PARTITION OF request_traces FOR VALUES FROM ('%s') TO ('%s')",
			pgx.Identifier{name}.Sanitize(), day.Format("2006-01-02"), next.Format("2006-01-02"),
		)
		if _, err := execer.Exec(ctx, query); err != nil {
			return fmt.Errorf("ensure partition %s: %w", name, err)
		}
	}
	return nil
}

func (p *Postgres) DropExpiredPartitions(ctx context.Context, retentionDays int) (int, error) {
	cutoff := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -retentionDays)
	rows, err := p.pool.Query(ctx, `
SELECT child.relname
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'request_traces' AND child.relname ~ '^request_traces_[0-9]{8}$'`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return 0, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	count := 0
	for _, name := range names {
		datePart := strings.TrimPrefix(name, "request_traces_")
		day, err := time.Parse("20060102", datePart)
		if err != nil || !day.Before(cutoff) {
			continue
		}
		if _, err := p.pool.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
			return count, err
		}
		count++
	}
	_, err = p.pool.Exec(ctx, `DELETE FROM tracking_records WHERE updated_at < now() - make_interval(days => $1)`, retentionDays)
	if err != nil {
		return count, err
	}
	return count, nil
}

func (p *Postgres) SeedDefaultRoute(ctx context.Context, key, name, baseURL, authMode string, statuses []int, patterns []string, allowPrivate bool) error {
	if strings.TrimSpace(baseURL) == "" {
		return nil
	}
	route := core.Route{
		Key: key, Name: name, BaseURL: baseURL, AuthMode: authMode,
		Headers: json.RawMessage(`{}`), ModelMap: json.RawMessage(`{}`), Enabled: true,
		TimeoutMS: 600000, NormalizeErrors: true, NormalizeStatuses: statuses,
		NormalizePatterns: patterns, AllowPrivateTarget: allowPrivate,
	}
	_, err := p.UpsertRoute(ctx, route)
	return err
}

func (p *Postgres) UpsertRoute(ctx context.Context, route core.Route) (core.Route, error) {
	route.Key = strings.TrimSpace(route.Key)
	if !routeKeyPattern.MatchString(route.Key) {
		return route, fmt.Errorf("route key must match %s", routeKeyPattern.String())
	}
	route.Name = strings.TrimSpace(route.Name)
	if route.Name == "" {
		route.Name = route.Key
	}
	if len(route.Name) > 160 {
		return route, fmt.Errorf("route name must be at most 160 characters")
	}
	route.BaseURL = strings.TrimRight(strings.TrimSpace(route.BaseURL), "/")
	if len(route.BaseURL) > 2048 {
		return route, fmt.Errorf("base_url must be at most 2048 characters")
	}
	if err := validateBaseURL(route.BaseURL); err != nil {
		return route, err
	}
	route.AuthMode = strings.ToLower(strings.TrimSpace(route.AuthMode))
	if route.AuthMode == "" {
		route.AuthMode = "passthrough"
	}
	if route.AuthMode != "passthrough" && route.AuthMode != "managed" && route.AuthMode != "none" {
		return route, fmt.Errorf("auth_mode must be passthrough, managed or none")
	}
	if route.TimeoutMS <= 0 {
		route.TimeoutMS = 600000
	}
	if route.TimeoutMS < 100 || route.TimeoutMS > 24*60*60*1000 {
		return route, fmt.Errorf("timeout_ms must be 100..86400000")
	}

	headers, err := validateStringMap(route.Headers, "headers", 64)
	if err != nil {
		return route, err
	}
	for key, value := range headers {
		if !headerNamePattern.MatchString(key) {
			return route, fmt.Errorf("headers contains invalid HTTP header name %q", key)
		}
		switch strings.ToLower(key) {
		case "authorization", "host", "content-length", "connection", "proxy-connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
			return route, fmt.Errorf("headers may not override protected header %q", key)
		}
		if len(value) > 8192 || strings.ContainsAny(value, "\r\n") {
			return route, fmt.Errorf("header %q has an invalid or oversized value", key)
		}
	}
	modelMap, err := validateStringMap(route.ModelMap, "model_map", 1000)
	if err != nil {
		return route, err
	}
	for from, to := range modelMap {
		if strings.TrimSpace(from) == "" || strings.TrimSpace(to) == "" || len(from) > 256 || len(to) > 256 {
			return route, fmt.Errorf("model_map keys and values must be non-empty and at most 256 characters")
		}
	}
	route.Headers, _ = json.Marshal(headers)
	route.ModelMap, _ = json.Marshal(modelMap)

	if len(route.NormalizeStatuses) > 100 {
		return route, fmt.Errorf("normalize_statuses supports at most 100 entries")
	}
	seenStatuses := map[int]struct{}{}
	statuses := make([]int, 0, len(route.NormalizeStatuses))
	for _, status := range route.NormalizeStatuses {
		if status < 100 || status > 599 {
			return route, fmt.Errorf("normalize_statuses contains invalid status %d", status)
		}
		if _, exists := seenStatuses[status]; exists {
			continue
		}
		seenStatuses[status] = struct{}{}
		statuses = append(statuses, status)
	}
	route.NormalizeStatuses = statuses
	if len(route.NormalizePatterns) > 200 {
		return route, fmt.Errorf("normalize_patterns supports at most 200 entries")
	}
	seenPatterns := map[string]struct{}{}
	patterns := make([]string, 0, len(route.NormalizePatterns))
	for _, pattern := range route.NormalizePatterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if len(pattern) > 512 {
			return route, fmt.Errorf("normalize pattern exceeds 512 bytes")
		}
		if _, exists := seenPatterns[pattern]; exists {
			continue
		}
		seenPatterns[pattern] = struct{}{}
		patterns = append(patterns, pattern)
	}
	route.NormalizePatterns = patterns

	secretEnc := ""
	if route.ManagedSecret != "" {
		secretEnc, err = p.cipher.Encrypt([]byte(route.ManagedSecret))
		if err != nil {
			return route, fmt.Errorf("encrypt managed secret: %w", err)
		}
	}

	row := p.pool.QueryRow(ctx, `
INSERT INTO routes (route_key, name, base_url, auth_mode, managed_secret_enc, headers, model_map, enabled, timeout_ms,
                    normalize_errors, normalize_statuses, normalize_patterns, allow_private_target)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
ON CONFLICT (route_key) DO UPDATE SET
    name = EXCLUDED.name,
    base_url = EXCLUDED.base_url,
    auth_mode = EXCLUDED.auth_mode,
    managed_secret_enc = CASE WHEN EXCLUDED.managed_secret_enc = '' THEN routes.managed_secret_enc ELSE EXCLUDED.managed_secret_enc END,
    headers = EXCLUDED.headers,
    model_map = EXCLUDED.model_map,
    enabled = EXCLUDED.enabled,
    timeout_ms = EXCLUDED.timeout_ms,
    normalize_errors = EXCLUDED.normalize_errors,
    normalize_statuses = EXCLUDED.normalize_statuses,
    normalize_patterns = EXCLUDED.normalize_patterns,
    allow_private_target = EXCLUDED.allow_private_target,
    updated_at = now()
RETURNING id, route_key, name, base_url, auth_mode, managed_secret_enc <> '', headers, model_map, enabled, timeout_ms,
          normalize_errors, normalize_statuses, normalize_patterns, allow_private_target, created_at, updated_at`,
		route.Key, route.Name, route.BaseURL, route.AuthMode, secretEnc, route.Headers, route.ModelMap,
		route.Enabled, route.TimeoutMS, route.NormalizeErrors, route.NormalizeStatuses, route.NormalizePatterns, route.AllowPrivateTarget)
	return scanRoute(row)
}

func validateStringMap(raw json.RawMessage, field string, maxEntries int) (map[string]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]string{}, nil
	}
	var out map[string]string
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return nil, fmt.Errorf("%s must be a JSON object with string values", field)
	}
	if len(out) > maxEntries {
		return nil, fmt.Errorf("%s supports at most %d entries", field, maxEntries)
	}
	return out, nil
}

func (p *Postgres) GetRoute(ctx context.Context, key string) (core.Route, error) {
	row := p.pool.QueryRow(ctx, `
SELECT id, route_key, name, base_url, auth_mode, managed_secret_enc, managed_secret_enc <> '', headers, model_map,
       enabled, timeout_ms, normalize_errors, normalize_statuses, normalize_patterns, allow_private_target, created_at, updated_at
FROM routes WHERE route_key = $1`, key)
	var route core.Route
	var secretEnc string
	if err := row.Scan(&route.ID, &route.Key, &route.Name, &route.BaseURL, &route.AuthMode, &secretEnc, &route.ManagedSecretSet,
		&route.Headers, &route.ModelMap, &route.Enabled, &route.TimeoutMS, &route.NormalizeErrors,
		&route.NormalizeStatuses, &route.NormalizePatterns, &route.AllowPrivateTarget, &route.CreatedAt, &route.UpdatedAt); err != nil {
		return route, err
	}
	if secretEnc != "" {
		plain, err := p.cipher.Decrypt(secretEnc)
		if err != nil {
			return route, fmt.Errorf("decrypt route secret: %w", err)
		}
		route.ManagedSecret = string(plain)
	}
	return route, nil
}

func (p *Postgres) ListRoutes(ctx context.Context) ([]core.Route, error) {
	rows, err := p.pool.Query(ctx, `
SELECT id, route_key, name, base_url, auth_mode, managed_secret_enc <> '', headers, model_map, enabled, timeout_ms,
       normalize_errors, normalize_statuses, normalize_patterns, allow_private_target, created_at, updated_at
FROM routes ORDER BY route_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.Route
	for rows.Next() {
		var route core.Route
		if err := rows.Scan(&route.ID, &route.Key, &route.Name, &route.BaseURL, &route.AuthMode, &route.ManagedSecretSet,
			&route.Headers, &route.ModelMap, &route.Enabled, &route.TimeoutMS, &route.NormalizeErrors,
			&route.NormalizeStatuses, &route.NormalizePatterns, &route.AllowPrivateTarget, &route.CreatedAt, &route.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, route)
	}
	return out, rows.Err()
}

func (p *Postgres) DeleteRoute(ctx context.Context, key string) error {
	result, err := p.pool.Exec(ctx, `DELETE FROM routes WHERE route_key = $1`, key)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func scanRoute(row pgx.Row) (core.Route, error) {
	var route core.Route
	err := row.Scan(&route.ID, &route.Key, &route.Name, &route.BaseURL, &route.AuthMode, &route.ManagedSecretSet,
		&route.Headers, &route.ModelMap, &route.Enabled, &route.TimeoutMS, &route.NormalizeErrors,
		&route.NormalizeStatuses, &route.NormalizePatterns, &route.AllowPrivateTarget, &route.CreatedAt, &route.UpdatedAt)
	return route, err
}

func (p *Postgres) LoadSettings(ctx context.Context, defaults core.RuntimeSettings) (core.RuntimeSettings, error) {
	var raw []byte
	var apiKeyEnc string
	err := p.pool.QueryRow(ctx, `SELECT config, audit_model_api_key_enc FROM app_settings WHERE id = 1`).Scan(&raw, &apiKeyEnc)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, err := p.SaveSettings(ctx, defaults); err != nil {
			return defaults, err
		}
		return defaults, nil
	}
	if err != nil {
		return defaults, err
	}
	settings := defaults
	if err := json.Unmarshal(raw, &settings); err != nil {
		return defaults, fmt.Errorf("decode settings: %w", err)
	}
	if apiKeyEnc != "" {
		plain, err := p.cipher.Decrypt(apiKeyEnc)
		if err != nil {
			return defaults, fmt.Errorf("decrypt audit model api key: %w", err)
		}
		settings.AuditModelAPIKey = string(plain)
		settings.AuditModelKeySet = true
	}
	return settings, nil
}

func (p *Postgres) SaveSettings(ctx context.Context, settings core.RuntimeSettings) (core.RuntimeSettings, error) {
	if settings.HotRetentionDays < 1 || settings.HotRetentionDays > 3650 {
		return settings, fmt.Errorf("hot_retention_days must be 1..3650")
	}
	if settings.KafkaRetentionDays < -1 || settings.KafkaRetentionDays > 36500 {
		return settings, fmt.Errorf("kafka_retention_days must be -1..36500")
	}
	if settings.PayloadMode != "none" && settings.PayloadMode != "redacted" && settings.PayloadMode != "encrypted" {
		return settings, fmt.Errorf("invalid payload_mode")
	}
	if settings.AuditTimeoutMS < 100 || settings.AuditTimeoutMS > 60000 {
		return settings, fmt.Errorf("audit_timeout_ms must be 100..60000")
	}
	if settings.BlockThreshold < 0 || settings.BlockThreshold > 1 || settings.ReviewThreshold < 0 || settings.ReviewThreshold > 1 {
		return settings, fmt.Errorf("thresholds must be 0..1")
	}
	if settings.AuditFailMode != "rules_only" && settings.AuditFailMode != "block" && settings.AuditFailMode != "allow" {
		return settings, fmt.Errorf("invalid audit_fail_mode")
	}
	if settings.ReviewThreshold > settings.BlockThreshold {
		return settings, fmt.Errorf("review_threshold must be <= block_threshold")
	}
	if settings.ModelAuditEnabled {
		if strings.TrimSpace(settings.AuditModelName) == "" {
			return settings, fmt.Errorf("audit_model_name is required when model audit is enabled")
		}
		if err := validateBaseURL(settings.AuditModelURL); err != nil {
			return settings, fmt.Errorf("invalid audit_model_url: %w", err)
		}
	}
	if settings.RateLimitEnabled && (settings.RateLimitRPS < 1 || settings.RateLimitBurst < 1) {
		return settings, fmt.Errorf("rate_limit_rps and rate_limit_burst must be >= 1")
	}
	if settings.MaxRequestBodyBytes < 1024 || settings.MaxRequestBodyBytes > 1<<30 {
		return settings, fmt.Errorf("max_request_body_bytes must be 1024..1073741824")
	}
	if settings.MaxResponseBytes < 1024 || settings.MaxResponseBytes > 2<<30 {
		return settings, fmt.Errorf("max_response_bytes must be 1024..2147483648")
	}
	for _, status := range settings.NormalizeStatuses {
		if status < 100 || status > 599 {
			return settings, fmt.Errorf("normalize_statuses contains invalid status %d", status)
		}
	}
	if len(settings.NormalizePatterns) > 200 {
		return settings, fmt.Errorf("normalize_patterns supports at most 200 entries")
	}
	for _, pattern := range settings.NormalizePatterns {
		if len(pattern) > 512 {
			return settings, fmt.Errorf("normalize pattern exceeds 512 bytes")
		}
	}
	settings.UpdatedAt = time.Now().UTC()

	apiKeyEnc := ""
	if settings.AuditModelAPIKey != "" {
		var err error
		apiKeyEnc, err = p.cipher.Encrypt([]byte(settings.AuditModelAPIKey))
		if err != nil {
			return settings, err
		}
	}
	settings.AuditModelAPIKey = ""
	raw, err := json.Marshal(settings)
	if err != nil {
		return settings, err
	}
	_, err = p.pool.Exec(ctx, `
INSERT INTO app_settings (id, config, audit_model_api_key_enc, updated_at)
VALUES (1, $1, $2, now())
ON CONFLICT (id) DO UPDATE SET
  config = EXCLUDED.config,
  audit_model_api_key_enc = CASE WHEN EXCLUDED.audit_model_api_key_enc = '' THEN app_settings.audit_model_api_key_enc ELSE EXCLUDED.audit_model_api_key_enc END,
  updated_at = now()`, raw, apiKeyEnc)
	if err != nil {
		return settings, err
	}
	settings.AuditModelKeySet = apiKeyEnc != "" || settings.AuditModelKeySet
	return settings, nil
}

func (p *Postgres) InsertTrace(ctx context.Context, trace core.RequestTrace) error {
	if trace.CreatedAt.IsZero() {
		trace.CreatedAt = time.Now().UTC()
	}
	_, err := p.pool.Exec(ctx, `
INSERT INTO request_traces (
 request_id, trace_id, route_key, method, path, model, user_hash, token_hash, client_ip_hash,
 audit_decision, audit_score, audit_categories, audit_reason, http_status, upstream_status,
 normalized_to_555, error_class, latency_ms, upstream_latency_ms, prompt_tokens, completion_tokens,
 total_tokens, request_payload, response_payload, metadata, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)`,
		trace.RequestID, trace.TraceID, trace.RouteKey, trace.Method, trace.Path, trace.Model, trace.UserHash,
		trace.TokenHash, trace.ClientIPHash, string(trace.AuditDecision), trace.AuditScore, trace.AuditCategories,
		trace.AuditReason, trace.HTTPStatus, trace.UpstreamStatus, trace.NormalizedTo555, trace.ErrorClass,
		trace.LatencyMS, trace.UpstreamLatencyMS, trace.PromptTokens, trace.CompletionTokens, trace.TotalTokens,
		nullJSON(trace.RequestPayload), nullJSON(trace.ResponsePayload), nullJSON(trace.Metadata), trace.CreatedAt)
	return err
}

func (p *Postgres) UpsertTracking(ctx context.Context, event core.TrackEvent) error {
	if event.RequestID == "" {
		return fmt.Errorf("request_id is required")
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	userHash := core.StableHash(p.hashSecret, event.UserID)
	tokenHash := core.StableHash(p.hashSecret, event.TokenID)
	_, err := p.pool.Exec(ctx, `
INSERT INTO tracking_records (
 request_id, trace_id, event, user_hash, token_hash, channel_id, route_key, model, http_status,
 upstream_status, latency_ms, prompt_tokens, completion_tokens, total_tokens, error_code, metadata, occurred_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
ON CONFLICT (request_id) DO UPDATE SET
 trace_id = CASE WHEN EXCLUDED.trace_id = '' THEN tracking_records.trace_id ELSE EXCLUDED.trace_id END,
 event = EXCLUDED.event,
 user_hash = CASE WHEN EXCLUDED.user_hash = '' THEN tracking_records.user_hash ELSE EXCLUDED.user_hash END,
 token_hash = CASE WHEN EXCLUDED.token_hash = '' THEN tracking_records.token_hash ELSE EXCLUDED.token_hash END,
 channel_id = CASE WHEN EXCLUDED.channel_id = '' THEN tracking_records.channel_id ELSE EXCLUDED.channel_id END,
 route_key = CASE WHEN EXCLUDED.route_key = '' THEN tracking_records.route_key ELSE EXCLUDED.route_key END,
 model = CASE WHEN EXCLUDED.model = '' THEN tracking_records.model ELSE EXCLUDED.model END,
 http_status = CASE WHEN EXCLUDED.http_status = 0 THEN tracking_records.http_status ELSE EXCLUDED.http_status END,
 upstream_status = CASE WHEN EXCLUDED.upstream_status = 0 THEN tracking_records.upstream_status ELSE EXCLUDED.upstream_status END,
 latency_ms = CASE WHEN EXCLUDED.latency_ms = 0 THEN tracking_records.latency_ms ELSE EXCLUDED.latency_ms END,
 prompt_tokens = CASE WHEN EXCLUDED.prompt_tokens = 0 THEN tracking_records.prompt_tokens ELSE EXCLUDED.prompt_tokens END,
 completion_tokens = CASE WHEN EXCLUDED.completion_tokens = 0 THEN tracking_records.completion_tokens ELSE EXCLUDED.completion_tokens END,
 total_tokens = CASE WHEN EXCLUDED.total_tokens = 0 THEN tracking_records.total_tokens ELSE EXCLUDED.total_tokens END,
 error_code = CASE WHEN EXCLUDED.error_code = '' THEN tracking_records.error_code ELSE EXCLUDED.error_code END,
 metadata = COALESCE(EXCLUDED.metadata, tracking_records.metadata),
 occurred_at = EXCLUDED.occurred_at,
 updated_at = now()`,
		event.RequestID, event.TraceID, event.Event, userHash, tokenHash, event.ChannelID, event.RouteKey,
		event.Model, event.HTTPStatus, event.UpstreamStatus, event.LatencyMS, event.PromptTokens,
		event.CompletionTokens, event.TotalTokens, event.ErrorCode, nullJSON(event.Metadata), event.OccurredAt)
	return err
}

func (p *Postgres) GetTracking(ctx context.Context, requestID string) (map[string]any, error) {
	var requestIDValue, traceID, event, userHash, tokenHash, channelID, routeKey, model, errorCode string
	var httpStatus, upstreamStatus int
	var latencyMS, promptTokens, completionTokens, totalTokens int64
	var rawMetadata []byte
	var occurredAt, createdAt, updatedAt time.Time
	err := p.pool.QueryRow(ctx, `
SELECT request_id, trace_id, event, user_hash, token_hash, channel_id, route_key, model, http_status,
       upstream_status, latency_ms, prompt_tokens, completion_tokens, total_tokens, error_code, metadata,
       occurred_at, created_at, updated_at
FROM tracking_records WHERE request_id = $1`, requestID).Scan(
		&requestIDValue, &traceID, &event, &userHash, &tokenHash, &channelID, &routeKey, &model,
		&httpStatus, &upstreamStatus, &latencyMS, &promptTokens, &completionTokens, &totalTokens,
		&errorCode, &rawMetadata, &occurredAt, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"request_id": requestIDValue, "trace_id": traceID, "event": event, "user_hash": userHash,
		"token_hash": tokenHash, "channel_id": channelID, "route_key": routeKey, "model": model,
		"http_status": httpStatus, "upstream_status": upstreamStatus, "latency_ms": latencyMS,
		"prompt_tokens": promptTokens, "completion_tokens": completionTokens, "total_tokens": totalTokens,
		"error_code": errorCode, "occurred_at": occurredAt, "created_at": createdAt, "updated_at": updatedAt,
	}
	if len(rawMetadata) > 0 {
		var metadata any
		if json.Unmarshal(rawMetadata, &metadata) == nil {
			out["metadata"] = metadata
		}
	}
	return out, nil
}

func (p *Postgres) ListTraces(ctx context.Context, limit int, routeKey, model, decision string, status int, requestID string) ([]core.RequestTrace, error) {
	if limit < 1 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := p.pool.Query(ctx, `
SELECT request_id, trace_id, route_key, method, path, model, user_hash, token_hash, client_ip_hash,
       audit_decision, audit_score, audit_categories, audit_reason, http_status, upstream_status,
       normalized_to_555, error_class, latency_ms, upstream_latency_ms, prompt_tokens, completion_tokens,
       total_tokens, request_payload, response_payload, metadata, created_at
FROM request_traces
WHERE ($2 = '' OR route_key = $2)
  AND ($3 = '' OR model = $3)
  AND ($4 = '' OR audit_decision = $4)
  AND ($5 = 0 OR http_status = $5)
  AND ($6 = '' OR request_id = $6 OR trace_id = $6)
ORDER BY created_at DESC LIMIT $1`, limit, routeKey, model, decision, status, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]core.RequestTrace, 0, limit)
	for rows.Next() {
		trace, err := scanTrace(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, trace)
	}
	return out, rows.Err()
}

func scanTrace(row pgx.Row) (core.RequestTrace, error) {
	var trace core.RequestTrace
	var decision string
	err := row.Scan(&trace.RequestID, &trace.TraceID, &trace.RouteKey, &trace.Method, &trace.Path, &trace.Model,
		&trace.UserHash, &trace.TokenHash, &trace.ClientIPHash, &decision, &trace.AuditScore,
		&trace.AuditCategories, &trace.AuditReason, &trace.HTTPStatus, &trace.UpstreamStatus,
		&trace.NormalizedTo555, &trace.ErrorClass, &trace.LatencyMS, &trace.UpstreamLatencyMS,
		&trace.PromptTokens, &trace.CompletionTokens, &trace.TotalTokens, &trace.RequestPayload,
		&trace.ResponsePayload, &trace.Metadata, &trace.CreatedAt)
	trace.AuditDecision = core.Decision(decision)
	return trace, err
}

func (p *Postgres) Dashboard(ctx context.Context) (core.DashboardStats, error) {
	stats := core.DashboardStats{}
	err := p.pool.QueryRow(ctx, `
SELECT count(*),
       count(*) FILTER (WHERE audit_decision = 'block'),
       count(*) FILTER (WHERE normalized_to_555),
       count(*) FILTER (WHERE http_status >= 400),
       COALESCE(avg(latency_ms),0),
       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms),0)
FROM request_traces WHERE created_at >= now() - interval '24 hours'`).Scan(
		&stats.Requests24H, &stats.Blocked24H, &stats.Normalized55524H, &stats.Errors24H,
		&stats.AverageLatencyMS, &stats.P95LatencyMS)
	if err != nil {
		return stats, err
	}
	stats.TopModels, err = p.topCounts(ctx, "model")
	if err != nil {
		return stats, err
	}
	stats.TopRoutes, err = p.topCounts(ctx, "route_key")
	if err != nil {
		return stats, err
	}
	stats.Recent, err = p.ListTraces(ctx, 20, "", "", "", 0, "")
	if err != nil {
		return stats, err
	}
	_ = p.pool.QueryRow(ctx, `SELECT count(*) FROM delivery_events WHERE destination='kafka' AND delivered AND created_at >= now() - interval '24 hours'`).Scan(&stats.KafkaPublished24H)
	_ = p.pool.QueryRow(ctx, `SELECT count(*) FROM delivery_events WHERE destination='deadletter' AND delivered AND created_at >= now() - interval '24 hours'`).Scan(&stats.Deadlettered24H)
	_ = p.pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox WHERE delivered_at IS NULL AND deadlettered_at IS NULL`).Scan(&stats.OutboxPending)
	return stats, nil
}

func (p *Postgres) topCounts(ctx context.Context, column string) ([]core.NameCount, error) {
	if column != "model" && column != "route_key" {
		return nil, fmt.Errorf("invalid top-count column")
	}
	query := fmt.Sprintf(`SELECT %s, count(*) FROM request_traces WHERE created_at >= now() - interval '24 hours' AND %s <> '' GROUP BY %s ORDER BY count(*) DESC LIMIT 10`, column, column, column)
	rows, err := p.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.NameCount
	for rows.Next() {
		var item core.NameCount
		if err := rows.Scan(&item.Name, &item.Count); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (p *Postgres) RecordDelivery(ctx context.Context, eventID, eventType, destination string, delivered bool, message string) error {
	if len(message) > 2000 {
		message = message[:2000]
	}
	_, err := p.pool.Exec(ctx, `
INSERT INTO delivery_events (event_id,event_type,destination,delivered,error_message)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (event_id,destination) DO UPDATE SET
  event_type = EXCLUDED.event_type, delivered = EXCLUDED.delivered, error_message = EXCLUDED.error_message, created_at = now()`,
		eventID, eventType, destination, delivered, message)
	return err
}

func validateBaseURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("base_url must be an absolute http(s) URL")
	}
	if u.User != nil {
		return fmt.Errorf("base_url must not contain credentials")
	}
	if u.Fragment != "" {
		return fmt.Errorf("base_url must not contain a fragment")
	}
	if u.Hostname() == "" {
		return fmt.Errorf("base_url must include a hostname")
	}
	return nil
}

func nullJSON(raw json.RawMessage) any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return raw
}

func SortRoutes(routes []core.Route) {
	sort.Slice(routes, func(i, j int) bool { return routes[i].Key < routes[j].Key })
}

func (p *Postgres) EnqueueOutbox(ctx context.Context, event core.OutboxEvent) (int64, error) {
	if event.EventID == "" || event.Topic == "" || len(event.Payload) == 0 {
		return 0, fmt.Errorf("invalid outbox event")
	}
	var id int64
	err := p.pool.QueryRow(ctx, `
INSERT INTO event_outbox (event_id, topic, event_key, event_type, payload, available_at)
VALUES ($1,$2,$3,$4,$5,now())
ON CONFLICT (event_id) DO UPDATE SET event_id = EXCLUDED.event_id
RETURNING id`, event.EventID, event.Topic, event.Key, event.EventType, event.Payload).Scan(&id)
	return id, err
}

func (p *Postgres) ClaimOutbox(ctx context.Context, workerID string, limit int, lease time.Duration) ([]core.OutboxEvent, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 1000 {
		limit = 1000
	}
	if lease < time.Second {
		lease = 30 * time.Second
	}
	rows, err := p.pool.Query(ctx, `
WITH picked AS (
    SELECT id
    FROM event_outbox
    WHERE delivered_at IS NULL
      AND deadlettered_at IS NULL
      AND available_at <= now()
      AND (locked_until IS NULL OR locked_until < now())
    ORDER BY id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE event_outbox AS e
SET locked_by = $1,
    locked_until = now() + ($3::bigint * interval '1 millisecond'),
    updated_at = now()
FROM picked
WHERE e.id = picked.id
RETURNING e.id, e.event_id, e.topic, e.event_key, e.event_type, e.payload, e.attempts, e.created_at`,
		workerID, limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]core.OutboxEvent, 0, limit)
	for rows.Next() {
		var event core.OutboxEvent
		if err := rows.Scan(&event.ID, &event.EventID, &event.Topic, &event.Key, &event.EventType, &event.Payload, &event.Attempts, &event.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

func (p *Postgres) MarkOutboxDelivered(ctx context.Context, id int64) error {
	_, err := p.pool.Exec(ctx, `
UPDATE event_outbox
SET delivered_at = now(), locked_until = NULL, locked_by = '', last_error = '', updated_at = now()
WHERE id = $1`, id)
	return err
}

func (p *Postgres) MarkOutboxDeadlettered(ctx context.Context, id int64, message string) error {
	if len(message) > 4000 {
		message = message[:4000]
	}
	_, err := p.pool.Exec(ctx, `
UPDATE event_outbox
SET deadlettered_at = now(), locked_until = NULL, locked_by = '', last_error = $2, updated_at = now()
WHERE id = $1`, id, message)
	return err
}

func (p *Postgres) RescheduleOutbox(ctx context.Context, id int64, attempts int, delay time.Duration, message string) error {
	if delay < time.Second {
		delay = time.Second
	}
	if delay > 10*time.Minute {
		delay = 10 * time.Minute
	}
	if len(message) > 4000 {
		message = message[:4000]
	}
	_, err := p.pool.Exec(ctx, `
UPDATE event_outbox
SET attempts = $2,
    available_at = now() + ($3::bigint * interval '1 millisecond'),
    locked_until = NULL,
    locked_by = '',
    last_error = $4,
    updated_at = now()
WHERE id = $1`, id, attempts, delay.Milliseconds(), message)
	return err
}

func (p *Postgres) OutboxPending(ctx context.Context) (int64, error) {
	var count int64
	err := p.pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox WHERE delivered_at IS NULL AND deadlettered_at IS NULL`).Scan(&count)
	return count, err
}

func (p *Postgres) CleanupEventHistory(ctx context.Context, retentionDays int) error {
	if retentionDays < 1 {
		retentionDays = 1
	}
	if _, err := p.pool.Exec(ctx, `
DELETE FROM event_outbox
WHERE COALESCE(delivered_at, deadlettered_at) IS NOT NULL
  AND updated_at < now() - make_interval(days => $1)`, retentionDays); err != nil {
		return err
	}
	_, err := p.pool.Exec(ctx, `DELETE FROM delivery_events WHERE created_at < now() - make_interval(days => $1)`, retentionDays)
	return err
}
