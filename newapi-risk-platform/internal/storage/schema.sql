CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS app_settings (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    config JSONB NOT NULL,
    audit_model_api_key_enc TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS routes (
    id BIGSERIAL PRIMARY KEY,
    route_key VARCHAR(96) NOT NULL UNIQUE,
    name VARCHAR(160) NOT NULL,
    base_url TEXT NOT NULL,
    auth_mode VARCHAR(24) NOT NULL DEFAULT 'passthrough',
    managed_secret_enc TEXT NOT NULL DEFAULT '',
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    model_map JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    timeout_ms INTEGER NOT NULL DEFAULT 600000,
    normalize_errors BOOLEAN NOT NULL DEFAULT TRUE,
    normalize_statuses INTEGER[] NOT NULL DEFAULT ARRAY[401,403,408,409,429,500,502,503,504],
    normalize_patterns TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    allow_private_target BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT routes_auth_mode_check CHECK (auth_mode IN ('passthrough', 'managed', 'none')),
    CONSTRAINT routes_key_check CHECK (route_key ~ '^[a-zA-Z0-9][a-zA-Z0-9._-]{0,95}$')
);

CREATE TABLE IF NOT EXISTS request_traces (
    request_id VARCHAR(96) NOT NULL,
    trace_id VARCHAR(96) NOT NULL,
    route_key VARCHAR(96) NOT NULL DEFAULT '',
    method VARCHAR(16) NOT NULL DEFAULT '',
    path TEXT NOT NULL DEFAULT '',
    model VARCHAR(192) NOT NULL DEFAULT '',
    user_hash CHAR(64) NOT NULL DEFAULT '',
    token_hash CHAR(64) NOT NULL DEFAULT '',
    client_ip_hash CHAR(64) NOT NULL DEFAULT '',
    audit_decision VARCHAR(16) NOT NULL DEFAULT 'allow',
    audit_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    audit_categories TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    audit_reason TEXT NOT NULL DEFAULT '',
    http_status INTEGER NOT NULL DEFAULT 0,
    upstream_status INTEGER NOT NULL DEFAULT 0,
    normalized_to_555 BOOLEAN NOT NULL DEFAULT FALSE,
    error_class VARCHAR(96) NOT NULL DEFAULT '',
    latency_ms BIGINT NOT NULL DEFAULT 0,
    upstream_latency_ms BIGINT NOT NULL DEFAULT 0,
    prompt_tokens BIGINT NOT NULL DEFAULT 0,
    completion_tokens BIGINT NOT NULL DEFAULT 0,
    total_tokens BIGINT NOT NULL DEFAULT 0,
    request_payload JSONB,
    response_payload JSONB,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS request_traces_request_id_idx ON request_traces (request_id);
CREATE INDEX IF NOT EXISTS request_traces_trace_id_idx ON request_traces (trace_id);
CREATE INDEX IF NOT EXISTS request_traces_route_created_idx ON request_traces (route_key, created_at DESC);
CREATE INDEX IF NOT EXISTS request_traces_model_created_idx ON request_traces (model, created_at DESC);
CREATE INDEX IF NOT EXISTS request_traces_status_created_idx ON request_traces (http_status, created_at DESC);
CREATE INDEX IF NOT EXISTS request_traces_created_brin_idx ON request_traces USING BRIN (created_at);

CREATE TABLE IF NOT EXISTS tracking_records (
    request_id VARCHAR(96) PRIMARY KEY,
    trace_id VARCHAR(96) NOT NULL DEFAULT '',
    event VARCHAR(32) NOT NULL DEFAULT 'finish',
    user_hash CHAR(64) NOT NULL DEFAULT '',
    token_hash CHAR(64) NOT NULL DEFAULT '',
    channel_id VARCHAR(128) NOT NULL DEFAULT '',
    route_key VARCHAR(96) NOT NULL DEFAULT '',
    model VARCHAR(192) NOT NULL DEFAULT '',
    http_status INTEGER NOT NULL DEFAULT 0,
    upstream_status INTEGER NOT NULL DEFAULT 0,
    latency_ms BIGINT NOT NULL DEFAULT 0,
    prompt_tokens BIGINT NOT NULL DEFAULT 0,
    completion_tokens BIGINT NOT NULL DEFAULT 0,
    total_tokens BIGINT NOT NULL DEFAULT 0,
    error_code VARCHAR(96) NOT NULL DEFAULT '',
    metadata JSONB,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS tracking_records_updated_idx ON tracking_records (updated_at DESC);
CREATE INDEX IF NOT EXISTS tracking_records_user_idx ON tracking_records (user_hash, updated_at DESC);

CREATE TABLE IF NOT EXISTS delivery_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(96) NOT NULL,
    event_type VARCHAR(96) NOT NULL,
    destination VARCHAR(32) NOT NULL,
    delivered BOOLEAN NOT NULL,
    error_message TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE delivery_events DROP CONSTRAINT IF EXISTS delivery_events_event_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS delivery_events_event_destination_uidx ON delivery_events (event_id, destination);
CREATE INDEX IF NOT EXISTS delivery_events_created_idx ON delivery_events (created_at DESC);


CREATE TABLE IF NOT EXISTS event_outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(96) NOT NULL UNIQUE,
    topic VARCHAR(249) NOT NULL,
    event_key VARCHAR(256) NOT NULL DEFAULT '',
    event_type VARCHAR(96) NOT NULL,
    payload BYTEA NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until TIMESTAMPTZ,
    locked_by VARCHAR(96) NOT NULL DEFAULT '',
    delivered_at TIMESTAMPTZ,
    deadlettered_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS event_outbox_pending_idx
    ON event_outbox (available_at, id)
    WHERE delivered_at IS NULL AND deadlettered_at IS NULL;
CREATE INDEX IF NOT EXISTS event_outbox_delivered_idx ON event_outbox (delivered_at);
