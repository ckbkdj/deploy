package config

import (
	"strings"
	"testing"
)

func setValidEnvironment(t *testing.T) {
	t.Helper()
	for key, value := range map[string]string{
		"APP_ENV":             "production",
		"PUBLIC_BASE_URL":     "https://risk.example.com",
		"DATABASE_URL":        "postgres://risk:password@postgres:5432/risk?sslmode=disable",
		"REDIS_REQUIRED":      "false",
		"KAFKA_ENABLED":       "false",
		"KAFKA_REQUIRED":      "false",
		"AUDIT_MODEL_ENABLED": "false",
		"ADMIN_TOKEN":         "admin-token-012345678901234567890",
		"TRACKING_TOKEN":      "tracking-token-012345678901234567890",
		"HASH_SECRET":         "hash-secret-012345678901234567890",
		"MASTER_KEY":          "master-key-012345678901234567890",
	} {
		t.Setenv(key, value)
	}
}

func TestLoadValidProduction(t *testing.T) {
	setValidEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.KafkaRetentionDays != 180 {
		t.Fatalf("KafkaRetentionDays = %d, want 180", cfg.KafkaRetentionDays)
	}
	if cfg.RedisPoolSize < cfg.RedisMinIdleConns {
		t.Fatalf("invalid Redis pool defaults: %+v", cfg)
	}
}

func TestLoadRejectsMalformedEnvironment(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("DATABASE_MAX_CONNS", "not-an-int")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "DATABASE_MAX_CONNS") {
		t.Fatalf("Load() error = %v, want DATABASE_MAX_CONNS validation", err)
	}
}

func TestLoadRejectsKafkaSASLWithoutCredentials(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("KAFKA_ENABLED", "true")
	t.Setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
	t.Setenv("KAFKA_USERNAME", "")
	t.Setenv("KAFKA_PASSWORD", "")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "KAFKA_USERNAME") {
		t.Fatalf("Load() error = %v, want SASL credential validation", err)
	}
}

func TestLoadDevelopmentSecrets(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("APP_ENV", "development")
	t.Setenv("ADMIN_TOKEN", "")
	t.Setenv("TRACKING_TOKEN", "")
	t.Setenv("HASH_SECRET", "")
	t.Setenv("MASTER_KEY", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.AdminToken == "" || cfg.TrackingToken == "" || cfg.HashSecret == "" || cfg.MasterKey == "" {
		t.Fatalf("development secrets were not populated")
	}
}
