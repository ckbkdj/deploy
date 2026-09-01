package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ckbkdj/newapi-risk-gateway/internal/audit"
	"github.com/ckbkdj/newapi-risk-gateway/internal/config"
	"github.com/ckbkdj/newapi-risk-gateway/internal/core"
	"github.com/ckbkdj/newapi-risk-gateway/internal/infra"
	"github.com/ckbkdj/newapi-risk-gateway/internal/server"
	"github.com/ckbkdj/newapi-risk-gateway/internal/storage"
)

var version = "dev"
var commit = "unknown"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	slog.SetDefault(logger)
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	logger.Info("starting NewAPI risk gateway", "version", version, "commit", commit, "env", cfg.AppEnv)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cipher, err := core.NewCipher(cfg.MasterKey)
	if err != nil {
		logger.Error("initialize cipher", "error", err)
		os.Exit(1)
	}
	store, err := storage.NewPostgres(ctx, cfg.DatabaseURL, cfg.DatabaseMaxConns, cfg.DatabaseMinConns, cipher, cfg.HashSecret)
	if err != nil {
		logger.Error("connect PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	if err := store.Migrate(ctx, cfg.HotRetentionDays); err != nil {
		logger.Error("database migration failed", "error", err)
		os.Exit(1)
	}
	if err := store.SeedDefaultRoute(ctx, cfg.DefaultRouteKey, cfg.DefaultRouteName, cfg.DefaultUpstreamBaseURL, cfg.DefaultUpstreamAuthMode, cfg.NormalizeStatuses, cfg.NormalizePatterns, cfg.AllowPrivateUpstreams); err != nil {
		logger.Error("seed default route", "error", err)
		os.Exit(1)
	}

	redisClient, err := infra.NewRedis(ctx, cfg.RedisURL, cfg.RedisPoolSize, cfg.RedisMinIdleConns)
	if err != nil {
		if cfg.RedisRequired {
			logger.Error("Redis required but unavailable", "error", err)
			os.Exit(1)
		}
		logger.Warn("Redis unavailable; using per-instance fallback for rate limiting and no audit cache", "error", err)
		redisClient, _ = infra.NewRedis(ctx, "", cfg.RedisPoolSize, cfg.RedisMinIdleConns)
	}
	defer redisClient.Close()

	kafkaClient, err := infra.NewKafka(cfg)
	if err != nil {
		if cfg.KafkaRequired {
			logger.Error("Kafka required but unavailable", "error", err)
			os.Exit(1)
		}
		logger.Warn("Kafka unavailable; PostgreSQL hot trace storage remains active", "error", err)
		kafkaClient = &infra.Kafka{}
	}
	defer kafkaClient.Close()
	if cfg.KafkaAutoCreateTopics && kafkaClient.Enabled() {
		topicCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err = kafkaClient.EnsureTopics(topicCtx, []string{cfg.KafkaAuditTopic, cfg.KafkaTraceTopic, cfg.KafkaDeadLetterTopic}, cfg.KafkaTopicPartitions, cfg.KafkaReplicationFactor, cfg.KafkaRetentionDays)
		cancel()
		if err != nil {
			if cfg.KafkaRequired {
				logger.Error("Kafka topic initialization failed", "error", err)
				os.Exit(1)
			}
			logger.Warn("Kafka topics could not be auto-created; create them with scripts/init-kafka.sh", "error", err)
		}
	}

	events := infra.NewEventPipeline(store, kafkaClient, cfg.KafkaAuditTopic, cfg.KafkaTraceTopic, cfg.KafkaDeadLetterTopic, cfg.AuditQueueSize, cfg.AuditWorkers, logger)
	events.Start(ctx)
	defer events.Stop()
	auditor := audit.NewService(audit.NewModelClient(cfg.AuditModelMaxConcurrency), redisClient, logger)
	app, err := server.New(cfg, store, redisClient, kafkaClient, events, auditor, cipher, logger)
	if err != nil {
		logger.Error("initialize server", "error", err)
		os.Exit(1)
	}
	go app.RunMaintenance(ctx)
	go app.RunRouteInvalidationSubscriber(ctx)

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           app.Handler(),
		ReadTimeout:       cfg.ClientReadTimeout,
		ReadHeaderTimeout: minDuration(cfg.ClientReadTimeout, 10*time.Second),
		IdleTimeout:       cfg.ClientIdleTimeout,
		WriteTimeout:      0, // SSE/streaming responses must not be cut by a global write deadline.
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", "address", cfg.ListenAddr, "admin", cfg.PublicBaseURL+"/admin/")
		serverErr <- httpServer.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", "error", err)
			stop()
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		_ = httpServer.Close()
	}
	logger.Info("shutdown complete")
}

func logLevel() slog.Level {
	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
