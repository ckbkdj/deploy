package server

import (
	"context"
	"time"

	"github.com/ckbkdj/newapi-risk-gateway/internal/infra"
)

func (s *Server) RunMaintenance(ctx context.Context) {
	cleanupTicker := time.NewTicker(s.cfg.CleanupInterval)
	settingsTicker := time.NewTicker(s.cfg.SettingsRefreshInterval)
	defer cleanupTicker.Stop()
	defer settingsTicker.Stop()

	cleanup := func() {
		settings := s.currentSettings()
		workCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := s.store.EnsurePartitions(workCtx, settings.HotRetentionDays); err != nil {
			s.logger.Error("partition maintenance failed", "error", err)
			return
		}
		dropped, err := s.store.DropExpiredPartitions(workCtx, settings.HotRetentionDays)
		if err != nil {
			s.logger.Error("retention cleanup failed", "error", err)
			return
		}
		if err := s.store.CleanupEventHistory(workCtx, settings.HotRetentionDays); err != nil {
			s.logger.Error("event history cleanup failed", "error", err)
		}
		if dropped > 0 {
			s.logger.Info("expired PostgreSQL partitions dropped", "count", dropped, "retention_days", settings.HotRetentionDays)
		}
	}
	refresh := func() {
		workCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.reloadSettings(workCtx); err != nil {
			s.logger.Warn("periodic settings refresh failed", "error", err)
		}
	}

	cleanup()
	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTicker.C:
			cleanup()
		case <-settingsTicker.C:
			refresh()
		}
	}
}

func (s *Server) RunRouteInvalidationSubscriber(ctx context.Context) {
	if s.redis == nil || !s.redis.Enabled() {
		return
	}
	backoff := time.Second
	for {
		err := s.redis.SubscribeInvalidations(ctx, func(key string) {
			if key == infra.SettingsInvalidationKey {
				workCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := s.reloadSettings(workCtx)
				cancel()
				if err != nil {
					s.logger.Warn("settings invalidation reload failed", "error", err)
					return
				}
				s.logger.Debug("runtime settings reloaded")
				return
			}
			s.invalidateRoute(key)
			s.logger.Debug("route cache invalidated", "route", key)
		})
		if ctx.Err() != nil {
			return
		}
		s.logger.Warn("configuration invalidation subscriber stopped; reconnecting", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}
