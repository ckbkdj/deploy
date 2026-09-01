package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ckbkdj/newapi-risk-gateway/internal/core"
	"github.com/ckbkdj/newapi-risk-gateway/internal/infra"
)

func (s *Server) adminRouter(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/admin/v1/"), "/")
	switch {
	case path == "dashboard" && r.Method == http.MethodGet:
		s.adminDashboard(w, r)
	case path == "routes" && r.Method == http.MethodGet:
		s.adminListRoutes(w, r)
	case path == "routes" && r.Method == http.MethodPost:
		s.adminUpsertRoute(w, r, "")
	case strings.HasPrefix(path, "routes/") && r.Method == http.MethodPut:
		s.adminUpsertRoute(w, r, strings.TrimPrefix(path, "routes/"))
	case strings.HasPrefix(path, "routes/") && r.Method == http.MethodDelete:
		s.adminDeleteRoute(w, r, strings.TrimPrefix(path, "routes/"))
	case path == "settings" && r.Method == http.MethodGet:
		s.adminGetSettings(w, r)
	case path == "settings" && r.Method == http.MethodPut:
		s.adminSaveSettings(w, r)
	case path == "requests" && r.Method == http.MethodGet:
		s.adminRequests(w, r)
	case path == "audit/test" && r.Method == http.MethodPost:
		s.adminAuditTest(w, r)
	case path == "storage/status" && r.Method == http.MethodGet:
		s.adminStorageStatus(w, r)
	case path == "kafka/retention/apply" && r.Method == http.MethodPost:
		s.adminApplyKafkaRetention(w, r)
	case path == "version" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"name": "newapi-risk-gateway", "api": "v1"})
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
	}
}

func (s *Server) adminDashboard(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	stats, err := s.store.Dashboard(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "dashboard query failed", "detail": err.Error()})
		return
	}
	stats.Storage = s.storageHealth(ctx)
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) adminListRoutes(w http.ResponseWriter, r *http.Request) {
	routes, err := s.store.ListRoutes(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": routes})
}

func (s *Server) adminUpsertRoute(w http.ResponseWriter, r *http.Request, pathKey string) {
	var route core.Route
	if !decodeJSON(w, r, &route, 2*1024*1024) {
		return
	}
	if pathKey != "" {
		route.Key = pathKey
	}
	saved, err := s.store.UpsertRoute(r.Context(), route)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	s.invalidateRoute(saved.Key)
	if s.redis != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_ = s.redis.PublishInvalidation(ctx, saved.Key)
		cancel()
	}
	writeJSON(w, http.StatusOK, saved)
}

func (s *Server) adminDeleteRoute(w http.ResponseWriter, r *http.Request, key string) {
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "route key required"})
		return
	}
	if err := s.store.DeleteRoute(r.Context(), key); err != nil {
		if isNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "route not found"})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		return
	}
	s.invalidateRoute(key)
	if s.redis != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_ = s.redis.PublishInvalidation(ctx, key)
		cancel()
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) adminGetSettings(w http.ResponseWriter, _ *http.Request) {
	settings := s.currentSettings()
	settings.AuditModelAPIKey = ""
	writeJSON(w, http.StatusOK, settings)
}
func (s *Server) adminSaveSettings(w http.ResponseWriter, r *http.Request) {
	settings := s.currentSettings()
	if !decodeJSON(w, r, &settings, 1024*1024) {
		return
	}
	_, err := s.store.SaveSettings(r.Context(), settings)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	loaded, err := s.store.LoadSettings(r.Context(), s.currentSettings())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	s.settings.Store(loaded)
	if s.redis != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_ = s.redis.PublishInvalidation(ctx, infra.SettingsInvalidationKey)
		cancel()
	}
	warning := ""
	applied := true
	if s.kafka != nil && s.kafka.Enabled() {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		err = s.kafka.ApplyRetention(ctx, []string{s.cfg.KafkaAuditTopic, s.cfg.KafkaTraceTopic, s.cfg.KafkaDeadLetterTopic}, loaded.KafkaRetentionDays)
		cancel()
		if err != nil {
			applied = false
			warning = err.Error()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := s.store.EnsurePartitions(ctx, loaded.HotRetentionDays); err != nil && warning == "" {
		warning = err.Error()
	}
	cancel()
	response := loaded
	response.AuditModelAPIKey = ""
	writeJSON(w, http.StatusOK, map[string]any{"settings": response, "kafka_retention_applied": applied, "warning": warning})
}

func (s *Server) adminRequests(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, limitErr := strconv.Atoi(q.Get("limit"))
	if q.Get("limit") != "" && limitErr != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "limit must be an integer"})
		return
	}
	status, statusErr := strconv.Atoi(q.Get("status"))
	if q.Get("status") != "" && (statusErr != nil || status < 100 || status > 599) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "status must be 100..599"})
		return
	}
	routeKey, model, decision, requestID := q.Get("route"), q.Get("model"), q.Get("decision"), q.Get("request_id")
	if len(routeKey) > 64 || len(model) > 192 || len(requestID) > 96 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "one or more filters exceed their maximum length"})
		return
	}
	if decision != "" && decision != "allow" && decision != "review" && decision != "block" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "decision must be allow, review or block"})
		return
	}
	items, err := s.store.ListTraces(r.Context(), limit, routeKey, model, decision, status, requestID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) adminAuditTest(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Text string `json:"text"`
	}
	if !decodeJSON(w, r, &input, 256*1024) {
		return
	}
	if strings.TrimSpace(input.Text) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "text is required"})
		return
	}
	result, err := s.auditor.Audit(r.Context(), input.Text, s.currentSettings())
	response := map[string]any{"result": result}
	if err != nil {
		response["warning"] = err.Error()
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) adminStorageStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	writeJSON(w, http.StatusOK, s.storageHealth(ctx))
}
func (s *Server) storageHealth(ctx context.Context) core.StorageHealth {
	out := core.StorageHealth{AsOf: time.Now().UTC().Format(time.RFC3339)}
	out.Postgres = s.store.Ping(ctx) == nil
	out.Redis = s.redis != nil && s.redis.Enabled() && s.redis.Ping(ctx) == nil
	out.Kafka = s.kafka != nil && s.kafka.Enabled() && s.kafka.Ping(ctx) == nil
	return out
}
func (s *Server) adminApplyKafkaRetention(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Days int `json:"days"`
	}
	if !decodeJSON(w, r, &input, 64*1024) {
		return
	}
	if input.Days < -1 || input.Days > 36500 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "days must be -1..36500"})
		return
	}
	if s.kafka == nil || !s.kafka.Enabled() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "kafka disabled"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	if err := s.kafka.ApplyRetention(ctx, []string{s.cfg.KafkaAuditTopic, s.cfg.KafkaTraceTopic, s.cfg.KafkaDeadLetterTopic}, input.Days); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"applied": true, "days": input.Days})
}

func (s *Server) track(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var event core.TrackEvent
	if !decodeJSON(w, r, &event, 1024*1024) {
		return
	}
	if event.RequestID == "" {
		event.RequestID = core.NewID("req_")
	} else if !validTrackingID(event.RequestID) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "request_id must be 1..96 safe identifier characters"})
		return
	}
	if event.TraceID == "" {
		event.TraceID = core.NewID("trace_")
	} else if !validTrackingID(event.TraceID) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "trace_id must be 1..96 safe identifier characters"})
		return
	}
	if event.Event == "" {
		event.Event = "finish"
	}
	switch event.Event {
	case "start", "finish", "error", "usage":
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "event must be start, finish, error or usage"})
		return
	}
	if err := validateTrackEvent(event); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if len(event.Metadata) > 0 && !json.Valid(event.Metadata) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "metadata must be JSON"})
		return
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if err := s.store.UpsertTracking(r.Context(), event); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	// Kafka receives irreversible identity hashes as well. Metadata is forwarded
	// verbatim, so callers must not place secrets or unnecessary PII in it.
	kafkaEvent := core.TrackingKafkaEvent{
		RequestID: event.RequestID, TraceID: event.TraceID, Event: event.Event,
		UserHash:  core.StableHash(s.cfg.HashSecret, event.UserID),
		TokenHash: core.StableHash(s.cfg.HashSecret, event.TokenID),
		ChannelID: event.ChannelID, RouteKey: event.RouteKey, Model: event.Model,
		HTTPStatus: event.HTTPStatus, UpstreamStatus: event.UpstreamStatus, LatencyMS: event.LatencyMS,
		PromptTokens: event.PromptTokens, CompletionTokens: event.CompletionTokens, TotalTokens: event.TotalTokens,
		ErrorCode: event.ErrorCode, Metadata: event.Metadata, OccurredAt: event.OccurredAt,
	}
	publishCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_ = s.events.SubmitTracking(publishCtx, event.RequestID, kafkaEvent)
	cancel()
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "request_id": event.RequestID, "trace_id": event.TraceID})
}
func (s *Server) getTrack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/track/")
	if !validTrackingID(id) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "valid request id required"})
		return
	}
	record, err := s.store.GetTracking(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func validTrackingID(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 96 {
		return false
	}
	for _, r := range value {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("-_.:", r)) {
			return false
		}
	}
	return true
}

func validateTrackEvent(event core.TrackEvent) error {
	for name, item := range map[string]struct {
		value string
		max   int
	}{
		"user_id": {event.UserID, 512}, "token_id": {event.TokenID, 512},
		"channel_id": {event.ChannelID, 128}, "route_key": {event.RouteKey, 64},
		"model": {event.Model, 192}, "error_code": {event.ErrorCode, 96},
	} {
		if len(item.value) > item.max {
			return fmt.Errorf("%s exceeds %d bytes", name, item.max)
		}
	}
	for name, status := range map[string]int{"http_status": event.HTTPStatus, "upstream_status": event.UpstreamStatus} {
		if status != 0 && (status < 100 || status > 599) {
			return fmt.Errorf("%s must be 0 or 100..599", name)
		}
	}
	for name, value := range map[string]int64{
		"latency_ms": event.LatencyMS, "prompt_tokens": event.PromptTokens,
		"completion_tokens": event.CompletionTokens, "total_tokens": event.TotalTokens,
	} {
		if value < 0 {
			return fmt.Errorf("%s must be non-negative", name)
		}
	}
	if !event.OccurredAt.IsZero() && event.OccurredAt.After(time.Now().Add(10*time.Minute)) {
		return fmt.Errorf("occurred_at may not be more than 10 minutes in the future")
	}
	return nil
}
