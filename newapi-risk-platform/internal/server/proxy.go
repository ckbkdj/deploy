package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ckbkdj/newapi-risk-gateway/internal/core"
)

var hopByHopHeaders = map[string]struct{}{
	"Connection": {}, "Proxy-Connection": {}, "Keep-Alive": {}, "Proxy-Authenticate": {},
	"Proxy-Authorization": {}, "Te": {}, "Trailer": {}, "Transfer-Encoding": {}, "Upgrade": {},
}

func (s *Server) proxy(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	completeMetric := s.metrics.Begin()
	metricDone := false
	statusForMetric := http.StatusInternalServerError
	blockedForMetric := false
	normalizedForMetric := false
	defer func() {
		if !metricDone {
			completeMetric(statusForMetric, blockedForMetric, normalizedForMetric, started)
		}
	}()

	requestID := validOrNewID(r.Header.Get("X-Request-ID"), "req_")
	traceID := validOrNewID(firstNonEmpty(r.Header.Get("X-Trace-ID"), parseTraceparent(r.Header.Get("traceparent"))), "trace_")
	w.Header().Set("X-Request-ID", requestID)
	w.Header().Set("X-Trace-ID", traceID)

	routeKey, upstreamPath, ok := parseProxyPath(r.URL.Path, s.cfg.DefaultRouteKey)
	if !ok {
		statusForMetric = http.StatusNotFound
		writeJSON(w, statusForMetric, map[string]any{"error": "invalid proxy route"})
		return
	}
	route, err := s.getRoute(r.Context(), routeKey)
	if err != nil {
		statusForMetric = http.StatusBadGateway
		if isNotFound(err) {
			statusForMetric = http.StatusNotFound
		}
		writeJSON(w, statusForMetric, map[string]any{"error": "route unavailable", "route": routeKey, "request_id": requestID})
		return
	}

	settings := s.currentSettings()
	maxBody := settings.MaxRequestBodyBytes
	if maxBody <= 0 {
		maxBody = s.cfg.MaxRequestBodyBytes
	}
	body, err := readLimitedBody(r.Body, maxBody)
	if err != nil {
		statusForMetric = http.StatusRequestEntityTooLarge
		writeJSON(w, statusForMetric, map[string]any{"error": err.Error(), "request_id": requestID})
		return
	}

	extracted := core.ExtractedRequest{}
	if len(bytes.TrimSpace(body)) > 0 && shouldParseJSON(r) {
		extracted, err = core.ExtractOpenAIRequest(body)
		if err != nil {
			statusForMetric = http.StatusBadRequest
			writeJSON(w, statusForMetric, map[string]any{"error": "invalid OpenAI-compatible JSON body", "detail": err.Error(), "request_id": requestID})
			return
		}
	}

	trace := core.RequestTrace{
		RequestID: requestID, TraceID: traceID, RouteKey: routeKey, Method: r.Method, Path: upstreamPath,
		Model: extracted.Model, UserHash: core.StableHash(s.cfg.HashSecret, userIdentity(r)),
		TokenHash: core.StableHash(s.cfg.HashSecret, authIdentity(r)), ClientIPHash: core.StableHash(s.cfg.HashSecret, s.clientIP(r)),
		AuditDecision: core.DecisionAllow, CreatedAt: started.UTC(),
	}
	trace.RequestPayload = s.capturePayload(settings.PayloadMode, body)

	record := func(status, upstreamStatus int, auditResult core.AuditResult, normalized bool, errorClass string, response []byte, upstreamStarted time.Time) {
		trace.HTTPStatus = status
		trace.UpstreamStatus = upstreamStatus
		trace.NormalizedTo555 = normalized
		trace.ErrorClass = errorClass
		trace.LatencyMS = time.Since(started).Milliseconds()
		if !upstreamStarted.IsZero() {
			trace.UpstreamLatencyMS = time.Since(upstreamStarted).Milliseconds()
		}
		trace.AuditDecision = auditResult.Decision
		trace.AuditScore = auditResult.Score
		trace.AuditCategories = auditResult.Categories
		trace.AuditReason = auditResult.Reason
		trace.ResponsePayload = s.capturePayload(settings.PayloadMode, response)
		metadata, _ := json.Marshal(map[string]any{"audit_source": auditResult.Source, "rule_hits": auditResult.RuleHits, "audit_cached": auditResult.Cached, "audit_latency_ms": auditResult.LatencyMS})
		trace.Metadata = metadata
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := s.events.SubmitTrace(ctx, trace); err != nil {
			s.logger.Error("submit trace failed", "request_id", requestID, "error", err)
		}
		cancel()
		statusForMetric = status
		blockedForMetric = auditResult.Decision == core.DecisionBlock
		normalizedForMetric = normalized
		completeMetric(status, blockedForMetric, normalized, started)
		metricDone = true
	}

	if settings.RateLimitEnabled {
		limitKey := routeKey + ":" + firstNonEmpty(userIdentity(r), authIdentity(r), s.clientIP(r))
		allowed := false
		if s.redis != nil && s.redis.Enabled() {
			allowed, _, err = s.redis.Allow(r.Context(), core.StableHash(s.cfg.HashSecret, limitKey), settings.RateLimitRPS, settings.RateLimitBurst)
			if err != nil {
				s.logger.Warn("redis rate limiter failed", "error", err)
				if s.cfg.RedisRequired {
					statusForMetric = http.StatusServiceUnavailable
					writeJSON(w, statusForMetric, map[string]any{"error": "rate limiter unavailable", "request_id": requestID})
					record(statusForMetric, 0, core.AuditResult{Decision: core.DecisionAllow, Reason: "not audited", Source: "none"}, false, "redis_unavailable", nil, time.Time{})
					return
				}
				allowed = s.localLimiter.Allow(limitKey, settings.RateLimitRPS, settings.RateLimitBurst)
			}
		} else {
			allowed = s.localLimiter.Allow(limitKey, settings.RateLimitRPS, settings.RateLimitBurst)
		}
		if !allowed {
			s.metrics.rateLimited.Add(1)
			statusForMetric = http.StatusTooManyRequests
			w.Header().Set("Retry-After", "1")
			writeJSON(w, statusForMetric, map[string]any{"error": {"message": "rate limit exceeded", "type": "rate_limit_error", "code": "rate_limit_exceeded"}, "request_id": requestID, "trace_id": traceID})
			record(statusForMetric, 0, core.AuditResult{Decision: core.DecisionAllow, Reason: "rate limited before audit", Source: "rate_limit"}, false, "rate_limit", nil, time.Time{})
			return
		}
	}

	auditResult := core.AuditResult{Decision: core.DecisionAllow, Reason: "request has no auditable text", Source: "none"}
	if strings.TrimSpace(extracted.Text) != "" {
		auditResult, err = s.auditor.Audit(r.Context(), extracted.Text, settings)
		if err != nil {
			s.metrics.auditErrors.Add(1)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_ = s.events.SubmitAudit(ctx, requestID, map[string]any{"request_id": requestID, "trace_id": traceID, "route_key": routeKey, "model": extracted.Model, "result": auditResult})
		cancel()
		if auditResult.Decision == core.DecisionBlock {
			blockedForMetric = true
			statusForMetric = s.cfg.RiskHTTPStatus
			w.Header().Set("X-Risk-Decision", "block")
			w.Header().Set("X-Risk-Error-Source", "policy")
			response := core.RiskErrorBody("Request rejected by cybersecurity risk policy", requestID, traceID)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(statusForMetric)
			_, _ = w.Write(response)
			record(statusForMetric, 0, auditResult, false, "cyber_policy", response, time.Time{})
			return
		}
		w.Header().Set("X-Risk-Decision", string(auditResult.Decision))
	}

	modelMap := map[string]string{}
	_ = json.Unmarshal(route.ModelMap, &modelMap)
	if rewritten, mapped, changed := core.RewriteModel(body, modelMap); changed {
		body = rewritten
		trace.Model = mapped
	}
	target, err := buildUpstreamURL(route.BaseURL, upstreamPath, r.URL.RawQuery)
	if err != nil {
		statusForMetric = http.StatusBadGateway
		writeJSON(w, statusForMetric, map[string]any{"error": "invalid upstream URL", "request_id": requestID})
		record(statusForMetric, 0, auditResult, false, "upstream_url", nil, time.Time{})
		return
	}

	timeout := time.Duration(route.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = s.cfg.RequestTimeout
	}
	proxyCtx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	proxyCtx = context.WithValue(proxyCtx, allowPrivateDialKey{}, s.cfg.AllowPrivateUpstreams || route.AllowPrivateTarget)
	upReq, err := http.NewRequestWithContext(proxyCtx, r.Method, target, bytes.NewReader(body))
	if err != nil {
		statusForMetric = http.StatusInternalServerError
		writeJSON(w, statusForMetric, map[string]any{"error": "build upstream request failed", "request_id": requestID})
		record(statusForMetric, 0, auditResult, false, "request_build", nil, time.Time{})
		return
	}
	copyRequestHeaders(upReq.Header, r.Header)
	applyRouteHeaders(upReq.Header, route)
	upReq.Header.Set("X-Risk-Request-ID", requestID)
	upReq.Header.Set("X-Risk-Trace-ID", traceID)
	if len(body) > 0 {
		upReq.ContentLength = int64(len(body))
	}

	upstreamStarted := time.Now()
	resp, err := s.client.Do(upReq)
	if err != nil {
		statusForMetric = s.cfg.RiskHTTPStatus
		normalizedForMetric = true
		w.Header().Set("X-Risk-Error-Source", "upstream_transport")
		response := core.RiskErrorBody("Upstream channel request failed", requestID, traceID)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(statusForMetric)
		_, _ = w.Write(response)
		record(statusForMetric, 0, auditResult, true, classifyTransportError(err), response, upstreamStarted)
		return
	}
	defer resp.Body.Close()

	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	isStream := strings.Contains(contentType, "text/event-stream")
	if isStream && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		status, normalized, errorClass, captured := s.forwardSSE(w, resp, requestID, traceID, route, settings, &trace)
		record(status, resp.StatusCode, auditResult, normalized, errorClass, captured, upstreamStarted)
		return
	}

	maxResponse := settings.MaxResponseBytes
	if maxResponse <= 0 {
		maxResponse = s.cfg.MaxResponseBytes
	}
	responseBody, readErr := readLimitedBody(resp.Body, maxResponse)
	if readErr != nil {
		statusForMetric = http.StatusBadGateway
		response := core.RiskErrorBody("Upstream response exceeded configured limit", requestID, traceID)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(statusForMetric)
		_, _ = w.Write(response)
		record(statusForMetric, resp.StatusCode, auditResult, false, "response_too_large", response, upstreamStarted)
		return
	}
	trace.PromptTokens, trace.CompletionTokens, trace.TotalTokens = parseUsage(responseBody)
	statuses := route.NormalizeStatuses
	if len(statuses) == 0 {
		statuses = settings.NormalizeStatuses
	}
	patterns := route.NormalizePatterns
	if len(patterns) == 0 {
		patterns = settings.NormalizePatterns
	}
	normalize, reason := false, ""
	if route.NormalizeErrors {
		normalize, reason = core.ShouldNormalizeUpstreamError(resp.StatusCode, responseBody, statuses, patterns)
		if resp.StatusCode >= 200 && resp.StatusCode < 300 && core.IsStructuredError(responseBody) {
			normalize = true
			reason = "structured error returned with successful HTTP status"
		}
	}
	if normalize {
		statusForMetric = s.cfg.RiskHTTPStatus
		normalizedForMetric = true
		w.Header().Set("X-Risk-Error-Source", "upstream")
		w.Header().Set("X-Risk-Normalization-Reason", safeHeader(reason))
		response := core.RiskErrorBody("Upstream model/channel error normalized by risk control", requestID, traceID)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(statusForMetric)
		_, _ = w.Write(response)
		record(statusForMetric, resp.StatusCode, auditResult, true, "upstream_model_error", response, upstreamStarted)
		return
	}
	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(responseBody)
	record(resp.StatusCode, resp.StatusCode, auditResult, false, errorClassFromStatus(resp.StatusCode), responseBody, upstreamStarted)
}

func (s *Server) forwardSSE(w http.ResponseWriter, resp *http.Response, requestID, traceID string, route core.Route, settings core.RuntimeSettings, trace *core.RequestTrace) (int, bool, string, []byte) {
	copyResponseHeaders(w.Header(), resp.Header)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(resp.StatusCode)
	flusher, _ := w.(http.Flusher)
	scanner := bufio.NewScanner(resp.Body)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 4*1024*1024)
	capture := bytes.Buffer{}
	normalized := false
	errorClass := ""
	patterns := route.NormalizePatterns
	if len(patterns) == 0 {
		patterns = settings.NormalizePatterns
	}
	errorEvent := false
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		trimmed := bytes.TrimSpace(line)
		if bytes.HasPrefix(trimmed, []byte("event:")) {
			eventName := strings.ToLower(strings.TrimSpace(string(bytes.TrimPrefix(trimmed, []byte("event:")))))
			errorEvent = strings.Contains(eventName, "error") || strings.Contains(eventName, "failed")
		}
		if route.NormalizeErrors && bytes.HasPrefix(trimmed, []byte("data:")) {
			changedLine, changed := core.NormalizeSSEDataLine(line, requestID, traceID, patterns)
			if !changed && errorEvent {
				changedLine = append([]byte("data: "), core.RiskErrorBody("Upstream model/channel error normalized by risk control", requestID, traceID)...)
				changed = true
			}
			if changed {
				line = changedLine
				normalized = true
				errorClass = "upstream_stream_error"
			}
			errorEvent = false
		}
		if capture.Len() < int(s.cfg.PayloadCaptureBytes) {
			remaining := int(s.cfg.PayloadCaptureBytes) - capture.Len()
			if len(line) > remaining {
				capture.Write(line[:remaining])
			} else {
				capture.Write(line)
			}
			capture.WriteByte('\n')
		}
		if bytes.HasPrefix(bytes.TrimSpace(line), []byte("data:")) {
			data := bytes.TrimSpace(bytes.TrimPrefix(bytes.TrimSpace(line), []byte("data:")))
			p, c, t := parseUsage(data)
			if p > 0 {
				trace.PromptTokens = p
			}
			if c > 0 {
				trace.CompletionTokens = c
			}
			if t > 0 {
				trace.TotalTokens = t
			}
		}
		_, _ = w.Write(line)
		_, _ = w.Write([]byte("\n"))
		if flusher != nil {
			flusher.Flush()
		}
	}
	if err := scanner.Err(); err != nil {
		normalized = true
		errorClass = "stream_interrupted"
		line := append([]byte("event: error\ndata: "), core.RiskErrorBody("Upstream stream interrupted", requestID, traceID)...)
		line = append(line, []byte("\n\n")...)
		_, _ = w.Write(line)
		if flusher != nil {
			flusher.Flush()
		}
	}
	return resp.StatusCode, normalized, errorClass, capture.Bytes()
}

func parseProxyPath(path, defaultKey string) (string, string, bool) {
	if strings.HasPrefix(path, "/r/") {
		rest := strings.TrimPrefix(path, "/r/")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) != 2 || parts[0] == "" {
			return "", "", false
		}
		return parts[0], "/" + parts[1], true
	}
	if strings.HasPrefix(path, "/v1/") {
		return defaultKey, path, true
	}
	return "", "", false
}
func buildUpstreamURL(baseURL, path, rawQuery string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.TrimLeft(path, "/")
	if u.RawQuery != "" && rawQuery != "" {
		u.RawQuery = u.RawQuery + "&" + rawQuery
	} else if rawQuery != "" {
		u.RawQuery = rawQuery
	}
	return u.String(), nil
}
func readLimitedBody(reader io.Reader, max int64) ([]byte, error) {
	if reader == nil {
		return nil, nil
	}
	if max < 1 {
		max = 8 * 1024 * 1024
	}
	body, err := io.ReadAll(io.LimitReader(reader, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > max {
		return nil, fmt.Errorf("body exceeds %d bytes", max)
	}
	return body, nil
}
func shouldParseJSON(r *http.Request) bool {
	if r.Method != http.MethodPost && r.Method != http.MethodPut && r.Method != http.MethodPatch {
		return false
	}
	contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	// OpenAI-compatible JSON requests sometimes omit Content-Type. Non-JSON
	// payloads (for example multipart audio uploads) are proxied without body audit.
	return contentType == "" || strings.Contains(contentType, "application/json") || strings.Contains(contentType, "+json")
}
func copyRequestHeaders(dst, src http.Header) {
	for key, values := range src {
		canonical := http.CanonicalHeaderKey(key)
		if _, skip := hopByHopHeaders[canonical]; skip || canonical == "Host" || canonical == "Content-Length" {
			continue
		}
		for _, value := range values {
			dst.Add(canonical, value)
		}
	}
}
func copyResponseHeaders(dst, src http.Header) {
	for key, values := range src {
		canonical := http.CanonicalHeaderKey(key)
		if _, skip := hopByHopHeaders[canonical]; skip || canonical == "Content-Length" {
			continue
		}
		for _, value := range values {
			dst.Add(canonical, value)
		}
	}
}
func applyRouteHeaders(headers http.Header, route core.Route) {
	var configured map[string]string
	_ = json.Unmarshal(route.Headers, &configured)
	for key, value := range configured {
		headers.Set(key, value)
	}
	switch route.AuthMode {
	case "none":
		headers.Del("Authorization")
	case "managed":
		secret := strings.TrimSpace(route.ManagedSecret)
		if secret == "" {
			headers.Del("Authorization")
		} else if strings.HasPrefix(strings.ToLower(secret), "bearer ") || strings.HasPrefix(strings.ToLower(secret), "basic ") {
			headers.Set("Authorization", secret)
		} else {
			headers.Set("Authorization", "Bearer "+secret)
		}
	}
}
func (s *Server) capturePayload(mode string, body []byte) json.RawMessage {
	if len(body) == 0 || mode == "none" {
		return nil
	}
	if mode == "encrypted" {
		ciphertext, err := s.cipher.Encrypt(body)
		if err != nil {
			s.logger.Error("payload encryption failed", "error", err)
			return core.RedactJSON(body, s.cfg.PayloadCaptureBytes)
		}
		raw, _ := json.Marshal(map[string]any{"alg": "AES-256-GCM", "ciphertext": ciphertext, "sha256": core.SHA256Hex(body), "bytes": len(body)})
		return raw
	}
	return core.RedactJSON(body, s.cfg.PayloadCaptureBytes)
}
func parseUsage(body []byte) (int64, int64, int64) {
	if len(bytes.TrimSpace(body)) == 0 {
		return 0, 0, 0
	}
	var root map[string]any
	if json.Unmarshal(body, &root) != nil {
		return 0, 0, 0
	}
	usage, ok := root["usage"].(map[string]any)
	if !ok {
		if response, ok := root["response"].(map[string]any); ok {
			usage, _ = response["usage"].(map[string]any)
		}
	}
	if usage == nil {
		return 0, 0, 0
	}
	prompt := number(usage, "prompt_tokens", "input_tokens")
	completion := number(usage, "completion_tokens", "output_tokens")
	total := number(usage, "total_tokens")
	if total == 0 {
		total = prompt + completion
	}
	return prompt, completion, total
}
func number(values map[string]any, keys ...string) int64 {
	for _, key := range keys {
		switch v := values[key].(type) {
		case float64:
			return int64(v)
		case json.Number:
			n, _ := v.Int64()
			return n
		case int64:
			return v
		case int:
			return int64(v)
		}
	}
	return 0
}
func validOrNewID(value, prefix string) string {
	value = strings.TrimSpace(value)
	if value != "" && len(value) <= 96 {
		for _, r := range value {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("-_.:", r)) {
				return core.NewID(prefix)
			}
		}
		return value
	}
	return core.NewID(prefix)
}
func parseTraceparent(value string) string {
	parts := strings.Split(value, "-")
	if len(parts) >= 4 && len(parts[1]) == 32 {
		return parts[1]
	}
	return ""
}
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
func userIdentity(r *http.Request) string {
	return firstNonEmpty(r.Header.Get("X-Risk-User-ID"), r.Header.Get("X-User-ID"), r.Header.Get("New-Api-User"), r.Header.Get("X-New-Api-User"))
}
func authIdentity(r *http.Request) string {
	value := r.Header.Get("Authorization")
	if len(value) > 256 {
		value = value[:256]
	}
	return value
}
func (s *Server) clientIP(r *http.Request) string {
	if s.cfg.TrustProxyHeaders {
		if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
			return strings.TrimSpace(strings.Split(xff, ",")[0])
		}
		if rip := strings.TrimSpace(r.Header.Get("X-Real-IP")); rip != "" {
			return rip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}
func classifyTransportError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "upstream_timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "upstream_timeout"
		}
		return "upstream_network"
	}
	return "upstream_transport"
}
func errorClassFromStatus(status int) string {
	if status < 400 {
		return ""
	}
	if status == 429 {
		return "upstream_rate_limit"
	}
	if status >= 500 {
		return "upstream_server"
	}
	return "upstream_client"
}
func safeHeader(value string) string {
	value = strings.ReplaceAll(value, "\r", "")
	value = strings.ReplaceAll(value, "\n", "")
	if len(value) > 180 {
		value = value[:180]
	}
	return value
}
