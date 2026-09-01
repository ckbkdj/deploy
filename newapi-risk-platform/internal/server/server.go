package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ckbkdj/newapi-risk-gateway/internal/audit"
	"github.com/ckbkdj/newapi-risk-gateway/internal/config"
	"github.com/ckbkdj/newapi-risk-gateway/internal/core"
	"github.com/ckbkdj/newapi-risk-gateway/internal/infra"
	"github.com/ckbkdj/newapi-risk-gateway/internal/storage"
	"github.com/ckbkdj/newapi-risk-gateway/internal/webui"
	"github.com/jackc/pgx/v5"
)

type cachedRoute struct {
	route   core.Route
	expires time.Time
}

type allowPrivateDialKey struct{}

type Server struct {
	cfg              config.Config
	store            *storage.Postgres
	redis            *infra.Redis
	kafka            *infra.Kafka
	events           *infra.EventPipeline
	auditor          *audit.Service
	cipher           *core.Cipher
	logger           *slog.Logger
	metrics          *Metrics
	settings         atomic.Value
	settingsDefaults core.RuntimeSettings
	routesMu         sync.RWMutex
	routes           map[string]cachedRoute
	client           *http.Client
	localLimiter     *localLimiter
}

func New(cfg config.Config, store *storage.Postgres, redis *infra.Redis, kafka *infra.Kafka, events *infra.EventPipeline, auditor *audit.Service, cipher *core.Cipher, logger *slog.Logger) (*Server, error) {
	defaults := core.RuntimeSettings{
		HotRetentionDays:    cfg.HotRetentionDays,
		KafkaRetentionDays:  cfg.KafkaRetentionDays,
		PayloadMode:         cfg.PayloadMode,
		ModelAuditEnabled:   cfg.AuditModelEnabled,
		AuditModelURL:       cfg.AuditModelURL,
		AuditModelName:      cfg.AuditModelName,
		AuditModelAPIKey:    cfg.AuditModelAPIKey,
		AuditModelKeySet:    cfg.AuditModelAPIKey != "",
		AuditTimeoutMS:      int(cfg.AuditTimeout.Milliseconds()),
		BlockThreshold:      0.75,
		ReviewThreshold:     0.45,
		AuditFailMode:       cfg.AuditFailMode,
		RateLimitEnabled:    cfg.RateLimitEnabled,
		RateLimitRPS:        cfg.RateLimitRPS,
		RateLimitBurst:      cfg.RateLimitBurst,
		NormalizeStatuses:   cfg.NormalizeStatuses,
		NormalizePatterns:   cfg.NormalizePatterns,
		MaxRequestBodyBytes: cfg.MaxRequestBodyBytes,
		MaxResponseBytes:    cfg.MaxResponseBytes,
	}
	settings, err := store.LoadSettings(context.Background(), defaults)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           secureDialContext(dialer),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.UpstreamIdleConns,
		MaxIdleConnsPerHost:   cfg.UpstreamIdleConnsPerHost,
		MaxConnsPerHost:       cfg.UpstreamMaxConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: 0,
	}
	s := &Server{
		cfg: cfg, store: store, redis: redis, kafka: kafka, events: events, auditor: auditor, cipher: cipher, logger: logger,
		metrics: &Metrics{}, routes: map[string]cachedRoute{}, localLimiter: newLocalLimiter(), settingsDefaults: defaults,
		client: &http.Client{
			Transport: transport,
			// API channels should not be able to redirect credentials or bypass the
			// route target validation. Return 3xx responses to NewAPI unchanged.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	s.settings.Store(settings)
	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/readyz", s.ready)
	mux.HandleFunc("/metrics", s.metricsHandler)
	mux.Handle("/admin/", webui.Handler())
	mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin/", http.StatusTemporaryRedirect)
	})
	mux.Handle("/api/admin/v1/", s.adminAuth(http.HandlerFunc(s.adminRouter)))
	mux.Handle("/api/v1/track", s.trackingAuth(http.HandlerFunc(s.track)))
	mux.Handle("/api/v1/track/", s.adminAuth(http.HandlerFunc(s.getTrack)))
	mux.HandleFunc("/r/", s.proxy)
	mux.HandleFunc("/v1/", s.proxy)
	return s.securityHeaders(s.recoverer(s.accessLog(mux)))
}

func (s *Server) currentSettings() core.RuntimeSettings {
	return s.settings.Load().(core.RuntimeSettings)
}

func (s *Server) reloadSettings(ctx context.Context) error {
	loaded, err := s.store.LoadSettings(ctx, s.settingsDefaults)
	if err != nil {
		return err
	}
	s.settings.Store(loaded)
	return nil
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time": time.Now().UTC()})
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	result := core.StorageHealth{AsOf: time.Now().UTC().Format(time.RFC3339)}
	result.Postgres = s.store.Ping(ctx) == nil
	result.Redis = s.redis != nil && s.redis.Enabled() && s.redis.Ping(ctx) == nil
	result.Kafka = s.kafka != nil && s.kafka.Enabled() && s.kafka.Ping(ctx) == nil
	ready := result.Postgres && (!s.cfg.RedisRequired || result.Redis) && (!s.cfg.KafkaRequired || result.Kafka)
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"ready": ready, "storage": result})
}
func (s *Server) metricsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	p, f, d, dlq := s.events.Stats()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	pending, _ := s.store.OutboxPending(ctx)
	cancel()
	s.metrics.Render(w, p, f, d, dlq, pending)
}

func (s *Server) adminAuth(next http.Handler) http.Handler {
	return tokenAuth(next, s.cfg.AdminToken, "admin")
}
func (s *Server) trackingAuth(next http.Handler) http.Handler {
	return tokenAuth(next, s.cfg.TrackingToken, "tracking")
}
func tokenAuth(next http.Handler, expected, realm string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		provided := bearerToken(r)
		if provided == "" {
			header := "X-Admin-Token"
			if realm == "tracking" {
				header = "X-Tracking-Token"
			}
			provided = r.Header.Get(header)
		}
		if expected == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+realm+`"`)
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}
func bearerToken(r *http.Request) string {
	value := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(value) > 7 && strings.EqualFold(value[:7], "Bearer ") {
		return strings.TrimSpace(value[7:])
	}
	return ""
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/admin") {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("X-Robots-Tag", "noindex, nofollow")
		}
		if strings.HasPrefix(r.URL.Path, "/admin") {
			w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'")
		}
		next.ServeHTTP(w, r)
	})
}
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Error("panic recovered", "panic", recovered, "path", r.URL.Path)
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}
func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		if !strings.HasPrefix(r.URL.Path, "/health") && !strings.HasPrefix(r.URL.Path, "/metrics") {
			s.logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(started).Milliseconds())
		}
	})
}

func (s *Server) getRoute(ctx context.Context, key string) (core.Route, error) {
	now := time.Now()
	s.routesMu.RLock()
	cached, ok := s.routes[key]
	s.routesMu.RUnlock()
	if ok && now.Before(cached.expires) {
		return cached.route, nil
	}
	route, err := s.store.GetRoute(ctx, key)
	if err != nil {
		return route, err
	}
	if !route.Enabled {
		return route, fmt.Errorf("route disabled")
	}
	if err := s.validateRouteTarget(ctx, route); err != nil {
		return route, err
	}
	s.routesMu.Lock()
	s.routes[key] = cachedRoute{route: route, expires: now.Add(30 * time.Second)}
	s.routesMu.Unlock()
	return route, nil
}
func (s *Server) invalidateRoute(key string) {
	s.routesMu.Lock()
	if key == "" {
		s.routes = map[string]cachedRoute{}
	} else {
		delete(s.routes, key)
	}
	s.routesMu.Unlock()
}
func (s *Server) validateRouteTarget(ctx context.Context, route core.Route) error {
	if s.cfg.AllowPrivateUpstreams || route.AllowPrivateTarget {
		return nil
	}
	u, err := url.Parse(route.BaseURL)
	if err != nil {
		return err
	}
	host := u.Hostname()
	if strings.EqualFold(host, "localhost") {
		return fmt.Errorf("private upstream blocked")
	}
	if ip := net.ParseIP(host); ip != nil {
		return rejectPrivateIP(ip)
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupIP(lookupCtx, "ip", host)
	if err != nil {
		return fmt.Errorf("resolve upstream: %w", err)
	}
	for _, ip := range ips {
		if err := rejectPrivateIP(ip); err != nil {
			return err
		}
	}
	return nil
}
func rejectPrivateIP(ip net.IP) error {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return fmt.Errorf("private upstream address blocked: %s", ip)
	}
	return nil
}

func secureDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if allowed, _ := ctx.Value(allowPrivateDialKey{}).(bool); allowed {
			return dialer.DialContext(ctx, network, address)
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("split upstream address: %w", err)
		}
		if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil {
			if err := rejectPrivateIP(ip); err != nil {
				return nil, err
			}
			return dialer.DialContext(ctx, network, address)
		}
		lookupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		addresses, err := net.DefaultResolver.LookupIPAddr(lookupCtx, host)
		if err != nil {
			return nil, fmt.Errorf("resolve upstream dial target: %w", err)
		}
		var lastErr error
		publicCount := 0
		for _, candidate := range addresses {
			if err := rejectPrivateIP(candidate.IP); err != nil {
				continue
			}
			publicCount++
			ipText := candidate.IP.String()
			if candidate.Zone != "" {
				ipText += "%" + candidate.Zone
			}
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ipText, port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		if publicCount == 0 {
			return nil, fmt.Errorf("upstream resolved only to private or non-routable addresses")
		}
		return nil, fmt.Errorf("dial upstream public addresses: %w", lastErr)
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any, max int64) bool {
	r.Body = http.MaxBytesReader(w, r.Body, max)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON", "detail": err.Error()})
		return false
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		detail := "multiple JSON values are not allowed"
		if err != nil {
			detail = err.Error()
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON", "detail": detail})
		return false
	}
	return true
}
func isNotFound(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
