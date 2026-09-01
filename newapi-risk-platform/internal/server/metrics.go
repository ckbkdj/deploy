package server

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type Metrics struct {
	requests       atomic.Uint64
	completed      atomic.Uint64
	blocked        atomic.Uint64
	normalized     atomic.Uint64
	upstreamErrors atomic.Uint64
	auditErrors    atomic.Uint64
	rateLimited    atomic.Uint64
	inflight       atomic.Int64
	latencyBuckets [8]atomic.Uint64
	latencySumMS   atomic.Uint64
}

var latencyBounds = []int64{25, 50, 100, 250, 500, 1000, 3000, 10000}

func (m *Metrics) Begin() func(status int, blocked, normalized bool, started time.Time) {
	m.requests.Add(1)
	m.inflight.Add(1)
	return func(status int, blocked, normalized bool, started time.Time) {
		m.inflight.Add(-1)
		m.completed.Add(1)
		if blocked {
			m.blocked.Add(1)
		}
		if normalized {
			m.normalized.Add(1)
		}
		if status >= 500 && !blocked {
			m.upstreamErrors.Add(1)
		}
		ms := time.Since(started).Milliseconds()
		if ms < 0 {
			ms = 0
		}
		m.latencySumMS.Add(uint64(ms))
		for i, bound := range latencyBounds {
			if ms <= bound {
				m.latencyBuckets[i].Add(1)
				break
			}
		}
	}
}

func (m *Metrics) Render(w io.Writer, kafkaPublished, kafkaFailed, queueDropped, kafkaDeadlettered uint64, outboxPending int64) {
	writeMetric := func(name, help, typ string, value any) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %v\n", name, help, name, typ, name, value)
	}
	writeMetric("risk_gateway_requests_total", "Total proxied requests", "counter", m.requests.Load())
	writeMetric("risk_gateway_blocked_total", "Requests blocked by risk control", "counter", m.blocked.Load())
	writeMetric("risk_gateway_normalized_555_total", "Upstream errors normalized to code 555", "counter", m.normalized.Load())
	writeMetric("risk_gateway_upstream_errors_total", "Upstream/server errors", "counter", m.upstreamErrors.Load())
	writeMetric("risk_gateway_audit_errors_total", "Audit model/cache errors", "counter", m.auditErrors.Load())
	writeMetric("risk_gateway_rate_limited_total", "Requests rejected by rate limiting", "counter", m.rateLimited.Load())
	writeMetric("risk_gateway_inflight", "Current in-flight proxy requests", "gauge", m.inflight.Load())
	writeMetric("risk_gateway_kafka_published_total", "Events published to Kafka", "counter", kafkaPublished)
	writeMetric("risk_gateway_kafka_failed_total", "Kafka/event delivery failures", "counter", kafkaFailed)
	writeMetric("risk_gateway_event_queue_fallback_total", "Queue saturation fallbacks", "counter", queueDropped)
	writeMetric("risk_gateway_kafka_deadlettered_total", "Events moved to the Kafka dead-letter topic", "counter", kafkaDeadlettered)
	writeMetric("risk_gateway_outbox_pending", "Durable Kafka outbox rows pending delivery", "gauge", outboxPending)
	fmt.Fprintln(w, "# HELP risk_gateway_request_latency_ms Proxy request latency in milliseconds")
	fmt.Fprintln(w, "# TYPE risk_gateway_request_latency_ms histogram")
	cumulative := uint64(0)
	for i, bound := range latencyBounds {
		cumulative += m.latencyBuckets[i].Load()
		fmt.Fprintf(w, "risk_gateway_request_latency_ms_bucket{le=\"%d\"} %d\n", bound, cumulative)
	}
	fmt.Fprintf(w, "risk_gateway_request_latency_ms_bucket{le=\"+Inf\"} %d\n", m.completed.Load())
	fmt.Fprintf(w, "risk_gateway_request_latency_ms_sum %d\n", m.latencySumMS.Load())
	fmt.Fprintf(w, "risk_gateway_request_latency_ms_count %d\n", m.completed.Load())
}
