package infra

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ckbkdj/newapi-risk-gateway/internal/core"
)

const (
	outboxBatchSize   = 250
	outboxLease       = 45 * time.Second
	outboxPoll        = 500 * time.Millisecond
	outboxMaxAttempts = 20
)

type TraceStore interface {
	InsertTrace(context.Context, core.RequestTrace) error
	RecordDelivery(context.Context, string, string, string, bool, string) error
	EnqueueOutbox(context.Context, core.OutboxEvent) (int64, error)
	ClaimOutbox(context.Context, string, int, time.Duration) ([]core.OutboxEvent, error)
	MarkOutboxDelivered(context.Context, int64) error
	MarkOutboxDeadlettered(context.Context, int64, string) error
	RescheduleOutbox(context.Context, int64, int, time.Duration, string) error
}

type queuedEvent struct {
	topic    string
	key      string
	typeName string
	data     any
	trace    *core.RequestTrace
}

type EventPipeline struct {
	store           TraceStore
	kafka           *Kafka
	auditTopic      string
	traceTopic      string
	deadLetterTopic string
	queue           chan queuedEvent
	workers         int
	outboxWorkers   int
	logger          *slog.Logger
	wg              sync.WaitGroup
	closed          atomic.Bool
	stop            chan struct{}
	stopOnce        sync.Once
	outboxWake      chan struct{}
	dropped         atomic.Uint64
	published       atomic.Uint64
	failed          atomic.Uint64
	deadlettered    atomic.Uint64
}

func NewEventPipeline(store TraceStore, kafka *Kafka, auditTopic, traceTopic, deadLetterTopic string, queueSize, workers int, logger *slog.Logger) *EventPipeline {
	if queueSize < 100 {
		queueSize = 100
	}
	if workers < 1 {
		workers = 1
	}
	outboxWorkers := workers / 2
	if outboxWorkers < 1 {
		outboxWorkers = 1
	}
	if outboxWorkers > 16 {
		outboxWorkers = 16
	}
	return &EventPipeline{
		store: store, kafka: kafka, auditTopic: auditTopic, traceTopic: traceTopic, deadLetterTopic: deadLetterTopic,
		queue: make(chan queuedEvent, queueSize), workers: workers, outboxWorkers: outboxWorkers, logger: logger,
		stop: make(chan struct{}), outboxWake: make(chan struct{}, outboxWorkers),
	}
}

func (p *EventPipeline) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	if p.kafka != nil && p.kafka.Enabled() {
		for i := 0; i < p.outboxWorkers; i++ {
			p.wg.Add(1)
			go p.outboxWorker(ctx)
		}
	}
}

func (p *EventPipeline) Stop() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	close(p.queue)
	p.stopOnce.Do(func() { close(p.stop) })
	p.wg.Wait()
}

func (p *EventPipeline) SubmitTrace(ctx context.Context, trace core.RequestTrace) error {
	if p.closed.Load() {
		return fmt.Errorf("event pipeline closed")
	}
	event := queuedEvent{topic: p.traceTopic, key: trace.RequestID, typeName: "risk.proxy.completed", data: trace, trace: &trace}
	select {
	case p.queue <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Preserve auditability under burst load: fall back to a bounded direct
		// write instead of silently dropping the trace.
		p.dropped.Add(1)
		fallback, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return p.process(fallback, event)
	}
}

func (p *EventPipeline) SubmitTracking(ctx context.Context, key string, data any) error {
	if p.closed.Load() {
		return fmt.Errorf("event pipeline closed")
	}
	event := queuedEvent{topic: p.traceTopic, key: key, typeName: "risk.tracking.received", data: data}
	select {
	case p.queue <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		p.dropped.Add(1)
		fallback, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return p.process(fallback, event)
	}
}

func (p *EventPipeline) SubmitAudit(ctx context.Context, key string, data any) error {
	if p.closed.Load() {
		return fmt.Errorf("event pipeline closed")
	}
	event := queuedEvent{topic: p.auditTopic, key: key, typeName: "risk.audit.decision", data: data}
	select {
	case p.queue <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		p.dropped.Add(1)
		fallback, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return p.process(fallback, event)
	}
}

func (p *EventPipeline) worker(id int) {
	defer p.wg.Done()
	for event := range p.queue {
		workCtx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		err := p.process(workCtx, event)
		cancel()
		if err != nil {
			p.logger.Error("event delivery failed", "worker", id, "type", event.typeName, "error", err)
		}
	}
}

func (p *EventPipeline) process(ctx context.Context, event queuedEvent) error {
	if event.trace != nil {
		if err := p.store.InsertTrace(ctx, *event.trace); err != nil {
			p.failed.Add(1)
			return fmt.Errorf("store trace: %w", err)
		}
	}
	data, err := json.Marshal(event.data)
	if err != nil {
		p.failed.Add(1)
		return err
	}
	envelope := core.EventEnvelope{Version: "1", Type: event.typeName, EventID: core.NewID("evt_"), Timestamp: time.Now().UTC(), Data: data}
	raw, err := json.Marshal(envelope)
	if err != nil {
		p.failed.Add(1)
		return err
	}
	if p.kafka == nil || !p.kafka.Enabled() {
		return nil
	}

	outbox := core.OutboxEvent{EventID: envelope.EventID, Topic: event.topic, Key: event.key, EventType: event.typeName, Payload: raw}
	if _, err := p.store.EnqueueOutbox(ctx, outbox); err != nil {
		p.failed.Add(1)
		return fmt.Errorf("enqueue durable Kafka outbox: %w", err)
	}
	// Publishing is deliberately decoupled from the ingestion workers. A Kafka
	// outage therefore cannot consume every trace worker or delay API requests;
	// SKIP LOCKED replay workers deliver the durable PostgreSQL outbox later.
	p.wakeOutbox()
	return nil

}

func (p *EventPipeline) outboxWorker(ctx context.Context) {
	defer p.wg.Done()
	workerID := core.NewID("outbox_")
	ticker := time.NewTicker(outboxPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stop:
			return
		case <-ticker.C:
		case <-p.outboxWake:
		}
		for {
			batchCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			events, err := p.store.ClaimOutbox(batchCtx, workerID, outboxBatchSize, outboxLease)
			cancel()
			if err != nil {
				p.logger.Error("claim Kafka outbox failed", "error", err)
				break
			}
			if len(events) == 0 {
				break
			}
			for _, event := range events {
				p.replayOutbox(event)
			}
			if len(events) < outboxBatchSize {
				break
			}
		}
	}
}

func (p *EventPipeline) replayOutbox(event core.OutboxEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	if err := p.kafka.Publish(ctx, event.Topic, event.Key, event.Payload); err == nil {
		if markErr := p.store.MarkOutboxDelivered(ctx, event.ID); markErr != nil {
			p.failed.Add(1)
			p.logger.Error("mark replayed outbox delivered failed", "event_id", event.EventID, "error", markErr)
			return
		}
		_ = p.store.RecordDelivery(ctx, event.EventID, event.EventType, "kafka", true, "")
		p.published.Add(1)
		return
	} else {
		attempts := event.Attempts + 1
		if attempts >= outboxMaxAttempts && p.deadLetterTopic != "" {
			dlq, marshalErr := json.Marshal(map[string]any{
				"version": "1", "type": "delivery.deadletter", "event_id": event.EventID,
				"original_topic": event.Topic, "original_type": event.EventType,
				"attempts": attempts, "last_error": err.Error(), "payload": json.RawMessage(event.Payload),
				"timestamp": time.Now().UTC(),
			})
			if marshalErr == nil {
				if dlqErr := p.kafka.Publish(ctx, p.deadLetterTopic, event.Key, dlq); dlqErr == nil {
					_ = p.store.MarkOutboxDeadlettered(ctx, event.ID, err.Error())
					_ = p.store.RecordDelivery(ctx, event.EventID, event.EventType, "deadletter", true, err.Error())
					p.deadlettered.Add(1)
					return
				}
			}
		}
		delay := retryDelay(attempts)
		if rescheduleErr := p.store.RescheduleOutbox(ctx, event.ID, attempts, delay, err.Error()); rescheduleErr != nil {
			p.logger.Error("reschedule Kafka outbox failed", "event_id", event.EventID, "error", rescheduleErr)
		}
		_ = p.store.RecordDelivery(ctx, event.EventID, event.EventType, "kafka", false, err.Error())
		p.failed.Add(1)
	}
}

func (p *EventPipeline) wakeOutbox() {
	select {
	case p.outboxWake <- struct{}{}:
	default:
	}
}

func retryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	seconds := math.Pow(2, float64(min(attempts, 8)))
	if seconds > 300 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}

func (p *EventPipeline) Stats() (published, failed, dropped, deadlettered uint64) {
	return p.published.Load(), p.failed.Load(), p.dropped.Load(), p.deadlettered.Load()
}
