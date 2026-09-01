package audit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ckbkdj/newapi-risk-gateway/internal/core"
)

func TestModelClientClassify(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("authorization not forwarded")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": `{"decision":"block","score":0.91,"categories":["malware"],"reason":"operational malware request"}`}}},
		})
	}))
	defer server.Close()

	client := NewModelClient(2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := client.Classify(ctx, server.URL, "secret", "audit-small", "untrusted content")
	if err != nil {
		t.Fatal(err)
	}
	if got.Decision != core.DecisionBlock || got.Score != 0.91 || got.Source != "model" {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestModelClientRetriesWithoutResponseFormat(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if _, exists := payload["response_format"]; exists {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"response_format json_object unsupported"}}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": "```json\n{\"decision\":\"allow\",\"score\":0.02,\"categories\":[],\"reason\":\"benign\"}\n```"}}},
		})
	}))
	defer server.Close()

	got, err := NewModelClient(1).Classify(context.Background(), server.URL, "", "audit-small", "hello")
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || got.Decision != core.DecisionAllow {
		t.Fatalf("expected compatibility retry, calls=%d result=%#v", calls.Load(), got)
	}
}

func TestCleanJSON(t *testing.T) {
	got := cleanJSON("analysis before {\"decision\":\"review\"} after")
	if got != `{"decision":"review"}` {
		t.Fatalf("unexpected cleaned JSON: %q", got)
	}
}
