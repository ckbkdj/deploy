package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type request struct {
	Model    string `json:"model"`
	Stream   bool   `json:"stream"`
	Messages []struct {
		Role    string `json:"role"`
		Content any    `json:"content"`
	} `json:"messages"`
	Input any `json:"input"`
}

func main() {
	addr := env("MOCK_LISTEN_ADDR", ":18081")
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("/v1/", handle)
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	log.Printf("mock upstream listening on %s", addr)
	log.Fatal(server.ListenAndServe())
}

func handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": map[string]any{"message": "method not allowed", "type": "invalid_request_error", "code": "method_not_allowed"}})
		return
	}
	var in request
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	if err := decoder.Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"message": "invalid JSON", "type": "invalid_request_error", "code": "invalid_json"}})
		return
	}
	text := requestText(in)
	switch {
	case in.Model == "missing-model" || strings.Contains(text, "trigger-provider-error"):
		writeJSON(w, http.StatusNotFound, map[string]any{"error": map[string]any{"message": "model not found", "type": "invalid_request_error", "code": "model_not_found"}})
		return
	case strings.Contains(text, "trigger-overload"):
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"message": "model overloaded", "type": "server_error", "code": "model_overloaded"}})
		return
	case strings.Contains(text, "trigger-structured-200-error"):
		writeJSON(w, http.StatusOK, map[string]any{"error": map[string]any{"message": "provider error", "type": "server_error", "code": "provider_error"}})
		return
	}
	if in.Stream || strings.Contains(text, "trigger-stream") {
		stream(w, strings.Contains(text, "trigger-stream-error"))
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/responses") {
		writeJSON(w, http.StatusOK, map[string]any{
			"id": "resp_mock", "object": "response", "status": "completed", "model": in.Model,
			"output": []any{map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "mock-ok"}}}},
			"usage":  map[string]any{"input_tokens": 3, "output_tokens": 2, "total_tokens": 5},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": "chatcmpl_mock", "object": "chat.completion", "model": in.Model,
		"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": "mock-ok"}, "finish_reason": "stop"}},
		"usage":   map[string]any{"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
	})
}

func stream(w http.ResponseWriter, withError bool) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	fmt.Fprint(w, "data: {\"id\":\"chatcmpl_mock\",\"choices\":[{\"delta\":{\"content\":\"mock\"}}]}\n\n")
	if flusher != nil {
		flusher.Flush()
	}
	if withError {
		fmt.Fprint(w, "event: error\n")
		fmt.Fprint(w, "data: {\"error\":{\"message\":\"model overloaded\",\"type\":\"server_error\",\"code\":\"model_overloaded\"}}\n\n")
	} else {
		fmt.Fprint(w, "data: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}
	if flusher != nil {
		flusher.Flush()
	}
}

func requestText(in request) string {
	var values []string
	for _, message := range in.Messages {
		switch value := message.Content.(type) {
		case string:
			values = append(values, value)
		default:
			raw, _ := json.Marshal(value)
			values = append(values, string(raw))
		}
	}
	if in.Input != nil {
		raw, _ := json.Marshal(in.Input)
		values = append(values, string(raw))
	}
	return strings.Join(values, "\n")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
