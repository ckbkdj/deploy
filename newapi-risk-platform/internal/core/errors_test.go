package core

import (
	"encoding/json"
	"testing"
)

func TestNormalizeByStatus(t *testing.T) {
	ok, _ := ShouldNormalizeUpstreamError(429, []byte(`{"error":{"message":"rate limit"}}`), []int{429, 503}, nil)
	if !ok {
		t.Fatal("expected normalization")
	}
}

func TestNormalizeByPattern(t *testing.T) {
	ok, _ := ShouldNormalizeUpstreamError(400, []byte(`{"error":{"message":"model not found"}}`), nil, []string{"model not found"})
	if !ok {
		t.Fatal("expected pattern normalization")
	}
}

func TestRiskBodyShape(t *testing.T) {
	var got OpenAIErrorEnvelope
	if err := json.Unmarshal(RiskErrorBody("blocked", "req_1", "tr_1"), &got); err != nil {
		t.Fatal(err)
	}
	if got.Error.Code != float64(555) && got.Error.Code != 555 {
		t.Fatalf("unexpected code: %#v", got.Error.Code)
	}
}

func TestNormalizeSSE(t *testing.T) {
	line := []byte(`data: {"error":{"message":"upstream failed"}}`)
	got, changed := NormalizeSSEDataLine(line, "req", "trace", nil)
	if !changed || len(got) == 0 {
		t.Fatalf("expected normalized line: %s", got)
	}
}

func TestStructuredErrorIgnoresNull(t *testing.T) {
	if IsStructuredError([]byte(`{"error":null,"choices":[]}`)) {
		t.Fatal("null error must not be treated as a provider failure")
	}
}

func TestStructuredResponsesFailure(t *testing.T) {
	body := []byte(`{"type":"response.failed","response":{"status":"failed","error":{"message":"model overloaded"}}}`)
	if !IsStructuredError(body) {
		t.Fatal("response.failed should be detected")
	}
	line := append([]byte("data: "), body...)
	if _, changed := NormalizeSSEDataLine(line, "req", "trace", nil); !changed {
		t.Fatal("response.failed SSE event should be normalized")
	}
}

func TestParseStatusesDeduplicates(t *testing.T) {
	got := ParseStatuses("429, 503,429,invalid", nil)
	if len(got) != 2 || got[0] != 429 || got[1] != 503 {
		t.Fatalf("unexpected statuses: %#v", got)
	}
}
