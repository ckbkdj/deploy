package core

import (
	"strings"
	"testing"
)

func TestExtractChatRequest(t *testing.T) {
	body := []byte(`{"model":"gpt-test","stream":true,"messages":[{"role":"system","content":"Be concise"},{"role":"user","content":[{"type":"text","text":"hello world"}]}]}`)
	got, err := ExtractOpenAIRequest(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.Model != "gpt-test" || !got.Stream {
		t.Fatalf("unexpected metadata: %#v", got)
	}
	if !strings.Contains(got.Text, "hello world") {
		t.Fatalf("text not extracted: %q", got.Text)
	}
}

func TestExtractResponsesRequest(t *testing.T) {
	body := []byte(`{"model":"gpt-test","instructions":"safe system","input":[{"role":"user","content":[{"type":"input_text","text":"inspect this log"}]}]}`)
	got, err := ExtractOpenAIRequest(body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Text, "inspect this log") || !strings.Contains(got.Text, "safe system") {
		t.Fatalf("unexpected text: %q", got.Text)
	}
}

func TestRewriteModel(t *testing.T) {
	body := []byte(`{"model":"public-name","messages":[]}`)
	got, model, changed := RewriteModel(body, map[string]string{"public-name": "provider-name"})
	if !changed || model != "provider-name" || !strings.Contains(string(got), "provider-name") {
		t.Fatalf("rewrite failed: changed=%v model=%q body=%s", changed, model, got)
	}
}
