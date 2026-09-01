package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

const defaultAuditTextLimit = 64 * 1024

type ExtractedRequest struct {
	Text   string
	Model  string
	Stream bool
}

// ExtractOpenAIRequest extracts text from OpenAI-compatible chat, responses,
// embeddings and legacy completion payloads without changing the original body.
func ExtractOpenAIRequest(body []byte) (ExtractedRequest, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return ExtractedRequest{}, nil
	}
	var root map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&root); err != nil {
		return ExtractedRequest{}, fmt.Errorf("decode request json: %w", err)
	}

	out := ExtractedRequest{}
	if v, ok := root["model"].(string); ok {
		out.Model = v
	}
	if v, ok := root["stream"].(bool); ok {
		out.Stream = v
	}

	parts := make([]string, 0, 16)
	appendValue := func(label string, value any) {
		text := flattenText(value, 0)
		if strings.TrimSpace(text) != "" {
			parts = append(parts, label+": "+text)
		}
	}

	for _, key := range []string{"instructions", "prompt", "input", "query"} {
		if value, ok := root[key]; ok {
			appendValue(key, value)
		}
	}
	if messages, ok := root["messages"].([]any); ok {
		for _, item := range messages {
			msg, ok := item.(map[string]any)
			if !ok {
				continue
			}
			role, _ := msg["role"].(string)
			if role == "" {
				role = "message"
			}
			appendValue(role, msg["content"])
		}
	}
	if len(parts) == 0 {
		appendValue("request", root)
	}
	out.Text = strings.Join(parts, "\n")
	if len(out.Text) > defaultAuditTextLimit {
		out.Text = out.Text[:defaultAuditTextLimit]
	}
	return out, nil
}

func flattenText(value any, depth int) string {
	if depth > 8 || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if text := flattenText(item, depth+1); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		// Prefer common content block fields and avoid base64/media blobs.
		for _, key := range []string{"text", "input_text", "output_text", "content", "value"} {
			if val, ok := v[key]; ok {
				if text := flattenText(val, depth+1); text != "" {
					return text
				}
			}
		}
		parts := make([]string, 0, len(v))
		for key, val := range v {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "image") || strings.Contains(lower, "audio") || strings.Contains(lower, "base64") || strings.Contains(lower, "url") {
				continue
			}
			if text := flattenText(val, depth+1); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func RewriteModel(body []byte, modelMap map[string]string) ([]byte, string, bool) {
	if len(modelMap) == 0 || len(bytes.TrimSpace(body)) == 0 {
		return body, "", false
	}
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		return body, "", false
	}
	model, _ := root["model"].(string)
	mapped, ok := modelMap[model]
	if !ok || mapped == "" || mapped == model {
		return body, model, false
	}
	root["model"] = mapped
	rewritten, err := json.Marshal(root)
	if err != nil {
		return body, model, false
	}
	return rewritten, mapped, true
}
