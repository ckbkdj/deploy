package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const RiskErrorCode = 555

type OpenAIErrorEnvelope struct {
	Error     OpenAIError `json:"error"`
	RequestID string      `json:"request_id,omitempty"`
	TraceID   string      `json:"trace_id,omitempty"`
}

type OpenAIError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Param   any    `json:"param"`
	Code    any    `json:"code"`
}

func RiskErrorBody(message, requestID, traceID string) []byte {
	body, _ := json.Marshal(OpenAIErrorEnvelope{
		Error: OpenAIError{
			Message: message,
			Type:    "risk_control_error",
			Param:   nil,
			Code:    RiskErrorCode,
		},
		RequestID: requestID,
		TraceID:   traceID,
	})
	return body
}

func ParseStatuses(csv string, fallback []int) []int {
	if strings.TrimSpace(csv) == "" {
		return append([]int(nil), fallback...)
	}
	var out []int
	seen := map[int]struct{}{}
	for _, part := range strings.Split(csv, ",") {
		value, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || value < 100 || value > 599 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return append([]int(nil), fallback...)
	}
	return out
}

func ShouldNormalizeUpstreamError(status int, body []byte, statuses []int, patterns []string) (bool, string) {
	for _, candidate := range statuses {
		if status == candidate {
			return true, fmt.Sprintf("upstream status %d matched normalization policy", status)
		}
	}
	text := strings.ToLower(string(bytes.TrimSpace(body)))
	for _, pattern := range patterns {
		pattern = strings.ToLower(strings.TrimSpace(pattern))
		if pattern != "" && strings.Contains(text, pattern) {
			return true, "upstream error pattern matched: " + pattern
		}
	}
	if status >= 400 && containsProviderError(body) {
		for _, marker := range []string{"model not found", "unsupported model", "model overloaded", "insufficient capacity", "upstream timeout", "provider error", "channel error", "模型不存在", "模型过载", "渠道错误"} {
			if strings.Contains(text, marker) {
				return true, "recognized provider/model error"
			}
		}
	}
	return false, ""
}

func IsStructuredError(body []byte) bool {
	return containsProviderError(body)
}

func containsProviderError(body []byte) bool {
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		return false
	}
	return mapLooksLikeError(root)
}

func mapLooksLikeError(root map[string]any) bool {
	if value, exists := root["error"]; exists && value != nil {
		switch typed := value.(type) {
		case string:
			if strings.TrimSpace(typed) != "" {
				return true
			}
		case map[string]any:
			if len(typed) > 0 {
				return true
			}
		default:
			return true
		}
	}
	typeName, _ := root["type"].(string)
	status, _ := root["status"].(string)
	if strings.Contains(strings.ToLower(typeName), "error") || strings.Contains(strings.ToLower(typeName), "failed") || strings.EqualFold(status, "failed") {
		return true
	}
	_, hasCode := root["code"]
	message, hasMessage := root["message"]
	if hasCode && hasMessage && strings.TrimSpace(fmt.Sprint(message)) != "" {
		return true
	}
	if response, ok := root["response"].(map[string]any); ok {
		return mapLooksLikeError(response)
	}
	return false
}

func NormalizeSSEDataLine(line []byte, requestID, traceID string, _ []string) ([]byte, bool) {
	trimmed := bytes.TrimSpace(line)
	if !bytes.HasPrefix(trimmed, []byte("data:")) {
		return line, false
	}
	data := bytes.TrimSpace(bytes.TrimPrefix(trimmed, []byte("data:")))
	if bytes.Equal(data, []byte("[DONE]")) || len(data) == 0 {
		return line, false
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil || !mapLooksLikeError(root) {
		return line, false
	}
	return append([]byte("data: "), RiskErrorBody("Upstream model/channel error normalized by risk control", requestID, traceID)...), true
}
