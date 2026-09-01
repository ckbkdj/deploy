package core

import (
	"bytes"
	"encoding/json"
	"strings"
)

var sensitiveKeys = map[string]struct{}{
	"authorization": {}, "api_key": {}, "apikey": {}, "token": {}, "access_token": {},
	"refresh_token": {}, "password": {}, "secret": {}, "cookie": {}, "set-cookie": {},
	"credit_card": {}, "card_number": {}, "cvv": {}, "private_key": {},
}

func RedactJSON(body []byte, maxBytes int64) json.RawMessage {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		// "redacted" must never silently persist arbitrary multipart, audio or
		// binary payloads. Keep only operational metadata; encrypted mode is the
		// explicit opt-in when full non-JSON payload retention is required.
		out, _ := json.Marshal(map[string]any{
			"sha256": SHA256Hex(body), "bytes": len(body), "non_json": true, "redacted": true,
		})
		return out
	}
	redactValue(value)
	out, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	if maxBytes > 0 && int64(len(out)) > maxBytes {
		out, _ = json.Marshal(map[string]any{
			"sha256":    SHA256Hex(body),
			"bytes":     len(body),
			"truncated": true,
		})
	}
	return out
}

func redactValue(value any) {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			lower := strings.ToLower(strings.TrimSpace(key))
			if _, ok := sensitiveKeys[lower]; ok || strings.Contains(lower, "password") || strings.Contains(lower, "secret") {
				v[key] = "[REDACTED]"
				continue
			}
			redactValue(item)
		}
	case []any:
		for _, item := range v {
			redactValue(item)
		}
	}
}
