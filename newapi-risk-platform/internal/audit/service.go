package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ckbkdj/newapi-risk-gateway/internal/core"
)

type Cache interface {
	GetAudit(context.Context, string) (core.AuditResult, bool, error)
	SetAudit(context.Context, string, core.AuditResult, time.Duration) error
}

type Service struct {
	model  *ModelClient
	cache  Cache
	logger *slog.Logger
}

func NewService(model *ModelClient, cache Cache, logger *slog.Logger) *Service {
	return &Service{model: model, cache: cache, logger: logger}
}

func (s *Service) Audit(ctx context.Context, text string, settings core.RuntimeSettings) (core.AuditResult, error) {
	started := time.Now()
	ruleResult := core.EvaluateCyberRules(text)
	if ruleResult.Decision == core.DecisionBlock {
		ruleResult.LatencyMS = time.Since(started).Milliseconds()
		return ruleResult, nil
	}
	if !settings.ModelAuditEnabled || strings.TrimSpace(text) == "" {
		ruleResult.LatencyMS = time.Since(started).Milliseconds()
		return ruleResult, nil
	}

	cacheKey := core.SHA256Hex([]byte(strings.Join([]string{
		"v1", settings.AuditModelURL, settings.AuditModelName,
		fmt.Sprintf("%.3f", settings.BlockThreshold), text,
	}, "\x00")))
	if s.cache != nil {
		if cached, ok, err := s.cache.GetAudit(ctx, cacheKey); err == nil && ok {
			cached.LatencyMS = time.Since(started).Milliseconds()
			return mergeRuleContext(cached, ruleResult), nil
		} else if err != nil {
			s.logger.Warn("audit cache read failed", "error", err)
		}
	}

	timeout := time.Duration(settings.AuditTimeoutMS) * time.Millisecond
	modelCtx, cancel := context.WithTimeout(ctx, timeout)
	modelResult, err := s.model.Classify(modelCtx, settings.AuditModelURL, settings.AuditModelAPIKey, settings.AuditModelName, text)
	cancel()
	if err != nil {
		s.logger.Warn("audit model unavailable", "error", err, "fail_mode", settings.AuditFailMode)
		result := applyFailMode(settings.AuditFailMode, ruleResult, err)
		result.LatencyMS = time.Since(started).Milliseconds()
		return result, err
	}

	modelResult = applyThresholds(modelResult, settings)
	modelResult = mergeRuleContext(modelResult, ruleResult)
	modelResult.LatencyMS = time.Since(started).Milliseconds()
	if s.cache != nil {
		cacheCtx, cacheCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		if err := s.cache.SetAudit(cacheCtx, cacheKey, modelResult, 10*time.Minute); err != nil {
			s.logger.Warn("audit cache write failed", "error", err)
		}
		cacheCancel()
	}
	return modelResult, nil
}

func applyThresholds(result core.AuditResult, settings core.RuntimeSettings) core.AuditResult {
	if result.Decision == core.DecisionBlock && result.Score < settings.BlockThreshold {
		result.Decision = core.DecisionReview
		result.Reason += " (below block threshold)"
	}
	if result.Decision == core.DecisionReview && result.Score < settings.ReviewThreshold {
		result.Decision = core.DecisionAllow
		result.Reason += " (below review threshold)"
	}
	if result.Score >= settings.BlockThreshold && containsBlockedCategory(result.Categories) {
		result.Decision = core.DecisionBlock
	}
	return result
}

func containsBlockedCategory(categories []string) bool {
	for _, category := range categories {
		switch strings.ToLower(category) {
		case "credential_theft", "phishing", "malware", "evasion", "unauthorized_exploitation", "destructive", "botnet_ddos", "data_exfiltration":
			return true
		}
	}
	return false
}

func mergeRuleContext(model, rules core.AuditResult) core.AuditResult {
	if len(rules.RuleHits) > 0 {
		model.RuleHits = append(model.RuleHits, rules.RuleHits...)
		model.Categories = uniqueStrings(append(model.Categories, rules.Categories...))
		if rules.Decision == core.DecisionReview && model.Decision == core.DecisionAllow {
			model.Decision = core.DecisionReview
			model.Reason += "; deterministic rules requested review"
		}
	}
	return model
}

func applyFailMode(mode string, rules core.AuditResult, err error) core.AuditResult {
	switch mode {
	case "block":
		return core.AuditResult{Decision: core.DecisionBlock, Score: 1, Categories: []string{"audit_unavailable"}, Reason: "audit model unavailable and fail-closed is enabled", Source: "model_fail_closed", RuleHits: rules.RuleHits}
	case "allow":
		return core.AuditResult{Decision: core.DecisionAllow, Score: rules.Score, Categories: rules.Categories, Reason: "audit model unavailable and fail-open is enabled", Source: "model_fail_open", RuleHits: rules.RuleHits}
	default:
		rules.Source = "rules_fallback"
		rules.Reason += "; audit model unavailable: " + sanitizeError(err)
		return rules
	}
}

func sanitizeError(err error) string {
	if err == nil {
		return "unknown"
	}
	raw, _ := json.Marshal(err.Error())
	value := string(raw)
	if len(value) > 220 {
		value = value[:220]
	}
	return value
}
func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
