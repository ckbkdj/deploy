package core

import (
	"regexp"
	"sort"
	"strings"
	"time"
)

type rule struct {
	ID       string
	Category string
	Weight   float64
	Pattern  *regexp.Regexp
}

// The deterministic layer is deliberately category-based rather than a raw
// keyword blacklist. It blocks clearly operational abuse while reducing false
// positives for defensive engineering, incident response, education and CTFs.
var cyberRules = []rule{
	{ID: "credential-theft", Category: "credential_theft", Weight: 5, Pattern: regexp.MustCompile(`(?i)(steal|harvest|dump|窃取|盗取|批量获取).{0,32}(password|credential|cookie|session|token|口令|密码|凭证)`)},
	{ID: "phishing-kit", Category: "credential_theft", Weight: 5, Pattern: regexp.MustCompile(`(?i)(phishing\s*(kit|page)|credential\s*harvester|钓鱼页面|钓鱼套件).{0,32}(deploy|host|bypass|部署|上线|绕过)?`)},
	{ID: "malware-build", Category: "malware", Weight: 5, Pattern: regexp.MustCompile(`(?i)(build|write|generate|制作|编写|生成).{0,40}(ransomware|keylogger|stealer|botnet|rootkit|勒索软件|键盘记录|木马|僵尸网络)`)},
	{ID: "malware-evasion", Category: "evasion", Weight: 4, Pattern: regexp.MustCompile(`(?i)(bypass|evade|disable|绕过|规避|关闭).{0,36}(edr|antivirus|defender|sandbox|detection|杀毒|安全软件|检测)`)},
	{ID: "unauthorized-exploit", Category: "unauthorized_exploitation", Weight: 5, Pattern: regexp.MustCompile(`(?i)(exploit|入侵|攻破|拿下).{0,48}(production|real\s*target|website|server|account|生产|真实目标|网站|服务器|账号)`)},
	{ID: "persistence-command-control", Category: "malware", Weight: 4, Pattern: regexp.MustCompile(`(?i)(persistence|command\s*and\s*control|c2\s*server|持久化|远控|命令控制).{0,40}(implant|agent|payload|植入|载荷|上线)`)},
	{ID: "destructive-action", Category: "destructive", Weight: 5, Pattern: regexp.MustCompile(`(?i)(wipe|destroy|encrypt|delete|破坏|清空|加密|删除).{0,40}(victim|remote|production|database|disk|files|受害者|远程|生产|数据库|磁盘|文件)`)},
	{ID: "ddos-botnet", Category: "botnet_ddos", Weight: 5, Pattern: regexp.MustCompile(`(?i)(ddos|denial.of.service|流量攻击|拒绝服务).{0,40}(script|botnet|amplification|attack|脚本|僵尸网络|放大|攻击)`)},
	{ID: "data-exfiltration", Category: "data_exfiltration", Weight: 5, Pattern: regexp.MustCompile(`(?i)(exfiltrate|steal|extract|窃取|外传|导出).{0,40}(customer\s*data|private\s*data|database|secrets|客户数据|隐私数据|数据库|密钥)`)},
	{ID: "weaponize-vulnerability", Category: "unauthorized_exploitation", Weight: 4, Pattern: regexp.MustCompile(`(?i)(weaponize|mass\s*scan|批量扫描|武器化).{0,48}(cve|vulnerability|漏洞|targets?|目标)`)},
}

var defensiveContext = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(defensive|defense|detect|detection|mitigat|prevent|patch|hardening|incident\s*response|forensic|authorized|permission|lab|sandbox|ctf|防御|检测|缓解|预防|修复|加固|应急响应|取证|已授权|实验室|靶场)`),
	regexp.MustCompile(`(?i)(explain|overview|high.level|risk\s*assessment|policy|audit|说明|概述|原理|风险评估|策略|审计)`),
}

func EvaluateCyberRules(text string) AuditResult {
	started := time.Now()
	normalized := strings.TrimSpace(text)
	if normalized == "" {
		return AuditResult{Decision: DecisionAllow, Score: 0, Reason: "empty or non-text request", Source: "rules", LatencyMS: time.Since(started).Milliseconds()}
	}

	score := 0.0
	categories := map[string]struct{}{}
	hits := make([]string, 0, 4)
	for _, r := range cyberRules {
		if r.Pattern.MatchString(normalized) {
			score += r.Weight
			categories[r.Category] = struct{}{}
			hits = append(hits, r.ID)
		}
	}

	defensive := false
	for _, p := range defensiveContext {
		if p.MatchString(normalized) {
			defensive = true
			score -= 2.5
			break
		}
	}
	if score < 0 {
		score = 0
	}

	cats := make([]string, 0, len(categories))
	for category := range categories {
		cats = append(cats, category)
	}
	sort.Strings(cats)

	decision := DecisionAllow
	reason := "no operational cyber-abuse rule matched"
	switch {
	case score >= 5:
		decision = DecisionBlock
		reason = "operational cyber-abuse pattern matched"
	case score >= 2.5:
		decision = DecisionReview
		reason = "potentially risky cyber request requires model review"
	case len(hits) > 0 && defensive:
		reason = "risky terminology found in defensive or authorized context"
	}

	return AuditResult{
		Decision:   decision,
		Score:      score,
		Categories: cats,
		Reason:     reason,
		Source:     "rules",
		RuleHits:   hits,
		LatencyMS:  time.Since(started).Milliseconds(),
	}
}
