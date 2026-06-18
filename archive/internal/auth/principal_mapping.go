package auth

import (
	"fmt"
	"regexp"
	"strings"
)

// PrincipalMapper extracts the principal name from an X.509 subject
// DN (or, in the future, SAN URI / SAN DNS) per Apache Kafka's
// `ssl.principal.mapping.rules` config (gh #43, KIP-371). Each rule
// is one of:
//
//   - "DEFAULT"     — return the subject's CommonName unchanged.
//   - "RULE:<regex>/<replacement>[/L|/U]" — match the full DN against
//     <regex>; on success, format the principal as <replacement>
//     (with $1, $2, ... back-references). Optional /L lowercases,
//     /U uppercases the result. First matching rule wins; if none
//     match, the next rule is tried in declaration order.
//
// Skafka mirrors Apache's first-match-wins, leftmost-rule precedence.
// If the rule list is empty (or no rule matches), the principal name
// defaults to the CN (pre-gh #43 behaviour, preserved as a back-stop
// so an empty config keeps clusters working unchanged).
type PrincipalMapper struct {
	rules []principalRule
}

type principalRule struct {
	regex       *regexp.Regexp
	replacement string
	caseFlag    byte // 'L', 'U', or 0
	isDefault   bool
}

// NewPrincipalMapper compiles a rule string in Apache's format.
// Rules are separated by commas. Empty input returns a mapper whose
// Apply() always returns the CN unchanged.
//
// Example: "RULE:^CN=(.*?),.*$/$1/L,DEFAULT"
//
// On parse error (invalid regex, malformed RULE syntax), returns the
// error so callers can surface it at chart-config-validate time
// rather than silently falling back to "use CN".
func NewPrincipalMapper(spec string) (*PrincipalMapper, error) {
	m := &PrincipalMapper{}
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return m, nil
	}
	for _, raw := range splitRules(spec) {
		r, err := parsePrincipalRule(raw)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", raw, err)
		}
		m.rules = append(m.rules, r)
	}
	return m, nil
}

// Apply returns the mapped principal name for an X.509 subject. The
// subject input is the full RFC 2253 DN string (e.g.
// "CN=alice,OU=Engineering,O=Acme"). cn is the convenience-extracted
// CommonName the caller already has — used as the fall-back when no
// rule matches.
func (m *PrincipalMapper) Apply(subject, cn string) string {
	if m == nil {
		return cn
	}
	for _, r := range m.rules {
		if r.isDefault {
			return cn
		}
		match := r.regex.FindStringSubmatchIndex(subject)
		if match == nil {
			continue
		}
		out := string(r.regex.ExpandString(nil, r.replacement, subject, match))
		switch r.caseFlag {
		case 'L':
			out = strings.ToLower(out)
		case 'U':
			out = strings.ToUpper(out)
		}
		return out
	}
	return cn
}

// splitRules splits the rule spec at rule boundaries. Rules are
// separated by commas, but commas can appear inside a RULE:'s regex
// body (Subject DNs use them as a separator). We split only at
// commas immediately followed by "RULE:" or "DEFAULT" so the regex
// body's internal commas stay attached to the previous rule.
//
// Apache uses the same boundary heuristic — its `SslPrincipalMapper`
// parses with a "rule starts with RULE:" prefix scan.
func splitRules(spec string) []string {
	var out []string
	i := 0
	for i < len(spec) {
		// Find the start of the next rule boundary: a comma followed
		// (after optional whitespace) by RULE: or DEFAULT.
		j := i
		for j < len(spec) {
			if spec[j] == ',' {
				k := j + 1
				for k < len(spec) && (spec[k] == ' ' || spec[k] == '\t') {
					k++
				}
				if strings.HasPrefix(spec[k:], "RULE:") || strings.HasPrefix(spec[k:], "DEFAULT") {
					break
				}
			}
			j++
		}
		out = append(out, strings.TrimSpace(spec[i:j]))
		if j < len(spec) {
			j++ // consume the comma
		}
		i = j
	}
	return out
}

// parsePrincipalRule parses one RULE: or DEFAULT entry. Apache's
// canonical format is:
//
//	RULE:<regex>/<replacement>[/L|/U]
//
// where the trailing /L|/U flag is optional and there is always a
// terminating "/" after the replacement (even when no flag follows).
// Equivalently: the rule body always ends with "/" or "/L" or "/U".
// Regexes containing literal slashes are not supported — Subject DNs
// never carry them; this matches Apache's behaviour.
func parsePrincipalRule(raw string) (principalRule, error) {
	if raw == "DEFAULT" {
		return principalRule{isDefault: true}, nil
	}
	if !strings.HasPrefix(raw, "RULE:") {
		return principalRule{}, fmt.Errorf("must start with RULE: or be DEFAULT")
	}
	body := raw[len("RULE:"):]

	// Apache's body always ends with `/`, `/L`, or `/U`. Trim
	// whichever suffix is present so the remaining `body` is the
	// raw `<regex>/<replacement>` pair.
	caseFlag := byte(0)
	switch {
	case strings.HasSuffix(body, "/L"):
		caseFlag = 'L'
		body = body[:len(body)-2]
	case strings.HasSuffix(body, "/U"):
		caseFlag = 'U'
		body = body[:len(body)-2]
	case strings.HasSuffix(body, "/"):
		body = body[:len(body)-1]
	default:
		return principalRule{}, fmt.Errorf("rule must end with '/', '/L', or '/U'")
	}

	// Split the remainder at the FIRST "/". DNs don't carry slashes,
	// so this is unambiguous in practice (matches Apache's behaviour).
	slash := strings.IndexByte(body, '/')
	if slash < 0 {
		return principalRule{}, fmt.Errorf("missing '/' separator between regex and replacement")
	}
	pattern := body[:slash]
	replacement := body[slash+1:]
	re, err := regexp.Compile(pattern)
	if err != nil {
		return principalRule{}, fmt.Errorf("compile regex: %w", err)
	}
	return principalRule{
		regex:       re,
		replacement: replacement,
		caseFlag:    caseFlag,
	}, nil
}
