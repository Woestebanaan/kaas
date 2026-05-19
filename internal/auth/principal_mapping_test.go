package auth

import "testing"

// TestPrincipalMapper_DefaultFallback pins gh #43: with no rules
// configured (or only DEFAULT), Apply returns the supplied CN. This
// is the pre-fix behaviour every existing cluster relies on.
func TestPrincipalMapper_DefaultFallback(t *testing.T) {
	cases := []string{"", "DEFAULT"}
	for _, spec := range cases {
		m, err := NewPrincipalMapper(spec)
		if err != nil {
			t.Fatalf("spec=%q: %v", spec, err)
		}
		got := m.Apply("CN=alice,O=Acme", "alice")
		if got != "alice" {
			t.Errorf("spec=%q: got %q, want %q (fall-back to CN)", spec, got, "alice")
		}
	}
}

// TestPrincipalMapper_SimpleRule pins Apache's `RULE:` syntax: regex
// against the full subject, replacement with $1 back-references.
func TestPrincipalMapper_SimpleRule(t *testing.T) {
	m, err := NewPrincipalMapper(`RULE:^CN=(.*?),.*$/$1/`)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		subject, cn, want string
	}{
		{"CN=alice,O=Acme,L=NY", "alice", "alice"},
		{"CN=bob-mtls,OU=Engineering,O=Acme", "bob-mtls", "bob-mtls"},
		// Rule doesn't match (no comma after CN); falls back to cn.
		{"CN=onlycn", "onlycn", "onlycn"},
	}
	for _, tc := range cases {
		if got := m.Apply(tc.subject, tc.cn); got != tc.want {
			t.Errorf("subject=%q: got %q, want %q", tc.subject, got, tc.want)
		}
	}
}

// TestPrincipalMapper_CaseFlags pins the /L and /U postfix modifiers.
func TestPrincipalMapper_CaseFlags(t *testing.T) {
	mL, err := NewPrincipalMapper(`RULE:^CN=(.*?),.*$/$1/L`)
	if err != nil {
		t.Fatal(err)
	}
	if got := mL.Apply("CN=Alice,O=Acme", "Alice"); got != "alice" {
		t.Errorf("/L: got %q, want %q", got, "alice")
	}
	mU, err := NewPrincipalMapper(`RULE:^CN=(.*?),.*$/$1/U`)
	if err != nil {
		t.Fatal(err)
	}
	if got := mU.Apply("CN=Alice,O=Acme", "Alice"); got != "ALICE" {
		t.Errorf("/U: got %q, want %q", got, "ALICE")
	}
}

// TestPrincipalMapper_FirstMatchWins pins Apache's leftmost-rule
// precedence: rules are evaluated in declaration order; the first
// to match wins, even if later ones would also match.
func TestPrincipalMapper_FirstMatchWins(t *testing.T) {
	m, err := NewPrincipalMapper(
		`RULE:^CN=admin,.*$/ADMIN/,RULE:^CN=(.*?),.*$/$1/`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Apply("CN=admin,O=Acme", "admin"); got != "ADMIN" {
		t.Errorf("admin: got %q, want ADMIN (first rule must win)", got)
	}
	if got := m.Apply("CN=alice,O=Acme", "alice"); got != "alice" {
		t.Errorf("alice: got %q, want alice (second rule applies)", got)
	}
}

// TestPrincipalMapper_DefaultAfterRules pins the typical config
// shape: specific rules first, DEFAULT as the catch-all. A DN that
// matches none of the explicit rules falls through to DEFAULT and
// returns the CN.
func TestPrincipalMapper_DefaultAfterRules(t *testing.T) {
	m, err := NewPrincipalMapper(
		`RULE:^CN=(\w+),OU=Engineering,.*$/eng-$1/,DEFAULT`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Apply("CN=alice,OU=Engineering,O=Acme", "alice"); got != "eng-alice" {
		t.Errorf("matched-rule: got %q, want eng-alice", got)
	}
	if got := m.Apply("CN=bob,OU=Sales,O=Acme", "bob"); got != "bob" {
		t.Errorf("default-fallthrough: got %q, want bob", got)
	}
}

// TestPrincipalMapper_InvalidRule pins parse-error reporting. A
// malformed RULE: surface bubbles up so the chart's config validate
// can fail fast instead of silently dropping to "use CN".
func TestPrincipalMapper_InvalidRule(t *testing.T) {
	cases := []string{
		"BOGUS",
		"RULE:no-slashes-at-all",
		// Invalid Go regex (unbalanced bracket).
		`RULE:[unclosed/$1/`,
	}
	for _, spec := range cases {
		if _, err := NewPrincipalMapper(spec); err == nil {
			t.Errorf("spec=%q: expected error, got nil", spec)
		}
	}
}
