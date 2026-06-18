package auth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeACLFile(t *testing.T, dir string, entries []aclEntry) string {
	t.Helper()
	path := filepath.Join(dir, "acls.json")
	data, _ := json.Marshal(aclFile{Version: 1, ACLs: entries})
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func TestACLDenyOverridesAllow(t *testing.T) {
	path := writeACLFile(t, t.TempDir(), []aclEntry{
		{Principal: "User:alice", Resource: aclResource{Type: "topic", Name: "payments", PatternType: "literal"}, Operations: []string{"Read"}, Permission: "Allow"},
		{Principal: "User:alice", Resource: aclResource{Type: "topic", Name: "payments", PatternType: "literal"}, Operations: []string{"Read"}, Permission: "Deny"},
	})
	e := NewACLEngine(path)
	if err := e.Reload(); err != nil {
		t.Fatal(err)
	}
	if e.Authorize(Principal{Name: "alice", Kind: "User"}, Resource{Type: "topic", Name: "payments"}, OpRead) {
		t.Error("Deny should override Allow")
	}
}

func TestACLPrefixMatch(t *testing.T) {
	path := writeACLFile(t, t.TempDir(), []aclEntry{
		{Principal: "User:alice", Resource: aclResource{Type: "topic", Name: "payments-", PatternType: "prefix"}, Operations: []string{"Write"}, Permission: "Allow"},
	})
	e := NewACLEngine(path)
	_ = e.Reload()
	if !e.Authorize(Principal{Name: "alice", Kind: "User"}, Resource{Type: "topic", Name: "payments-events"}, OpWrite) {
		t.Error("prefix should match payments-events")
	}
	if e.Authorize(Principal{Name: "alice", Kind: "User"}, Resource{Type: "topic", Name: "other-topic"}, OpWrite) {
		t.Error("prefix should not match other-topic")
	}
}

func TestACLWildcardPrincipal(t *testing.T) {
	path := writeACLFile(t, t.TempDir(), []aclEntry{
		{Principal: "User:*", Resource: aclResource{Type: "topic", Name: "public", PatternType: "literal"}, Operations: []string{"Read"}, Permission: "Allow"},
	})
	e := NewACLEngine(path)
	_ = e.Reload()
	if !e.Authorize(Principal{Name: "anyone", Kind: "User"}, Resource{Type: "topic", Name: "public"}, OpRead) {
		t.Error("User:* should match any principal")
	}
}

func TestACLDefaultDeny(t *testing.T) {
	path := writeACLFile(t, t.TempDir(), []aclEntry{
		{Principal: "User:alice", Resource: aclResource{Type: "topic", Name: "other", PatternType: "literal"}, Operations: []string{"Read"}, Permission: "Allow"},
	})
	e := NewACLEngine(path)
	_ = e.Reload()
	if e.Authorize(Principal{Name: "alice", Kind: "User"}, Resource{Type: "topic", Name: "payments"}, OpRead) {
		t.Error("unmatched request should default to deny")
	}
}

func TestACLCacheTTL(t *testing.T) {
	path := writeACLFile(t, t.TempDir(), []aclEntry{
		{Principal: "User:alice", Resource: aclResource{Type: "topic", Name: "t", PatternType: "literal"}, Operations: []string{"Read"}, Permission: "Allow"},
	})
	e := NewACLEngine(path)
	_ = e.Reload()

	p := Principal{Name: "alice", Kind: "User"}
	r := Resource{Type: "topic", Name: "t"}

	if !e.Authorize(p, r, OpRead) {
		t.Error("first call should allow")
	}
	// Cached decision — modify rules without reload and verify cache still serves old answer.
	e.mu.Lock()
	e.rules[0].deny = true
	e.mu.Unlock()
	if !e.Authorize(p, r, OpRead) {
		t.Error("cached decision should still allow")
	}

	// Flush cache by sleeping past TTL.
	e.cacheMu.Lock()
	for k, v := range e.cache {
		v.expiresAt = time.Now().Add(-time.Second)
		e.cache[k] = v
	}
	e.cacheMu.Unlock()

	if e.Authorize(p, r, OpRead) {
		t.Error("after cache expires, updated rule should take effect")
	}
}

func TestACLReload(t *testing.T) {
	dir := t.TempDir()
	path := writeACLFile(t, dir, []aclEntry{
		{Principal: "User:alice", Resource: aclResource{Type: "topic", Name: "t", PatternType: "literal"}, Operations: []string{"Read"}, Permission: "Allow"},
	})
	e := NewACLEngine(path)
	_ = e.Reload()
	if !e.Authorize(Principal{Name: "alice", Kind: "User"}, Resource{Type: "topic", Name: "t"}, OpRead) {
		t.Error("initial Allow failed")
	}

	// Rewrite file with Deny instead.
	_ = writeACLFile(t, dir, []aclEntry{
		{Principal: "User:alice", Resource: aclResource{Type: "topic", Name: "t", PatternType: "literal"}, Operations: []string{"Read"}, Permission: "Deny"},
	})
	_ = e.Reload()
	if e.Authorize(Principal{Name: "alice", Kind: "User"}, Resource{Type: "topic", Name: "t"}, OpRead) {
		t.Error("after reload, Deny should apply")
	}
}
