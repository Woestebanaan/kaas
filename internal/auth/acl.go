package auth

import (
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// aclFile mirrors the JSON structure written by the operator.
type aclFile struct {
	Version int        `json:"version"`
	ACLs    []aclEntry `json:"acls"`
}

type aclEntry struct {
	Principal  string      `json:"principal"`
	Resource   aclResource `json:"resource"`
	Operations []string    `json:"operations"`
	Permission string      `json:"permission"`
}

type aclResource struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	PatternType string `json:"patternType"`
}

type aclRule struct {
	principal   string
	resType     string
	resName     string
	patternType string
	operations  map[Operation]bool
	deny        bool // true = Deny, false = Allow
}

type cacheKey struct {
	principal string
	resType   string
	resName   string
	op        Operation
}

type cachedDecision struct {
	allowed   bool
	expiresAt time.Time
}

// ACLEngine enforces access control based on acls.json.
// Deny takes precedence over Allow. Default is deny.
type ACLEngine struct {
	path string

	mu    sync.RWMutex
	rules []aclRule

	cacheMu sync.Mutex
	cache   map[cacheKey]cachedDecision
}

func NewACLEngine(path string) *ACLEngine {
	return &ACLEngine{path: path, cache: make(map[cacheKey]cachedDecision)}
}

// Reload reads acls.json and atomically replaces the rule set and flushes the cache.
func (e *ACLEngine) Reload() error {
	data, err := os.ReadFile(e.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var af aclFile
	if err := json.Unmarshal(data, &af); err != nil {
		return err
	}

	rules := make([]aclRule, 0, len(af.ACLs))
	for _, a := range af.ACLs {
		ops := make(map[Operation]bool, len(a.Operations))
		for _, op := range a.Operations {
			ops[Operation(op)] = true
		}
		rules = append(rules, aclRule{
			principal:   a.Principal,
			resType:     a.Resource.Type,
			resName:     a.Resource.Name,
			patternType: a.Resource.PatternType,
			operations:  ops,
			deny:        a.Permission == "Deny",
		})
	}

	e.mu.Lock()
	e.rules = rules
	e.mu.Unlock()

	e.cacheMu.Lock()
	e.cache = make(map[cacheKey]cachedDecision)
	e.cacheMu.Unlock()
	return nil
}

// Authorize checks whether principal may perform op on resource.
func (e *ACLEngine) Authorize(principal Principal, resource Resource, op Operation) bool {
	ck := cacheKey{
		principal: principal.Name,
		resType:   resource.Type,
		resName:   resource.Name,
		op:        op,
	}

	e.cacheMu.Lock()
	if cd, ok := e.cache[ck]; ok && time.Now().Before(cd.expiresAt) {
		e.cacheMu.Unlock()
		return cd.allowed
	}
	e.cacheMu.Unlock()

	allowed := e.evaluate(principal, resource, op)

	if !allowed {
		slog.Warn("acl: denied",
			"principal", principal.Name,
			"resource", resource.Type+"/"+resource.Name,
			"operation", string(op))
	}

	e.cacheMu.Lock()
	e.cache[ck] = cachedDecision{allowed: allowed, expiresAt: time.Now().Add(5 * time.Second)}
	e.cacheMu.Unlock()
	return allowed
}

func (e *ACLEngine) evaluate(principal Principal, resource Resource, op Operation) bool {
	e.mu.RLock()
	rules := e.rules
	e.mu.RUnlock()

	principalStr := principal.Kind + ":" + principal.Name

	allow := false
	for _, r := range rules {
		if !matchesPrincipal(r.principal, principalStr) {
			continue
		}
		if !matchesResource(r, resource) {
			continue
		}
		if !r.operations[op] && !r.operations["All"] {
			continue
		}
		if r.deny {
			return false // Deny takes precedence immediately.
		}
		allow = true
	}
	return allow
}

func matchesPrincipal(rulePrincipal, reqPrincipal string) bool {
	if rulePrincipal == "User:*" || rulePrincipal == "*" {
		return true
	}
	return rulePrincipal == reqPrincipal
}

func matchesResource(r aclRule, res Resource) bool {
	if r.resType != "*" && r.resType != res.Type {
		return false
	}
	switch r.patternType {
	case "literal":
		return r.resName == res.Name || r.resName == "*"
	case "prefix":
		return strings.HasPrefix(res.Name, r.resName)
	case "any":
		return true
	case "match":
		// Treat match as prefix for Phase 7.
		return strings.HasPrefix(res.Name, r.resName)
	}
	return r.resName == res.Name
}
