package auth

import (
	"fmt"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
)

// RealAuthEngine is the production AuthEngine backed by credentials.json and acls.json.
type RealAuthEngine struct {
	creds  *CredentialLoader
	acls   *ACLEngine
	quotas *QuotaEnforcer
	k8s    kubernetes.Interface // nil if SA auth is disabled
}

// NewRealAuthEngine creates an engine reading from dataDir/__cluster/.
// k8s may be nil — ServiceAccount JWT auth will be unavailable in that case.
func NewRealAuthEngine(dataDir string, k8s kubernetes.Interface) (*RealAuthEngine, error) {
	clusterDir := filepath.Join(dataDir, "__cluster")
	creds := NewCredentialLoader(filepath.Join(clusterDir, "credentials.json"))
	aclEng := NewACLEngine(filepath.Join(clusterDir, "acls.json"))

	if err := creds.Reload(); err != nil {
		return nil, fmt.Errorf("auth: load credentials: %w", err)
	}
	if err := aclEng.Reload(); err != nil {
		return nil, fmt.Errorf("auth: load acls: %w", err)
	}

	return &RealAuthEngine{
		creds:  creds,
		acls:   aclEng,
		quotas: NewQuotaEnforcer(creds),
		k8s:    k8s,
	}, nil
}

// NewSASLExchange returns a fresh exchange for the given SASL mechanism.
func (e *RealAuthEngine) NewSASLExchange(mechanism string) (SASLExchange, error) {
	switch mechanism {
	case "SCRAM-SHA-512":
		return NewScramExchange(e.creds), nil
	case "PLAIN":
		if e.k8s == nil {
			return nil, fmt.Errorf("PLAIN mechanism requires kubernetes client (SA JWT auth)")
		}
		return NewSAExchange(e.k8s, e.creds), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %q", mechanism)
	}
}

// Authorize delegates to the ACL engine.
func (e *RealAuthEngine) Authorize(principal Principal, resource Resource, op Operation) bool {
	return e.acls.Authorize(principal, resource, op)
}

// Reload hot-reloads credentials and ACLs from disk.
// Called by the ClusterFileWatcher on inotify events.
func (e *RealAuthEngine) Reload() {
	_ = e.creds.Reload()
	_ = e.acls.Reload()
}

// Quotas returns the quota enforcer for use by produce/fetch handlers.
func (e *RealAuthEngine) Quotas() *QuotaEnforcer { return e.quotas }

// CheckProduceQuota delegates to the quota enforcer.
func (e *RealAuthEngine) CheckProduceQuota(p Principal, bytes int) int32 {
	return e.quotas.CheckProduce(p, bytes)
}

// CheckFetchQuota delegates to the quota enforcer.
func (e *RealAuthEngine) CheckFetchQuota(p Principal, bytes int) int32 {
	return e.quotas.CheckFetch(p, bytes)
}
