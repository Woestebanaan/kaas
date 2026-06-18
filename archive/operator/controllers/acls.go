package controllers

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// ACLFile is the in-memory representation of __cluster/acls.json.
// Schema is deliberately UNCHANGED by gh #135 — the broker still reads
// this exact layout from disk. Only the CR-side authoring story changed:
// pre-gh #135 the operator merged a separate KafkaAcl CR + a
// KafkaUserGroup expansion into entries; post-gh #135 it iterates
// KafkaUser CRs and pulls the inline `spec.authorization.acls` slice.
// The on-disk format ("Allow"/"Deny" capitalisation, principal prefix)
// matches what the broker's loader already expects, so no broker-side
// code or data migration is needed.
type ACLFile struct {
	Version int        `json:"version"`
	ACLs    []ACLEntry `json:"acls"`
}

// ACLEntry is one access control rule as stored on disk.
type ACLEntry struct {
	Principal  string      `json:"principal"`
	Resource   ACLResource `json:"resource"`
	Operations []string    `json:"operations"`
	Permission string      `json:"permission"`
}

// ACLResource mirrors the in-CR resource shape for JSON output.
type ACLResource struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	PatternType string `json:"patternType"`
}

func aclsPath(dataDir string) string {
	return filepath.Join(dataDir, "__cluster", "acls.json")
}

// reconcileACLs lists all KafkaUser objects in namespace, collects their
// inline `spec.authorization.acls` rules, and atomically writes the
// merged set to dataDir/__cluster/acls.json.
//
// Called from KafkaUserReconciler on every KafkaUser change so a single
// user-level edit is reflected on disk within one reconcile cycle. Safe
// to call from concurrent reconciles — the atomic rename guarantees the
// broker only ever sees a complete file.
func reconcileACLs(ctx context.Context, c client.Client, namespace, dataDir string) error {
	var userList v1alpha1.KafkaUserList
	if err := c.List(ctx, &userList, client.InNamespace(namespace)); err != nil {
		return err
	}

	var entries []ACLEntry
	for _, u := range userList.Items {
		if u.DeletionTimestamp != nil {
			continue
		}
		if u.Spec.Authorization == nil || len(u.Spec.Authorization.ACLs) == 0 {
			continue
		}
		principal := "User:" + u.Name
		for _, acl := range u.Spec.Authorization.ACLs {
			entries = append(entries, aclToEntry(principal, acl))
		}
	}

	if err := os.MkdirAll(filepath.Join(dataDir, "__cluster"), 0o775); err != nil {
		return err
	}
	return writeAtomic(aclsPath(dataDir), &ACLFile{Version: 1, ACLs: entries})
}

// aclToEntry projects one KafkaUserACL (Strimzi-style, with
// `type: allow|deny` lowercased) onto the on-disk ACLEntry format
// (`permission: Allow|Deny` capitalised). Defaults the pattern type
// to "literal" when the CR didn't set one and the type to "allow"
// when omitted — both mirror Strimzi defaults.
func aclToEntry(principal string, acl v1alpha1.KafkaUserACL) ACLEntry {
	patternType := acl.Resource.PatternType
	if patternType == "" {
		patternType = "literal"
	}
	aclType := acl.Type
	if aclType == "" {
		aclType = "allow"
	}
	// On-disk format is the historical "Allow" / "Deny" capitalisation
	// (broker AclEngine matches case-sensitively). Translate at the
	// boundary so the public CR stays Strimzi-faithful.
	permission := "Allow"
	if strings.EqualFold(aclType, "deny") {
		permission = "Deny"
	}
	return ACLEntry{
		Principal: principal,
		Resource: ACLResource{
			Type:        acl.Resource.Type,
			Name:        acl.Resource.Name,
			PatternType: patternType,
		},
		Operations: acl.Operations,
		Permission: permission,
	}
}

// writeAtomic serialises v to JSON and writes it to path via a tmp+rename.
func writeAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0640); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
