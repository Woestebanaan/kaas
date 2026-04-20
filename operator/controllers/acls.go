package controllers

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// ACLFile is the in-memory representation of __cluster/acls.json.
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

// ACLResource mirrors the CRD's AclResource for JSON output.
type ACLResource struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	PatternType string `json:"patternType"`
}

func aclsPath(dataDir string) string {
	return filepath.Join(dataDir, "__cluster", "acls.json")
}

// reconcileACLs lists all KafkaAcl + KafkaUserGroup objects in namespace, merges them
// into a single ACLFile, and atomically writes it to dataDir/__cluster/acls.json.
// It is safe to call from multiple controllers concurrently — the atomic rename ensures
// the file is always complete.
func reconcileACLs(ctx context.Context, c client.Client, namespace, dataDir string) error {
	var aclList v1alpha1.KafkaAclList
	if err := c.List(ctx, &aclList, client.InNamespace(namespace)); err != nil {
		return err
	}

	var groupList v1alpha1.KafkaUserGroupList
	if err := c.List(ctx, &groupList, client.InNamespace(namespace)); err != nil {
		return err
	}

	var entries []ACLEntry

	// Entries from KafkaAcl objects.
	for _, acl := range aclList.Items {
		if acl.DeletionTimestamp != nil {
			continue
		}
		principal := formatPrincipal(acl.Spec.Principal)
		for _, rule := range acl.Spec.Rules {
			entries = append(entries, ruleToEntry(principal, rule))
		}
	}

	// Entries from KafkaUserGroup objects — expand each member individually.
	for _, group := range groupList.Items {
		if group.DeletionTimestamp != nil {
			continue
		}
		for _, member := range group.Spec.Members {
			for _, rule := range group.Spec.Rules {
				entries = append(entries, ruleToEntry("User:"+member, rule))
			}
		}
	}

	if err := os.MkdirAll(filepath.Join(dataDir, "__cluster"), 0755); err != nil {
		return err
	}
	return writeAtomic(aclsPath(dataDir), &ACLFile{Version: 1, ACLs: entries})
}

func formatPrincipal(p v1alpha1.AclPrincipal) string {
	switch p.Kind {
	case "KafkaUser":
		return "User:" + p.Name
	case "KafkaUserGroup":
		return "Group:" + p.Name
	default:
		return p.Name
	}
}

func ruleToEntry(principal string, rule v1alpha1.AclRule) ACLEntry {
	return ACLEntry{
		Principal: principal,
		Resource: ACLResource{
			Type:        rule.Resource.Type,
			Name:        rule.Resource.Name,
			PatternType: rule.Resource.PatternType,
		},
		Operations: rule.Operations,
		Permission: rule.Permission,
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
