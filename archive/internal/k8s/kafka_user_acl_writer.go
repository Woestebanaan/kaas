package k8s

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// KafkaUserACLWriter implements handlers.ACLCRWriter against the
// inline KafkaUser.spec.authorization.acls list (gh #107 + gh #135).
// The pre-gh #135 KafkaACL CR is gone; every ACL row is now authored
// on the principal's own KafkaUser CR. CreateAcls / DeleteAcls /
// DescribeAcls mutate that inline list; the operator's reconcileACLs
// rebuilds /data/__cluster/acls.json from the merged set, and every
// broker's AclEngine hot-reloads.
//
// Last-write-wins under concurrent edits: a `kafka-acls.sh --add`
// racing a `kubectl edit kafkauser/alice` follows controller-runtime's
// Update semantics (resource-version check + retry-on-conflict at the
// apiserver). The handler doesn't loop on its own — one shot, return
// error → wire-level UNKNOWN_SERVER_ERROR → AdminClient retry.
//
// ArgoCD drift: like gh #103/#104, mutating spec.authorization.acls
// on a git-managed KafkaUser will surface as drift in ArgoCD until the
// next git sync. That's the intentional trade-off for letting the
// admin protocol reach the canonical store; operators who don't want
// drift should stick to the CR / GitOps path.
type KafkaUserACLWriter struct {
	client    client.Client
	namespace string
}

// NewKafkaUserACLWriter binds a writer to the controller-runtime client
// and the namespace where KafkaUser CRs live (typically `skafka`,
// matching the broker's pod namespace).
func NewKafkaUserACLWriter(c client.Client, namespace string) *KafkaUserACLWriter {
	return &KafkaUserACLWriter{client: c, namespace: namespace}
}

// CreateACL appends one ACL row to spec.authorization.acls on the
// KafkaUser CR named by binding.Principal (parsed as "User:<name>").
//
// Idempotent: if an entry already exists with the same Resource +
// PatternType + Type + Host, the operation is folded into that entry's
// Operations slice (or skipped if already present). This matches
// AdminClient's contract that creating the same ACL twice is a no-op,
// and avoids accumulating phantom duplicate rows under retries.
//
// Returns handlers.ErrInvalidPrincipal if the principal isn't
// "User:<name>". Returns handlers.ErrUnknownPrincipal if no KafkaUser
// CR exists with that name — skafka does not auto-create CRs on a
// runtime ACL write (same model as gh #103/#104).
func (w *KafkaUserACLWriter) CreateACL(ctx context.Context, b handlers.ACLBinding) error {
	username, err := principalToUserName(b.Principal)
	if err != nil {
		return err
	}

	var u v1alpha1.KafkaUser
	if err := observability.RecordK8sCall(ctx, "Get", "KafkaUser", func() error {
		return w.client.Get(ctx, types.NamespacedName{Namespace: w.namespace, Name: username}, &u)
	}); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: %s", handlers.ErrUnknownPrincipal, b.Principal)
		}
		return fmt.Errorf("get KafkaUser %s/%s: %w", w.namespace, username, err)
	}

	if u.Spec.Authorization == nil {
		u.Spec.Authorization = &v1alpha1.KafkaUserAuthorization{Type: "simple"}
	}
	if u.Spec.Authorization.Type == "" {
		u.Spec.Authorization.Type = "simple"
	}

	patternType := b.PatternType
	if patternType == "" {
		patternType = "literal"
	}
	aclType := strings.ToLower(b.Permission)
	if aclType == "" {
		aclType = "allow"
	}

	// Look for an existing entry with the same resource shape we can
	// fold this operation into. Matching is by (resource.{type,name,
	// patternType}, type, host) — Operations is intentionally NOT part
	// of the match key so we coalesce reads/writes onto a single entry.
	for i, e := range u.Spec.Authorization.ACLs {
		if !sameACLShape(e, b, patternType, aclType) {
			continue
		}
		for _, op := range e.Operations {
			if op == b.Operation {
				return nil
			}
		}
		u.Spec.Authorization.ACLs[i].Operations = append(e.Operations, b.Operation)
		return w.update(ctx, &u)
	}

	u.Spec.Authorization.ACLs = append(u.Spec.Authorization.ACLs, v1alpha1.KafkaUserACL{
		Resource: v1alpha1.KafkaUserACLResource{
			Type:        b.ResourceType,
			Name:        b.ResourceName,
			PatternType: patternType,
		},
		Operations: []string{b.Operation},
		Type:       aclType,
		Host:       b.Host,
	})
	return w.update(ctx, &u)
}

// DeleteACLs walks every KafkaUser CR in the namespace and removes
// entries (or specific operations within an entry) that match the
// filter. Returns the flat list of removed bindings — one
// handlers.ACLBinding per (entry, operation) pair the filter touched.
//
// Partial-op semantics: when an entry has Operations=[Read, Write] and
// the filter matches Read only, the entry is updated to [Write] (not
// deleted). This mirrors AdminClient's per-operation granularity.
//
// CRs whose Authorization is unchanged after filtering are not patched
// (no apiserver write, no spurious resourceVersion bump).
func (w *KafkaUserACLWriter) DeleteACLs(ctx context.Context, f handlers.ACLFilter) ([]handlers.ACLBinding, error) {
	users, err := w.listUsers(ctx)
	if err != nil {
		return nil, err
	}

	var removed []handlers.ACLBinding
	for i := range users {
		u := &users[i]
		if u.DeletionTimestamp != nil {
			continue
		}
		if u.Spec.Authorization == nil || len(u.Spec.Authorization.ACLs) == 0 {
			continue
		}
		principal := "User:" + u.Name
		if !matchString(f.Principal, principal) {
			continue
		}

		newACLs := make([]v1alpha1.KafkaUserACL, 0, len(u.Spec.Authorization.ACLs))
		mutated := false
		for _, e := range u.Spec.Authorization.ACLs {
			matched, keptOps := splitOpsByFilter(e, f, principal)
			for _, op := range matched {
				removed = append(removed, aclEntryToBinding(principal, e, op))
				mutated = true
			}
			if len(matched) == 0 {
				newACLs = append(newACLs, e)
				continue
			}
			if len(keptOps) == 0 {
				continue
			}
			updated := e
			updated.Operations = keptOps
			newACLs = append(newACLs, updated)
		}
		if !mutated {
			continue
		}
		u.Spec.Authorization.ACLs = newACLs
		if err := w.update(ctx, u); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

// ListACLs walks every KafkaUser CR in the namespace, expands each
// inline ACL entry into one binding per operation, and returns those
// that match the filter. Read-only — does not mutate any CR.
func (w *KafkaUserACLWriter) ListACLs(ctx context.Context, f handlers.ACLFilter) ([]handlers.ACLBinding, error) {
	users, err := w.listUsers(ctx)
	if err != nil {
		return nil, err
	}

	var out []handlers.ACLBinding
	for _, u := range users {
		if u.DeletionTimestamp != nil {
			continue
		}
		if u.Spec.Authorization == nil {
			continue
		}
		principal := "User:" + u.Name
		if !matchString(f.Principal, principal) {
			continue
		}
		for _, e := range u.Spec.Authorization.ACLs {
			for _, op := range e.Operations {
				b := aclEntryToBinding(principal, e, op)
				if matchBinding(f, b) {
					out = append(out, b)
				}
			}
		}
	}
	return out, nil
}

func (w *KafkaUserACLWriter) listUsers(ctx context.Context) ([]v1alpha1.KafkaUser, error) {
	var list v1alpha1.KafkaUserList
	if err := observability.RecordK8sCall(ctx, "List", "KafkaUser", func() error {
		return w.client.List(ctx, &list, client.InNamespace(w.namespace))
	}); err != nil {
		return nil, fmt.Errorf("list KafkaUser in %s: %w", w.namespace, err)
	}
	return list.Items, nil
}

func (w *KafkaUserACLWriter) update(ctx context.Context, u *v1alpha1.KafkaUser) error {
	if err := observability.RecordK8sCall(ctx, "Update", "KafkaUser", func() error {
		return w.client.Update(ctx, u)
	}); err != nil {
		return fmt.Errorf("update KafkaUser %s/%s: %w", w.namespace, u.Name, err)
	}
	return nil
}

// principalToUserName parses "User:alice" → "alice". skafka maps ACLs
// to a KafkaUser CR keyed on the bare username; non-User principals
// (Group:, ServiceAccount:) are rejected because there's nowhere to
// store them in the current CR model. The character set is otherwise
// passed through unchanged — RFC-1123 validation happens on apiserver
// Update, not here.
func principalToUserName(principal string) (string, error) {
	const prefix = "User:"
	if !strings.HasPrefix(principal, prefix) || len(principal) == len(prefix) {
		return "", fmt.Errorf("%w: %q", handlers.ErrInvalidPrincipal, principal)
	}
	return principal[len(prefix):], nil
}

// sameACLShape returns true when entry e is the same resource + type +
// host as binding b (with the resolved patternType / aclType defaults
// already applied to b). Used by CreateACL's idempotent fold.
func sameACLShape(e v1alpha1.KafkaUserACL, b handlers.ACLBinding, patternType, aclType string) bool {
	if e.Resource.Type != b.ResourceType {
		return false
	}
	if e.Resource.Name != b.ResourceName {
		return false
	}
	entryPattern := e.Resource.PatternType
	if entryPattern == "" {
		entryPattern = "literal"
	}
	if entryPattern != patternType {
		return false
	}
	entryType := strings.ToLower(e.Type)
	if entryType == "" {
		entryType = "allow"
	}
	if entryType != aclType {
		return false
	}
	return e.Host == b.Host
}

// splitOpsByFilter partitions the entry's Operations into (matched,
// kept). The match check fans out across the entry's other axes too —
// resource, pattern, permission, host — so an entry that doesn't match
// at all returns ([], entry.Operations).
func splitOpsByFilter(e v1alpha1.KafkaUserACL, f handlers.ACLFilter, principal string) (matched, kept []string) {
	if !matchString(f.Principal, principal) {
		return nil, e.Operations
	}
	if !matchString(f.ResourceType, e.Resource.Type) {
		return nil, e.Operations
	}
	if !matchString(f.ResourceName, e.Resource.Name) {
		return nil, e.Operations
	}
	entryPattern := e.Resource.PatternType
	if entryPattern == "" {
		entryPattern = "literal"
	}
	if !matchPattern(f.PatternType, entryPattern) {
		return nil, e.Operations
	}
	entryPerm := normalisePermission(e.Type)
	if !matchString(f.Permission, entryPerm) {
		return nil, e.Operations
	}
	if !matchString(f.Host, e.Host) {
		return nil, e.Operations
	}

	for _, op := range e.Operations {
		if f.Operation == "" || op == f.Operation {
			matched = append(matched, op)
		} else {
			kept = append(kept, op)
		}
	}
	return matched, kept
}

// aclEntryToBinding projects one (entry, operation) pair onto the
// flat handlers.ACLBinding shape. Defaults are applied so the result
// is wire-clean even if the CR omitted optional fields.
func aclEntryToBinding(principal string, e v1alpha1.KafkaUserACL, op string) handlers.ACLBinding {
	pattern := e.Resource.PatternType
	if pattern == "" {
		pattern = "literal"
	}
	return handlers.ACLBinding{
		Principal:    principal,
		ResourceType: e.Resource.Type,
		ResourceName: e.Resource.Name,
		PatternType:  pattern,
		Operation:    op,
		Permission:   normalisePermission(e.Type),
		Host:         e.Host,
	}
}

// matchBinding tests every axis of a flat binding against the filter.
// Used by ListACLs after expanding entries op-by-op.
func matchBinding(f handlers.ACLFilter, b handlers.ACLBinding) bool {
	if !matchString(f.Principal, b.Principal) {
		return false
	}
	if !matchString(f.ResourceType, b.ResourceType) {
		return false
	}
	if !matchString(f.ResourceName, b.ResourceName) {
		return false
	}
	if !matchPattern(f.PatternType, b.PatternType) {
		return false
	}
	if !matchString(f.Operation, b.Operation) {
		return false
	}
	if !matchString(f.Permission, b.Permission) {
		return false
	}
	if !matchString(f.Host, b.Host) {
		return false
	}
	return true
}

// matchString returns true when filter is empty (wildcard) or matches
// exactly. Apache Kafka's "match" semantics for non-literal-prefix
// resource names aren't honoured here — see matchPattern for the
// pattern-type axis where they matter.
func matchString(filter, value string) bool {
	if filter == "" {
		return true
	}
	return filter == value
}

// matchPattern handles the wire-level PatternType MATCH wildcard.
// Apache Kafka treats MATCH on a filter as "any of LITERAL or
// PREFIXED on the stored entry". Empty filter = ANY (all). Otherwise
// exact match.
func matchPattern(filter, value string) bool {
	if filter == "" || filter == "match" {
		// MATCH expands to literal+prefix per KIP-290; on the entry
		// side we only ever store one of those, so MATCH = ANY.
		return value == "literal" || value == "prefix"
	}
	return filter == value
}

// normalisePermission lifts the CR's lowercase "allow"/"deny" into the
// on-disk "Allow"/"Deny" casing that the broker's AclEngine and the
// wire-level response both expect (the boundary translation lives in
// operator/controllers/acls.go for the JSON-on-disk path; we duplicate
// here so the writer is self-contained and tests don't have to import
// the operator package).
func normalisePermission(t string) string {
	switch strings.ToLower(t) {
	case "deny":
		return "Deny"
	default:
		return "Allow"
	}
}
