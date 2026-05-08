package k8s

import "fmt"

// ArgoCDConfig controls the optional ArgoCD annotations skafka stamps
// on CRs it creates from the Kafka admin protocol (gh #84 + gh #106).
//
// Skafka deployments NOT using ArgoCD should leave ApplicationName
// empty — every CR writer that consults this config produces plain
// CRs with no argocd.argoproj.io/* annotations. Adding ArgoCD-specific
// metadata to non-ArgoCD installs is just clutter; the broker has no
// way to detect ArgoCD's presence at runtime, so the operator opts in
// explicitly via the chart's `admin.argocd.applicationName` value.
//
// When ApplicationName is non-empty, every admin-protocol-created CR
// gets two annotations:
//
//   - argocd.argoproj.io/compare-options: IgnoreExtraneous
//     ArgoCD doesn't compare the CR against git, so its selfHeal
//     sync doesn't delete it.
//
//   - argocd.argoproj.io/tracking-id: <app>:<group>/<kind>:<ns>/<name>
//     Claims the CR as part of the named ArgoCD Application so it
//     appears in the Application's UI tree alongside the git-managed
//     CRs (rather than being invisible — the gh #106 improvement
//     over gh #84's silent-coexistence).
//
// First introduced for KafkaTopic CRs (TopicCRWriter); the same shape
// applies to any future writer that creates CRs from the Kafka admin
// protocol — KafkaACL on CreateAcls/DeleteAcls, KafkaUser on
// AlterClientQuotas / AlterUserScramCredentials, etc.
type ArgoCDConfig struct {
	// Enabled is the explicit primary gate. When false (the
	// default), Annotations() returns nil regardless of any other
	// field — every other knob (ApplicationName, CompareOptions,
	// SyncOptions) is ignored. This is defensive: an operator who
	// flips `admin.argocd.enabled: false` on the chart must NEVER
	// see argocd.argoproj.io/* annotations on runtime-created CRs,
	// even if some other env var (or programmatic caller) has set
	// ApplicationName.
	Enabled bool

	// ApplicationName is the ArgoCD Application name (typically the
	// chart's release name, e.g. "skafka"). Empty disables all
	// ArgoCD-specific annotations — the writer produces plain CRs
	// with no argocd.argoproj.io/* metadata. (Enabled is checked
	// first; both must be set for any annotation to be emitted.)
	ApplicationName string

	// CompareOptions is the value for `argocd.argoproj.io/compare-options`.
	// Default "IgnoreExtraneous" tells ArgoCD's selfHeal sync to skip
	// the resource (no drift, no prune). Empty string skips the
	// compare-options annotation entirely — useful when the operator
	// wants the runtime-created CR to be considered drift-on-purpose
	// (e.g., to surface "this topic isn't in git" as a deliberate
	// alert in ArgoCD's UI). Other valid values include
	// "IgnoreResourceUpdates" + comma-combinations; see
	// https://argo-cd.readthedocs.io/en/stable/user-guide/compare-options/.
	//
	// Only consulted when ApplicationName is non-empty (otherwise no
	// annotations at all). Operators set this via the
	// SKAFKA_ARGOCD_COMPARE_OPTIONS env var, exposed in the chart as
	// admin.argocd.compareOptions.
	CompareOptions string

	// SyncOptions is the value for `argocd.argoproj.io/sync-options`.
	// Empty (the default) skips the annotation entirely. Common
	// non-default values:
	//
	//   - "Prune=false" — resource shows as out-of-sync in the UI
	//     but ArgoCD won't delete it on prune. Pairs with empty
	//     CompareOptions for "alert me without deleting" patterns.
	//   - "Delete=false" — resource survives even when the parent
	//     Application is deleted. Production safety: topics outlive
	//     a chart uninstall.
	//   - "Prune=false,Delete=false" — both at once.
	//
	// See https://argo-cd.readthedocs.io/en/stable/user-guide/sync-options/.
	// The string is passed through verbatim — skafka doesn't validate
	// or interpret it. Operators set this via SKAFKA_ARGOCD_SYNC_OPTIONS
	// (chart: admin.argocd.syncOptions). Only consulted when
	// ApplicationName is non-empty.
	SyncOptions string
}

// Annotations returns the argocd.argoproj.io/* annotations a CR
// writer should stamp for the given resource. Empty map (which
// metav1.ObjectMeta accepts as "no annotations") when ArgoCD
// integration is disabled.
//
// `group` and `kind` come from the operator's `KafkaTopic`,
// `KafkaACL`, etc. types — typically `"skafka.io"` + the type name.
// `namespace` and `metaName` are the resource's metadata.namespace /
// metadata.name; the latter must already account for the gh #86
// synthesised-name path (Streams-style names that aren't RFC-1123
// valid get a sha1 prefix). ArgoCD's UI tree is keyed by metadata.name,
// so the tracking-id must reference the synthesised name, not the
// human-friendly Kafka resource name.
func (c ArgoCDConfig) Annotations(group, kind, namespace, metaName string) map[string]string {
	// Two gates both must pass: the explicit Enabled flag (operator
	// flipped admin.argocd.enabled: true on the chart) AND a non-
	// empty ApplicationName (something to put in the tracking-id).
	// Either alone is insufficient — disabled means disabled, no
	// matter what other fields say.
	if !c.Enabled || c.ApplicationName == "" {
		return nil
	}
	out := map[string]string{
		argoTrackingIDAnnotation: argoTrackingID(c.ApplicationName, group, kind, namespace, metaName),
	}
	if c.CompareOptions != "" {
		out[argoCompareOptionsAnnotation] = c.CompareOptions
	}
	if c.SyncOptions != "" {
		out[argoSyncOptionsAnnotation] = c.SyncOptions
	}
	return out
}

// argoTrackingID builds the value of argocd.argoproj.io/tracking-id.
// ArgoCD's format is "<app>:<group>/<kind>:<ns>/<name>" — generated
// by ArgoCD itself when it applies a manifest from git, replicated
// here for admin-protocol-created CRs so they coexist in the same
// Application resource tree.
func argoTrackingID(applicationName, group, kind, namespace, metaName string) string {
	return fmt.Sprintf("%s:%s/%s:%s/%s",
		applicationName, group, kind, namespace, metaName)
}

// Annotation keys are kept package-private constants so a single
// place owns the wire-format strings; callers go through
// ArgoCDConfig.Annotations() rather than constructing the map by
// hand.
const (
	argoCompareOptionsAnnotation = "argocd.argoproj.io/compare-options"
	argoSyncOptionsAnnotation    = "argocd.argoproj.io/sync-options"
	argoTrackingIDAnnotation     = "argocd.argoproj.io/tracking-id"
)
