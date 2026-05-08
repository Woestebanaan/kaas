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
	// ApplicationName is the ArgoCD Application name (typically the
	// chart's release name, e.g. "skafka"). Empty disables all
	// ArgoCD-specific annotations.
	ApplicationName string
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
	if c.ApplicationName == "" {
		return nil
	}
	return map[string]string{
		argoCompareOptionsAnnotation: "IgnoreExtraneous",
		argoTrackingIDAnnotation:     argoTrackingID(c.ApplicationName, group, kind, namespace, metaName),
	}
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
	argoTrackingIDAnnotation     = "argocd.argoproj.io/tracking-id"
)
