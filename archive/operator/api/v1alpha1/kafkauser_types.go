package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Auth type",type=string,JSONPath=".spec.authentication.type"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"

type KafkaUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaUserSpec   `json:"spec,omitempty"`
	Status KafkaUserStatus `json:"status,omitempty"`
}

type KafkaUserSpec struct {
	Authentication KafkaUserAuthentication  `json:"authentication"`
	// Authorization carries the user's ACL rules inline (Strimzi-style).
	// Pre-gh #135 ACLs lived in a separate KafkaAcl CR (and could also be
	// attached to a KafkaUserGroup that listed multiple members). Both
	// secondary CRs are removed in v0.1.117 — every rule that applies to
	// a principal is authored on the principal's own KafkaUser CR.
	Authorization *KafkaUserAuthorization `json:"authorization,omitempty"`
	Quotas        *KafkaUserQuotas        `json:"quotas,omitempty"`
}

// KafkaUserAuthorization wraps the ACL list, mirroring Strimzi's
// spec.authorization shape so paste-from-Strimzi works verbatim. The
// `type` discriminator is set to "simple" today; reserved for forward
// compatibility with future authorization backends (e.g. OPA, OIDC).
//
// gh #137: defaults are deliberately NOT declared via
// +kubebuilder:default. Apiserver-side defaulting collides with
// GitOps tooling (ArgoCD reports permanent drift because the stored
// object has fields the git source omits). The operator is the
// canonical defaulter — empty `type` is treated as "simple", same
// pattern as the ACL fields below.
type KafkaUserAuthorization struct {
	// +kubebuilder:validation:Enum=simple
	Type string         `json:"type,omitempty"`
	ACLs []KafkaUserACL `json:"acls"`
}

// KafkaUserACL is one access-control rule attached to this principal.
// Field naming matches Strimzi exactly: `type: allow|deny` (lowercase),
// optional `host`. Operations are validated against Apache Kafka's
// standard set. Defaults applied at the operator boundary (acls.go
// aclToEntry), not via apiserver +kubebuilder:default — see gh #137.
type KafkaUserACL struct {
	Resource   KafkaUserACLResource `json:"resource"`
	// +kubebuilder:validation:MinItems=1
	Operations []string `json:"operations"`
	// +kubebuilder:validation:Enum=allow;deny
	Type string `json:"type,omitempty"`
	// Source-IP filter. Empty means "any host" (the Apache Kafka
	// authorizer's "*" wildcard). Reserved for forward-compat with the
	// host filter; skafka enforces "any" only — non-empty values are
	// stored in the CR for round-trip but ignored at the broker today.
	Host string `json:"host,omitempty"`
}

// KafkaUserACLResource identifies the Kafka resource the ACL applies to.
// Same shape as the prior AclResource type; renamed for namespace
// hygiene now that ACLs live inside KafkaUser. PatternType defaulting
// happens operator-side (acls.go), not via apiserver defaulting — see
// gh #137.
type KafkaUserACLResource struct {
	// +kubebuilder:validation:Enum=topic;group;cluster;transactionalId
	Type string `json:"type"`
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=literal;prefix
	PatternType string `json:"patternType,omitempty"`
}

type KafkaUserAuthentication struct {
	// +kubebuilder:validation:Enum=scram-sha-512;tls;kubernetes-serviceaccount
	Type string `json:"type"`
	// Used when type=scram-sha-512
	Password *SecretKeyRef `json:"password,omitempty"`
	// Scram is the runtime-rotation path for type=scram-sha-512 (gh #104,
	// KIP-554 AlterUserScramCredentials API). When set, the operator uses
	// the supplied (salt, storedKey, serverKey, iterations) verbatim and
	// skips the Password → PBKDF2 derivation. The wire-level admin path
	// computes these from the client-supplied SaltedPassword and patches
	// this field; clients receive the rotated credential via the same
	// AdminClient call that wrote it (Apache's model: the rotator knows
	// the new password locally and doesn't need a Secret to learn it).
	Scram *KafkaUserScramCredential `json:"scram,omitempty"`
	// Used when type=tls
	CertificateRef *LocalObjectRef `json:"certificateRef,omitempty"`
	// Used when type=kubernetes-serviceaccount
	ServiceAccountRef *ServiceAccountRef `json:"serviceAccountRef,omitempty"`
}

// KafkaUserScramCredential carries SCRAM-SHA-512 credentials in their
// already-derived form (so the operator skips PBKDF2). All fields are
// base64-encoded raw bytes. Matches the credentials.json ScramCredential
// shape verbatim so the operator's serialiser can pass it through.
type KafkaUserScramCredential struct {
	Salt       string `json:"salt"`
	StoredKey  string `json:"storedKey"`
	ServerKey  string `json:"serverKey"`
	Iterations int    `json:"iterations"`
}

type SecretKeyRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type LocalObjectRef struct {
	Name string `json:"name"`
}

type ServiceAccountRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type KafkaUserQuotas struct {
	// ProducerMaxByteRatePerBroker caps producer throughput at THIS broker
	// only (Apache Kafka 3.7-style per-broker quota; KIP-13). With N brokers
	// the effective cluster-wide ceiling is N × the configured value.
	// +kubebuilder:validation:Minimum=0
	ProducerMaxByteRatePerBroker *int64 `json:"producerMaxByteRatePerBroker,omitempty"`
	// ConsumerMaxByteRatePerBroker caps consumer throughput at THIS broker
	// only (same per-broker semantics as the producer field).
	// +kubebuilder:validation:Minimum=0
	ConsumerMaxByteRatePerBroker *int64 `json:"consumerMaxByteRatePerBroker,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	RequestPercentage *int32 `json:"requestPercentage,omitempty"`
}

type KafkaUserStatus struct {
	Secret     string             `json:"secret,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true

type KafkaUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaUser{}, &KafkaUserList{})
}
