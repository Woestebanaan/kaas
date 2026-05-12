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
type KafkaUserAuthorization struct {
	// +kubebuilder:validation:Enum=simple
	// +kubebuilder:default=simple
	Type string         `json:"type"`
	ACLs []KafkaUserACL `json:"acls"`
}

// KafkaUserACL is one access-control rule attached to this principal.
// Field naming matches Strimzi exactly: `type: allow|deny` (lowercase),
// optional `host` (defaults to "*"). Operations are validated against
// Apache Kafka's standard set.
type KafkaUserACL struct {
	Resource   KafkaUserACLResource `json:"resource"`
	// +kubebuilder:validation:MinItems=1
	Operations []string `json:"operations"`
	// +kubebuilder:validation:Enum=allow;deny
	// +kubebuilder:default=allow
	Type string `json:"type,omitempty"`
	// Source-IP filter. Defaults to "*" (any host). Reserved for
	// forward-compat with the Apache Kafka authorizer's host field;
	// skafka 0.1.117 enforces only "*" — other values are accepted at
	// the CR level but treated as "*" at the broker.
	// +kubebuilder:default="*"
	Host string `json:"host,omitempty"`
}

// KafkaUserACLResource identifies the Kafka resource the ACL applies to.
// Same shape as the prior AclResource type; renamed for namespace
// hygiene now that ACLs live inside KafkaUser.
type KafkaUserACLResource struct {
	// +kubebuilder:validation:Enum=topic;group;cluster;transactionalId
	Type string `json:"type"`
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=literal;prefix
	// +kubebuilder:default=literal
	PatternType string `json:"patternType,omitempty"`
}

type KafkaUserAuthentication struct {
	// +kubebuilder:validation:Enum=scram-sha-512;tls;kubernetes-serviceaccount
	Type string `json:"type"`
	// Used when type=scram-sha-512
	Password *SecretKeyRef `json:"password,omitempty"`
	// Used when type=tls
	CertificateRef *LocalObjectRef `json:"certificateRef,omitempty"`
	// Used when type=kubernetes-serviceaccount
	ServiceAccountRef *ServiceAccountRef `json:"serviceAccountRef,omitempty"`
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
	// +kubebuilder:validation:Minimum=0
	ProducerByteRate *int64 `json:"producerByteRate,omitempty"`
	// +kubebuilder:validation:Minimum=0
	ConsumerByteRate *int64 `json:"consumerByteRate,omitempty"`
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
