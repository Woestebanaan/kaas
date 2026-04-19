package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Principal",type=string,JSONPath=".spec.principal.name"
// +kubebuilder:printcolumn:name="ACLs",type=integer,JSONPath=".status.aclCount"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"

type KafkaAcl struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaAclSpec   `json:"spec,omitempty"`
	Status KafkaAclStatus `json:"status,omitempty"`
}

type KafkaAclSpec struct {
	Principal AclPrincipal `json:"principal"`
	Rules     []AclRule    `json:"rules"`
}

type AclPrincipal struct {
	// +kubebuilder:validation:Enum=KafkaUser;KafkaUserGroup
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type AclRule struct {
	Resource   AclResource  `json:"resource"`
	Operations []string     `json:"operations"`
	// +kubebuilder:validation:Enum=Allow;Deny
	Permission string       `json:"permission"`
}

type AclResource struct {
	// +kubebuilder:validation:Enum=topic;group;cluster;transactionalId
	Type string `json:"type"`
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=literal;prefix;match;any
	PatternType string `json:"patternType"`
}

type KafkaAclStatus struct {
	AclCount   int32              `json:"aclCount,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true

type KafkaAclList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaAcl `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaAcl{}, &KafkaAclList{})
}
