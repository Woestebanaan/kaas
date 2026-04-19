package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Members",type=integer,JSONPath=".status.memberCount"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"

type KafkaUserGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaUserGroupSpec   `json:"spec,omitempty"`
	Status KafkaUserGroupStatus `json:"status,omitempty"`
}

type KafkaUserGroupSpec struct {
	Members []string    `json:"members"`
	Rules   []AclRule   `json:"rules,omitempty"`
}

type KafkaUserGroupStatus struct {
	MemberCount int32              `json:"memberCount,omitempty"`
	Conditions  []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true

type KafkaUserGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaUserGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaUserGroup{}, &KafkaUserGroupList{})
}
