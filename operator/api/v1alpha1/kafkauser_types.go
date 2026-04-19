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
	Authentication KafkaUserAuthentication `json:"authentication"`
	Quotas         *KafkaUserQuotas        `json:"quotas,omitempty"`
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
