package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Partitions",type=integer,JSONPath=".spec.partitions"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"

type KafkaTopic struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaTopicSpec   `json:"spec,omitempty"`
	Status KafkaTopicStatus `json:"status,omitempty"`
}

type KafkaTopicSpec struct {
	// +kubebuilder:validation:Minimum=1
	Partitions int32            `json:"partitions"`
	Config     KafkaTopicConfig `json:"config,omitempty"`
}

type KafkaTopicConfig struct {
	// +kubebuilder:validation:Minimum=0
	RetentionMs *int64 `json:"retentionMs,omitempty"`
	// +kubebuilder:validation:Minimum=1
	SegmentBytes *int64 `json:"segmentBytes,omitempty"`
	// +kubebuilder:validation:Enum=delete;compact;"compact,delete"
	CleanupPolicy string `json:"cleanupPolicy,omitempty"`
	// +kubebuilder:validation:Minimum=0
	MinCompactionLagMs *int64 `json:"minCompactionLagMs,omitempty"`
	// +kubebuilder:validation:Minimum=0
	DeleteRetentionMs *int64 `json:"deleteRetentionMs,omitempty"`
}

type KafkaTopicStatus struct {
	PartitionCount int32              `json:"partitionCount,omitempty"`
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true

type KafkaTopicList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaTopic `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaTopic{}, &KafkaTopicList{})
}
