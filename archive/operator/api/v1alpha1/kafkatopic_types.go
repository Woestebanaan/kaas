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
	// TopicName is the name of the Kafka topic on the wire. When unset
	// (the common case), it defaults to metadata.name. Set this only
	// when the desired Kafka name isn't a valid Kubernetes resource
	// name — e.g. uppercase letters, double underscores, or names
	// longer than 253 characters. Kafka allows up to 249 characters
	// from [A-Za-z0-9._-]; Kubernetes resource names must follow RFC
	// 1123 (lowercase, dots/hyphens, start+end alphanumeric, ≤ 253).
	// The admin-protocol path (gh #51 + #86) synthesises a
	// hash-derived metadata.name and sets this field when the literal
	// Kafka name fails RFC 1123. Mirrors Strimzi's spec.topicName
	// (https://strimzi.io/docs).
	// +kubebuilder:validation:MaxLength=249
	TopicName string `json:"topicName,omitempty"`
	// +kubebuilder:validation:Minimum=1
	Partitions int32            `json:"partitions"`
	Config     KafkaTopicConfig `json:"config,omitempty"`
}

// EffectiveTopicName returns the on-wire Kafka topic name, falling
// back to the resource name when spec.topicName is unset. Callers in
// the broker (TopicWatcher) and operator (KafkaTopicReconciler) MUST
// use this rather than reading either field directly — that way old
// CRs (no spec.topicName) keep working AND the new admin-protocol
// path (synthetic metadata.name + spec.topicName) is correctly
// resolved.
func (t *KafkaTopic) EffectiveTopicName() string {
	if t.Spec.TopicName != "" {
		return t.Spec.TopicName
	}
	return t.Name
}

type KafkaTopicConfig struct {
	// retention.ms in Kafka semantics: -1 means "infinite" (never
	// delete by time). Streams sets this on its changelog topics.
	// +kubebuilder:validation:Minimum=-1
	RetentionMs *int64 `json:"retentionMs,omitempty"`
	// RetentionBytes caps the per-partition log size. When the cleaner runs
	// and a partition's total segment bytes exceed this value, oldest
	// closed segments are deleted until the partition is back under the
	// limit. Active segment is never touched. -1 = unlimited (Kafka
	// convention); 0 = treat as unlimited too.
	// +kubebuilder:validation:Minimum=-1
	RetentionBytes *int64 `json:"retentionBytes,omitempty"`
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
	PartitionCount int32 `json:"partitionCount,omitempty"`
	// TopicID is the stable UUID for this topic (KIP-516, gh #105).
	// The operator generates a random v4 UUID on first reconcile when
	// missing and never rotates it; deleting + re-creating a topic
	// produces a fresh UUID (matches Apache's "re-created topics have
	// distinct IDs" contract).
	//
	// Format: 36-char canonical hyphenated UUID
	// (8-4-4-4-12, e.g. "00112233-4455-6677-8899-aabbccddeeff"). The
	// broker surfaces this on Metadata v10+ responses and on the
	// CreateTopics v7+ response so AdminClient consumers can address
	// topics by ID. Empty/missing → broker emits all-zero UUID
	// (pre-#105 behaviour, preserved for CRs that never had a status
	// populated).
	TopicID    string             `json:"topicId,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
