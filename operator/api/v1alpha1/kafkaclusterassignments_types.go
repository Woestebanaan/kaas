package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Controller",type=string,JSONPath=".status.controller"
// +kubebuilder:printcolumn:name="Epoch",type=integer,JSONPath=".status.controllerEpoch"
// +kubebuilder:printcolumn:name="Version",type=integer,JSONPath=".status.assignmentVersion"
// +kubebuilder:printcolumn:name="Truncated",type=boolean,JSONPath=".status.truncated"

// KafkaClusterAssignments is a best-effort, read-only debug mirror of the
// authoritative cluster assignment held in /data/__cluster/assignment.json on
// the shared PVC. The controller broker writes this CR fire-and-forget after
// every authoritative file write; brokers never read it. One CR per
// KafkaCluster, sharing the KafkaCluster's name and namespace.
//
// Spec is intentionally empty — all state lives in Status. Modifying Spec has
// no effect on cluster behaviour.
type KafkaClusterAssignments struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaClusterAssignmentsSpec   `json:"spec,omitempty"`
	Status KafkaClusterAssignmentsStatus `json:"status,omitempty"`
}

type KafkaClusterAssignmentsSpec struct{}

type KafkaClusterAssignmentsStatus struct {
	// ControllerEpoch is the leaseTransitions value of the skafka-controller
	// Lease at the moment the assignment was written. Brokers reject files
	// with a stale epoch.
	ControllerEpoch int64 `json:"controllerEpoch,omitempty"`

	// AssignmentVersion is a controller-local monotonic counter, incremented
	// on every write within a single controller's tenure.
	AssignmentVersion int64 `json:"assignmentVersion,omitempty"`

	// GeneratedAt is the wall-clock time the controller produced this
	// assignment. RFC3339.
	GeneratedAt string `json:"generatedAt,omitempty"`

	// Controller is the broker ID currently holding the controller Lease.
	Controller string `json:"controller,omitempty"`

	// Truncated is true when the partition list was clipped to fit under the
	// 1MB Kubernetes object size limit. Inspect the file on the PVC for the
	// full list when this is set.
	Truncated bool `json:"truncated,omitempty"`

	Brokers        []KafkaClusterBroker             `json:"brokers,omitempty"`
	Partitions     []KafkaClusterPartitionAssign    `json:"partitions,omitempty"`
	ConsumerGroups []KafkaClusterConsumerGroupAssign `json:"consumerGroups,omitempty"`
}

type KafkaClusterBroker struct {
	ID string `json:"id"`
	// +kubebuilder:validation:Enum=alive;draining;dead
	Health   string `json:"health"`
	LastSeen string `json:"lastSeen,omitempty"`
}

type KafkaClusterPartitionAssign struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Broker    string `json:"broker"`
	Epoch     int64  `json:"epoch"`
	// +kubebuilder:validation:Enum=leader
	Role string `json:"role,omitempty"`
}

type KafkaClusterConsumerGroupAssign struct {
	GroupID string `json:"groupId"`
	Broker  string `json:"broker"`
	Epoch   int64  `json:"epoch"`
}

// +kubebuilder:object:root=true

type KafkaClusterAssignmentsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaClusterAssignments `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaClusterAssignments{}, &KafkaClusterAssignmentsList{})
}
