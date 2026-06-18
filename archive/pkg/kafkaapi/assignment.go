// Package kafkaapi defines the Go-level types and interfaces shared between
// the broker, the cluster controller, and the operator. Types here are not
// Kafka wire types — those live in internal/protocol/codec — but the
// service-level contract for coordination, storage, and authentication.
package kafkaapi

import (
	"context"
	"time"
)

// BrokerHealth tracks a broker's liveness as observed by the controller.
type BrokerHealth string

const (
	BrokerHealthAlive    BrokerHealth = "alive"
	BrokerHealthDraining BrokerHealth = "draining"
	BrokerHealthDead     BrokerHealth = "dead"
)

// PartitionRole describes a broker's role for a partition. Skafka is RF=1,
// so the only role today is leader; the field exists to make the file format
// forward-compatible with v2 transactional or replicated extensions.
type PartitionRole string

const PartitionRoleLeader PartitionRole = "leader"

// Assignment is the authoritative cluster state: which broker leads which
// partition (and serves which consumer group) at which epoch.
//
// Persisted to /data/__cluster/assignment.json on the shared PVC by the
// controller broker, atomically replaced via tmp + rename. Brokers consume
// it via heartbeat-pushed ASSIGNMENT_CHANGED notifications and a 1s mtime
// polling safety net (see internal/broker/assignment_watch.go in Phase 4).
//
// ControllerEpoch is the leaseTransitions value of the skafka-controller
// Lease at write time. Brokers reject any file whose epoch is stale relative
// to the current Lease, fencing out partitioned ex-controllers.
type Assignment struct {
	ControllerEpoch   int64                       `json:"controllerEpoch"`
	AssignmentVersion int64                       `json:"assignmentVersion"`
	GeneratedAt       time.Time                   `json:"generatedAt"`
	Controller        string                      `json:"controller"`
	Brokers           []BrokerAssignment          `json:"brokers"`
	Partitions        []PartitionAssignment       `json:"partitions"`
	ConsumerGroups    []ConsumerGroupAssignment   `json:"consumerGroups,omitempty"`
}

type BrokerAssignment struct {
	ID       string       `json:"id"`
	Health   BrokerHealth `json:"health"`
	LastSeen time.Time    `json:"lastSeen"`
}

type PartitionAssignment struct {
	Topic     string        `json:"topic"`
	Partition int32         `json:"partition"`
	Broker    string        `json:"broker"`
	Epoch     uint32        `json:"epoch"`
	Role      PartitionRole `json:"role"`
}

type ConsumerGroupAssignment struct {
	GroupID string `json:"groupId"`
	Broker  string `json:"broker"`
	Epoch   uint32 `json:"epoch"`
}

// AssignmentStore is the persistence boundary for the cluster's authoritative
// assignment. The controller is the only Writer; every broker is a Reader and
// observes changes via Watch.
//
// The file-backed implementation (Phase 4) writes via tmp + rename within
// /data/__cluster/ for atomicity on NFSv4-class storage.
type AssignmentStore interface {
	// Read returns the current assignment, or an error if the file is
	// missing or malformed.
	Read(ctx context.Context) (*Assignment, error)

	// Write replaces the current assignment atomically. Caller must have
	// already populated ControllerEpoch and AssignmentVersion.
	Write(ctx context.Context, a *Assignment) error

	// Watch returns a channel that fires whenever the underlying file
	// changes (push from the controller via heartbeat, or detected by the
	// poller). Receivers re-read the file via Read.
	Watch(ctx context.Context) (<-chan struct{}, error)
}
