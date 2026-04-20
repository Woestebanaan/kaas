package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// ReadinessCondition is the custom pod condition type that gates headless-service membership.
// Must be declared in the StatefulSet pod spec under readinessGates.
const ReadinessCondition = "skafka.io/PartitionsReady"

// ReadinessUpdater patches the broker's own Pod condition to signal readiness.
// The pod only joins the headless service (and receives client traffic) once Ready=True.
type ReadinessUpdater struct {
	client    kubernetes.Interface
	podName   string
	namespace string
}

func NewReadinessUpdater(client kubernetes.Interface, podName, namespace string) *ReadinessUpdater {
	return &ReadinessUpdater{client: client, podName: podName, namespace: namespace}
}

// SetReady patches the pod condition to True or False.
func (r *ReadinessUpdater) SetReady(ctx context.Context, ready bool) error {
	status := corev1.ConditionFalse
	msg := "waiting for partition leases"
	if ready {
		status = corev1.ConditionTrue
		msg = "all assigned partitions are ready"
	}

	condition := map[string]interface{}{
		"type":               ReadinessCondition,
		"status":             string(status),
		"message":            msg,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"conditions": []interface{}{condition},
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = r.client.CoreV1().Pods(r.namespace).Patch(
		ctx, r.podName, types.MergePatchType, data, metav1.PatchOptions{}, "status",
	)
	return err
}

// WatchAndSetReady watches the LeaderChange channel and calls SetReady(true) once all
// partitions in wantPartitions have a known leader. Runs until ctx is cancelled.
func (r *ReadinessUpdater) WatchAndSetReady(ctx context.Context, changes <-chan struct{},
	isAllReady func() bool) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-changes:
			if isAllReady() {
				if err := r.SetReady(ctx, true); err != nil {
					fmt.Printf("readiness: SetReady error: %v\n", err)
				}
				return
			}
		}
	}
}
