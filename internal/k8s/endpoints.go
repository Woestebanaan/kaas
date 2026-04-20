package k8s

import (
	"context"
	"log/slog"
	"sort"
	"sync"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// BrokerEndpoint describes one broker pod.
type BrokerEndpoint struct {
	NodeID int32
	Host   string
	Port   int32
	Ready  bool
}

// BrokerRegistry maintains a live map of broker endpoints derived from EndpointSlice events.
type BrokerRegistry struct {
	self     BrokerEndpoint
	onChange func([]BrokerEndpoint)

	mu      sync.RWMutex
	brokers map[int32]BrokerEndpoint // ordinal → endpoint
}

func NewBrokerRegistry(self BrokerEndpoint, onChange func([]BrokerEndpoint)) *BrokerRegistry {
	return &BrokerRegistry{
		self:     self,
		onChange: onChange,
		brokers:  map[int32]BrokerEndpoint{self.NodeID: self},
	}
}

// Self returns this broker's own endpoint.
func (r *BrokerRegistry) Self() BrokerEndpoint { return r.self }

// All returns all known broker endpoints sorted by NodeID.
func (r *BrokerRegistry) All() []BrokerEndpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]BrokerEndpoint, 0, len(r.brokers))
	for _, b := range r.brokers {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// Count returns the number of known ready brokers.
func (r *BrokerRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.brokers)
}

// Upsert manually registers a broker endpoint (used in tests and local dev).
func (r *BrokerRegistry) Upsert(ep BrokerEndpoint) {
	r.mu.Lock()
	r.brokers[ep.NodeID] = ep
	r.mu.Unlock()
}

// Watch streams EndpointSlice events for the headless service and updates the registry.
// Blocks until ctx is cancelled.
func (r *BrokerRegistry) Watch(ctx context.Context, client kubernetes.Interface, namespace, headlessSvc string) error {
	labelSel := "kubernetes.io/service-name=" + headlessSvc
	for {
		watcher, err := client.DiscoveryV1().EndpointSlices(namespace).Watch(ctx, metav1.ListOptions{
			LabelSelector: labelSel,
		})
		if err != nil {
			slog.Error("endpoints watcher: failed to start watch", "err", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				continue
			}
		}
		if err := r.consumeWatch(ctx, watcher); err != nil {
			watcher.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				slog.Warn("endpoints watcher: restarting after error", "err", err)
			}
		}
	}
}

func (r *BrokerRegistry) consumeWatch(ctx context.Context, w watch.Interface) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-w.ResultChan():
			if !ok {
				return nil
			}
			es, ok := event.Object.(*discoveryv1.EndpointSlice)
			if !ok {
				continue
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				r.applySlice(es)
			case watch.Deleted:
				r.deleteSlice(es)
			}
		}
	}
}

func (r *BrokerRegistry) applySlice(es *discoveryv1.EndpointSlice) {
	port := int32(9092)
	for _, p := range es.Ports {
		if p.Port != nil {
			port = *p.Port
			break
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, ep := range es.Endpoints {
		if ep.Hostname == nil || len(ep.Addresses) == 0 {
			continue
		}
		ordinal, err := parseOrdinal(*ep.Hostname)
		if err != nil {
			continue
		}
		ready := ep.Conditions.Ready != nil && *ep.Conditions.Ready
		if ready {
			r.brokers[ordinal] = BrokerEndpoint{NodeID: ordinal, Host: ep.Addresses[0], Port: port, Ready: true}
		} else {
			delete(r.brokers, ordinal)
		}
	}
	r.fireOnChange()
}

func (r *BrokerRegistry) deleteSlice(es *discoveryv1.EndpointSlice) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ep := range es.Endpoints {
		if ep.Hostname == nil {
			continue
		}
		if ordinal, err := parseOrdinal(*ep.Hostname); err == nil {
			delete(r.brokers, ordinal)
		}
	}
	r.fireOnChange()
}

// fireOnChange calls onChange with a snapshot. Must be called with r.mu held.
func (r *BrokerRegistry) fireOnChange() {
	if r.onChange == nil {
		return
	}
	all := make([]BrokerEndpoint, 0, len(r.brokers))
	for _, b := range r.brokers {
		all = append(all, b)
	}
	r.onChange(all)
}
