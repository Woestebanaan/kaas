package observability

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// TopicTrafficMeter holds the always-emit per-topic Produce/Fetch
// counters that drive Grafana's "records in / records out per
// topic" panels. gh #115 + gh #121 PR1.
//
// Apache Kafka exposes BytesInPerSec / BytesOutPerSec as MBean
// meters keyed by topic — they have a "current value" at every
// scrape, even when the topic is idle, so dashboard panels show a
// flat zero line instead of a gap. Skafka pre-#121 used OTel
// Int64Counter instruments that only emit when Add() fires; idle
// topics disappeared from the timeseries entirely.
//
// Design:
//
//  1. Per-topic atomic accumulators, set up at topic-discovery
//     time via Touch(). Hot-path Produce/Fetch handlers update
//     them with one atomic.Add — fewer allocations than the old
//     `metric.WithAttributes(...)` per call.
//  2. The OTel SDK registers 4 ObservableInt64Counter instruments
//     and calls a single callback at every scrape interval. The
//     callback walks the accumulator map and emits one cumulative
//     observation per topic per metric. Observable counters always
//     emit, even when the underlying value hasn't changed since
//     the last scrape — that's the gh #115 fix.
//
// Topics that have NEVER received traffic but exist in the
// TopicRegistry still emit zero — callers should invoke Touch()
// for every topic the broker becomes aware of (TopicRegistry.Add
// hook). This guarantees a dashboard panel shows every existing
// topic, traffic or not.
type TopicTrafficMeter struct {
	mu       sync.RWMutex
	counters map[string]*topicCounters
}

type topicCounters struct {
	produceRecords atomic.Int64
	produceBytes   atomic.Int64
	fetchRecords   atomic.Int64
	fetchBytes     atomic.Int64
}

// NewTopicTrafficMeter constructs the meter. The returned meter is
// usable immediately; the OTel instruments are wired separately by
// registerTopicTrafficInstruments.
func NewTopicTrafficMeter() *TopicTrafficMeter {
	return &TopicTrafficMeter{counters: make(map[string]*topicCounters)}
}

// Touch ensures an accumulator entry exists for the topic. Idempotent.
// Called when a topic becomes known to the broker (TopicRegistry.Add).
// After this, the topic emits zero on every scrape until traffic
// arrives — eliminating the gh #115 "no data" gap on idle topics.
func (m *TopicTrafficMeter) Touch(topic string) {
	if topic == "" {
		return
	}
	m.mu.RLock()
	_, ok := m.counters[topic]
	m.mu.RUnlock()
	if ok {
		return
	}
	m.mu.Lock()
	if _, ok := m.counters[topic]; !ok {
		m.counters[topic] = &topicCounters{}
	}
	m.mu.Unlock()
}

// Forget removes the accumulator. Called when a topic is deleted so
// orphan timeseries don't linger on the dashboard indefinitely.
// Idempotent.
func (m *TopicTrafficMeter) Forget(topic string) {
	m.mu.Lock()
	delete(m.counters, topic)
	m.mu.Unlock()
}

// RecordProduce is the hot-path call from the Produce handler.
// Auto-touches the topic — callers don't have to invoke Touch
// separately for first-Produce-on-a-new-topic.
func (m *TopicTrafficMeter) RecordProduce(topic string, records, bytes int64) {
	tc := m.ensure(topic)
	if tc == nil {
		return
	}
	if records > 0 {
		tc.produceRecords.Add(records)
	}
	if bytes > 0 {
		tc.produceBytes.Add(bytes)
	}
}

// RecordFetch is the hot-path call from the Fetch handler.
// Auto-touches the topic. bytes may be 0 (empty Fetch response);
// the call still updates the timestamp on the underlying timeseries
// so dashboards see "fetch happened, just empty".
func (m *TopicTrafficMeter) RecordFetch(topic string, records, bytes int64) {
	tc := m.ensure(topic)
	if tc == nil {
		return
	}
	if records > 0 {
		tc.fetchRecords.Add(records)
	}
	if bytes > 0 {
		tc.fetchBytes.Add(bytes)
	}
}

// ensure returns the accumulator for `topic`, creating it if missing.
// Fast-path is a read-lock; only the first-time-seen case takes the
// write lock.
func (m *TopicTrafficMeter) ensure(topic string) *topicCounters {
	if topic == "" {
		return nil
	}
	m.mu.RLock()
	tc, ok := m.counters[topic]
	m.mu.RUnlock()
	if ok {
		return tc
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if tc, ok := m.counters[topic]; ok {
		return tc
	}
	tc = &topicCounters{}
	m.counters[topic] = tc
	return tc
}

// snapshot returns a slice of (topic, counters) pairs for the
// callback. Holds the read lock just long enough to copy the slice
// header — no per-topic copying needed because the accumulator
// pointers are stable across map shape changes.
func (m *TopicTrafficMeter) snapshot() []topicSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]topicSnapshot, 0, len(m.counters))
	for name, tc := range m.counters {
		out = append(out, topicSnapshot{
			topic:          name,
			produceRecords: tc.produceRecords.Load(),
			produceBytes:   tc.produceBytes.Load(),
			fetchRecords:   tc.fetchRecords.Load(),
			fetchBytes:     tc.fetchBytes.Load(),
		})
	}
	return out
}

type topicSnapshot struct {
	topic          string
	produceRecords int64
	produceBytes   int64
	fetchRecords   int64
	fetchBytes     int64
}

// registerTopicTrafficInstruments wires the ObservableInt64Counters
// on the given meter and registers a single callback that walks the
// accumulator map and emits one observation per topic per metric.
// Returns the four instruments so callers can hold them if needed
// (currently only the callback uses them).
//
// Idempotent only across distinct meters — calling twice on the
// same meter panics (OTel forbids duplicate instrument names).
func registerTopicTrafficInstruments(m metric.Meter, meter *TopicTrafficMeter) error {
	produceRecords, err := m.Int64ObservableCounter("skafka.produce.records",
		metric.WithDescription("Cumulative records produced per topic. Emits at every scrape interval (including 0 for idle topics) so dashboard rate() panels never gap."),
		metric.WithUnit("{record}"))
	if err != nil {
		return err
	}
	produceBytes, err := m.Int64ObservableCounter("skafka.produce.bytes",
		metric.WithDescription("Cumulative bytes produced per topic. Idle-emit invariant."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	fetchRecords, err := m.Int64ObservableCounter("skafka.fetch.records",
		metric.WithDescription("Cumulative records fetched per topic. Idle-emit invariant."),
		metric.WithUnit("{record}"))
	if err != nil {
		return err
	}
	fetchBytes, err := m.Int64ObservableCounter("skafka.fetch.bytes",
		metric.WithDescription("Cumulative bytes fetched per topic. Idle-emit invariant."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		for _, snap := range meter.snapshot() {
			attrs := metric.WithAttributes(attribute.String("topic", snap.topic))
			obs.ObserveInt64(produceRecords, snap.produceRecords, attrs)
			obs.ObserveInt64(produceBytes, snap.produceBytes, attrs)
			obs.ObserveInt64(fetchRecords, snap.fetchRecords, attrs)
			obs.ObserveInt64(fetchBytes, snap.fetchBytes, attrs)
		}
		return nil
	}, produceRecords, produceBytes, fetchRecords, fetchBytes)
	return err
}
