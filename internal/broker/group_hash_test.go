package broker

import (
	"fmt"
	"testing"
)

// TestGroupCoordinatorSlotDeterministic: same input → same slot,
// across many invocations and across restarts. Coordinator routing
// is the contract every broker computes locally; if any one broker
// disagrees, that broker fences out clients that expect to hit
// somebody else.
func TestGroupCoordinatorSlotDeterministic(t *testing.T) {
	const numBrokers = 3
	for _, gid := range []string{"foo", "bar", "skafka-test-cg-12345", ""} {
		first := GroupCoordinatorSlot(gid, numBrokers)
		for i := 0; i < 100; i++ {
			if got := GroupCoordinatorSlot(gid, numBrokers); got != first {
				t.Errorf("groupID=%q slot drifted: %d vs %d on iteration %d", gid, first, got, i)
			}
		}
	}
}

// TestGroupCoordinatorSlotInRange: every result is a valid broker
// index. Catches a regression where a refactor lets the modulo
// overflow or returns a negative.
func TestGroupCoordinatorSlotInRange(t *testing.T) {
	const numBrokers = 7
	for i := 0; i < 1000; i++ {
		gid := fmt.Sprintf("group-%d", i)
		s := GroupCoordinatorSlot(gid, numBrokers)
		if s < 0 || s >= numBrokers {
			t.Errorf("groupID=%q slot=%d out of [0,%d)", gid, s, numBrokers)
		}
	}
}

// TestGroupCoordinatorSlotZeroBrokers: numBrokers=0 returns 0
// without panicking. Lets callers hand off the "no brokers alive"
// case to PickGroupCoordinator's empty-set check rather than
// having to guard every call site.
func TestGroupCoordinatorSlotZeroBrokers(t *testing.T) {
	if got := GroupCoordinatorSlot("anything", 0); got != 0 {
		t.Errorf("zero brokers got slot=%d, want 0", got)
	}
}

// TestGroupCoordinatorSlotEvenDistribution: 1000 random group IDs
// land in roughly equal proportions across 3 slots. Catches a
// regression where the hash collapses to a single slot (eg. a typo
// that always returns 0). 20% slack is plenty — perfect uniformity
// isn't required, just absence of pathological skew.
func TestGroupCoordinatorSlotEvenDistribution(t *testing.T) {
	const numBrokers = 3
	const samples = 1000
	counts := make([]int, numBrokers)
	for i := 0; i < samples; i++ {
		s := GroupCoordinatorSlot(fmt.Sprintf("group-%d", i), numBrokers)
		counts[s]++
	}
	expected := samples / numBrokers
	tolerance := expected / 5 // 20% slack
	for slot, c := range counts {
		if c < expected-tolerance || c > expected+tolerance {
			t.Errorf("slot %d got %d samples, want ~%d (±%d)", slot, c, expected, tolerance)
		}
	}
}

// TestPickGroupCoordinatorPrefersPreferred: when the hashed slot
// belongs to an alive broker, that broker is the answer. The
// stable-preferred case is the dominant one — every group that
// was happy yesterday should stay happy today.
func TestPickGroupCoordinatorPrefersPreferred(t *testing.T) {
	brokers := []string{"skafka-0", "skafka-1", "skafka-2"}
	alive := map[string]bool{"skafka-0": true, "skafka-1": true, "skafka-2": true}
	for i := 0; i < 100; i++ {
		gid := fmt.Sprintf("group-%d", i)
		slot := GroupCoordinatorSlot(gid, len(brokers))
		want := brokers[slot]
		got := PickGroupCoordinator(gid, brokers, alive)
		if got != want {
			t.Errorf("groupID=%q got %q, want preferred %q", gid, got, want)
		}
	}
}

// TestPickGroupCoordinatorFallsBackOnDownSlot: if the hashed slot's
// broker is down, the fallback picks deterministically from the
// alive subset. Same input twice → same answer (no rejoin
// ping-pong during a transient outage).
func TestPickGroupCoordinatorFallsBackOnDownSlot(t *testing.T) {
	brokers := []string{"skafka-0", "skafka-1", "skafka-2"}
	alive := map[string]bool{"skafka-0": true, "skafka-1": false, "skafka-2": true}

	// Pick a groupID that hashes to slot 1 (the down broker).
	var targetGroup string
	for i := 0; i < 1000; i++ {
		gid := fmt.Sprintf("group-%d", i)
		if GroupCoordinatorSlot(gid, 3) == 1 {
			targetGroup = gid
			break
		}
	}
	if targetGroup == "" {
		t.Fatal("could not find a groupID that hashes to slot 1; distribution test was a lie")
	}

	first := PickGroupCoordinator(targetGroup, brokers, alive)
	if first == "skafka-1" {
		t.Errorf("fallback returned the down broker %q", first)
	}
	if first != "skafka-0" && first != "skafka-2" {
		t.Errorf("fallback returned %q, want one of skafka-0 or skafka-2", first)
	}
	// Stable across re-invocations.
	for i := 0; i < 100; i++ {
		if got := PickGroupCoordinator(targetGroup, brokers, alive); got != first {
			t.Errorf("fallback drifted: %q vs %q on iteration %d", got, first, i)
		}
	}
}

// TestPickGroupCoordinatorEmptyAliveReturnsEmpty: every broker is
// down, return "". Caller surfaces CoordinatorNotAvailable so the
// client retries instead of routing to a hallucinated broker.
func TestPickGroupCoordinatorEmptyAliveReturnsEmpty(t *testing.T) {
	brokers := []string{"skafka-0", "skafka-1"}
	alive := map[string]bool{"skafka-0": false, "skafka-1": false}
	if got := PickGroupCoordinator("anything", brokers, alive); got != "" {
		t.Errorf("all-dead got %q, want empty", got)
	}
}

// TestPickGroupCoordinatorEmptyBrokerListReturnsEmpty: defense in
// depth against a caller passing nil/empty brokers (no assignment
// loaded yet).
func TestPickGroupCoordinatorEmptyBrokerListReturnsEmpty(t *testing.T) {
	if got := PickGroupCoordinator("anything", nil, nil); got != "" {
		t.Errorf("nil brokers got %q, want empty", got)
	}
}

// TestPickGroupCoordinatorStableAcrossSnapshotCopies: brokers slice
// passed in different order produces the same answer. PickGroupCoordinator
// must sort defensively because the caller may walk a map (random order).
func TestPickGroupCoordinatorStableAcrossSnapshotCopies(t *testing.T) {
	alive := map[string]bool{"skafka-0": true, "skafka-1": true, "skafka-2": true}
	a := PickGroupCoordinator("my-group", []string{"skafka-0", "skafka-1", "skafka-2"}, alive)
	b := PickGroupCoordinator("my-group", []string{"skafka-2", "skafka-0", "skafka-1"}, alive)
	c := PickGroupCoordinator("my-group", []string{"skafka-1", "skafka-2", "skafka-0"}, alive)
	if a != b || b != c {
		t.Errorf("input-order changed the answer: %q %q %q", a, b, c)
	}
}

// TestPickGroupCoordinatorStabilityWhenOneBrokerLeaves: a rolling
// restart that brings broker-1 down for ~30s should NOT migrate
// every group — only groups whose preferred slot is broker-1.
// This is the headline win over a naive `len(alive)` divisor.
func TestPickGroupCoordinatorStabilityWhenOneBrokerLeaves(t *testing.T) {
	brokers := []string{"skafka-0", "skafka-1", "skafka-2"}
	allAlive := map[string]bool{"skafka-0": true, "skafka-1": true, "skafka-2": true}
	oneDown := map[string]bool{"skafka-0": true, "skafka-1": false, "skafka-2": true}

	const samples = 300
	migrated := 0
	for i := 0; i < samples; i++ {
		gid := fmt.Sprintf("group-%d", i)
		before := PickGroupCoordinator(gid, brokers, allAlive)
		after := PickGroupCoordinator(gid, brokers, oneDown)
		if before != after {
			migrated++
			// Only groups preferring skafka-1 should migrate.
			if before != "skafka-1" {
				t.Errorf("groupID=%q migrated %q→%q, but its preferred wasn't skafka-1", gid, before, after)
			}
		}
	}
	// Roughly 1/3 of groups should migrate (the ones preferring the
	// down broker), not 2/3 (which would happen if we modded by
	// len(alive)).
	expected := samples / 3
	tolerance := expected / 4 // 25% slack
	if migrated < expected-tolerance || migrated > expected+tolerance {
		t.Errorf("one-broker-down migration count = %d, want ~%d (±%d) — naive len(alive) modulo would have migrated ~%d",
			migrated, expected, tolerance, samples*2/3)
	}
}
