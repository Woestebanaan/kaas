package controller

import (
	"sort"
	"testing"

	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// TestRendezvousPickAgreesWithBalance guards gh #75: the exported
// RendezvousPick must produce the same broker per (topic, partition)
// that Balance() assigns. The broker's topic_watcher uses
// RendezvousPick to decide who acquires the per-partition Lease first;
// if those two paths disagreed, the Lease holder and assignment.json
// could end up pointing at different brokers and produce requests
// would fail with NotLeaderOrFollowerException for that partition
// (the original kperf-0 split-brain symptom).
func TestRendezvousPickAgreesWithBalance(t *testing.T) {
	brokers := []string{"skafka-0", "skafka-1", "skafka-2"}
	topics := []TopicSpec{
		{Name: "events", PartitionCount: 9},
		{Name: "kperf", PartitionCount: 3},
		{Name: "audit-log", PartitionCount: 1},
	}
	for _, p := range Balance(nil, brokers, topics) {
		got := RendezvousPick(p.Topic, p.Partition, brokers)
		if got != p.Broker {
			t.Errorf("%s/%d: Balance assigned %q, RendezvousPick returns %q (gh #75)",
				p.Topic, p.Partition, p.Broker, got)
		}
	}
}

func TestBalance_FreshClusterDistributesEvenly(t *testing.T) {
	brokers := []string{"a", "b", "c"}
	topics := []TopicSpec{{Name: "events", PartitionCount: 9}}

	out := Balance(nil, brokers, topics)

	if len(out) != 9 {
		t.Fatalf("want 9 assignments, got %d", len(out))
	}
	counts := map[string]int{}
	for _, p := range out {
		counts[p.Broker]++
		if p.Epoch != 1 {
			t.Errorf("fresh assignment should start at epoch=1, got %d for %s/%d", p.Epoch, p.Topic, p.Partition)
		}
		if p.Role != kafkaapi.PartitionRoleLeader {
			t.Errorf("partition role should be leader, got %q", p.Role)
		}
	}
	// With 9 partitions / 3 brokers we don't demand perfect 3-3-3 (FNV-1a
	// isn't perfectly uniform on small inputs) but we do demand "no broker
	// gets all of them" and "every broker gets at least one".
	for _, b := range brokers {
		if counts[b] == 0 {
			t.Errorf("broker %s got no partitions: %v", b, counts)
		}
	}
}

func TestBalance_StableAcrossRecomputeWithSameInputs(t *testing.T) {
	brokers := []string{"a", "b", "c"}
	topics := []TopicSpec{{Name: "events", PartitionCount: 6}}

	first := Balance(nil, brokers, topics)
	prev := &kafkaapi.Assignment{Partitions: first}

	second := Balance(prev, brokers, topics)
	if !samePartitions(first, second) {
		t.Errorf("Balance was not stable across no-op recompute:\nfirst:  %+v\nsecond: %+v", first, second)
	}
}

// TestBalance_BrokerJoinRedistributes guards gh #78: when a broker
// joins a cluster that previously had a single-broker assignment
// (e.g. only one broker was alive when the controller first
// recomputed), subsequent recomputes with the larger alive set must
// move some partitions onto the newcomers. Pre-fix the stability
// rule kept everything on the original broker forever.
func TestBalance_BrokerJoinRedistributes(t *testing.T) {
	topics := []TopicSpec{{Name: "events", PartitionCount: 9}}

	// First recompute with only "a" alive — every partition lands on a.
	prev := &kafkaapi.Assignment{Partitions: Balance(nil, []string{"a"}, topics)}
	for _, p := range prev.Partitions {
		if p.Broker != "a" {
			t.Fatalf("setup: expected single-broker assignment to land on a, got %q", p.Broker)
		}
	}

	// b and c join. Recompute. We expect a meaningful redistribution —
	// pure rendezvous would put roughly a third on each broker.
	out := Balance(prev, []string{"a", "b", "c"}, topics)
	counts := map[string]int{}
	for _, p := range out {
		counts[p.Broker]++
	}
	if counts["b"] == 0 || counts["c"] == 0 {
		t.Errorf("brokers b and c got no partitions after join: %v (gh #78)", counts)
	}
	// And the partitions that DID move should have an incremented epoch.
	prevByKey := map[string]kafkaapi.PartitionAssignment{}
	for _, p := range prev.Partitions {
		prevByKey[p.Topic+"/"+string(rune(p.Partition))] = p
	}
	for _, p := range out {
		if p.Broker == "a" {
			continue
		}
		old := prevByKey[p.Topic+"/"+string(rune(p.Partition))]
		if p.Epoch <= old.Epoch {
			t.Errorf("%s/%d moved from %s to %s but epoch did not bump (%d -> %d)",
				p.Topic, p.Partition, old.Broker, p.Broker, old.Epoch, p.Epoch)
		}
	}
}

func TestBalance_BrokerDeathRebalancesOnlyAffectedPartitions(t *testing.T) {
	brokers := []string{"a", "b", "c"}
	topics := []TopicSpec{{Name: "events", PartitionCount: 9}}

	prev := &kafkaapi.Assignment{Partitions: Balance(nil, brokers, topics)}

	// b dies — recompute with {a, c}.
	out := Balance(prev, []string{"a", "c"}, topics)

	moved := 0
	for _, p := range out {
		var prevAssign kafkaapi.PartitionAssignment
		for _, q := range prev.Partitions {
			if q.Topic == p.Topic && q.Partition == p.Partition {
				prevAssign = q
				break
			}
		}
		if prevAssign.Broker != p.Broker {
			moved++
			if p.Epoch <= prevAssign.Epoch {
				t.Errorf("partition %s/%d moved %s→%s but epoch %d→%d (should be strictly greater)",
					p.Topic, p.Partition, prevAssign.Broker, p.Broker, prevAssign.Epoch, p.Epoch)
			}
			if p.Broker != "a" && p.Broker != "c" {
				t.Errorf("reassigned partition %s/%d went to dead broker %q", p.Topic, p.Partition, p.Broker)
			}
		} else {
			// Untouched partition keeps its epoch.
			if p.Epoch != prevAssign.Epoch {
				t.Errorf("partition %s/%d kept broker %s but epoch changed %d→%d",
					p.Topic, p.Partition, p.Broker, prevAssign.Epoch, p.Epoch)
			}
		}
	}
	if moved == 0 {
		t.Error("expected some partitions to move when broker b died")
	}
	// Strict-stability claim: only partitions whose broker died should move.
	// Partitions originally on a or c must stay put.
	for _, p := range prev.Partitions {
		if p.Broker == "a" || p.Broker == "c" {
			for _, q := range out {
				if q.Topic == p.Topic && q.Partition == p.Partition && q.Broker != p.Broker {
					t.Errorf("partition %s/%d on alive broker %s was moved to %s — strict-stability violated",
						p.Topic, p.Partition, p.Broker, q.Broker)
				}
			}
		}
	}
}

func TestBalance_NewlyAddedPartitionsHashedAcrossAliveSet(t *testing.T) {
	brokers := []string{"a", "b"}
	topics := []TopicSpec{{Name: "events", PartitionCount: 3}}
	prev := &kafkaapi.Assignment{Partitions: Balance(nil, brokers, topics)}

	// Partition count grows from 3 → 6.
	topicsNew := []TopicSpec{{Name: "events", PartitionCount: 6}}
	out := Balance(prev, brokers, topicsNew)
	if len(out) != 6 {
		t.Fatalf("want 6, got %d", len(out))
	}
	// First three keep prior assignment.
	for i := int32(0); i < 3; i++ {
		var got, want string
		for _, p := range out {
			if p.Topic == "events" && p.Partition == i {
				got = p.Broker
			}
		}
		for _, p := range prev.Partitions {
			if p.Topic == "events" && p.Partition == i {
				want = p.Broker
			}
		}
		if got != want {
			t.Errorf("partition %d should be stable %s, got %s", i, want, got)
		}
	}
	// New partitions land somewhere alive.
	for i := int32(3); i < 6; i++ {
		for _, p := range out {
			if p.Topic == "events" && p.Partition == i {
				if p.Broker != "a" && p.Broker != "b" {
					t.Errorf("partition %d landed on unknown broker %q", i, p.Broker)
				}
			}
		}
	}
}

func TestBalance_NoBrokersReturnsNil(t *testing.T) {
	out := Balance(nil, nil, []TopicSpec{{Name: "x", PartitionCount: 5}})
	if out != nil {
		t.Errorf("Balance with no brokers should return nil, got %+v", out)
	}
}

func TestRendezvousHashIsDeterministic(t *testing.T) {
	a := rendezvousHash("events", 7, "broker-2")
	b := rendezvousHash("events", 7, "broker-2")
	if a != b {
		t.Errorf("rendezvous hash non-deterministic: %d vs %d", a, b)
	}
}

func TestRendezvousPickIsConsistent(t *testing.T) {
	brokers := []string{"a", "b", "c"}
	first := rendezvousPick("events", 3, brokers)
	// Reorder shouldn't change the pick.
	scrambled := []string{"c", "a", "b"}
	second := rendezvousPick("events", 3, scrambled)
	if first != second {
		t.Errorf("rendezvous depends on broker order: %q vs %q", first, second)
	}
}

// samePartitions returns true when two assignment slices represent the
// same (topic, partition) → (broker, epoch) mapping, ignoring slice order.
func samePartitions(a, b []kafkaapi.PartitionAssignment) bool {
	if len(a) != len(b) {
		return false
	}
	keyOf := func(p kafkaapi.PartitionAssignment) string {
		return p.Topic + "/" + itoa(p.Partition)
	}
	ma := map[string]kafkaapi.PartitionAssignment{}
	mb := map[string]kafkaapi.PartitionAssignment{}
	for _, p := range a {
		ma[keyOf(p)] = p
	}
	for _, p := range b {
		mb[keyOf(p)] = p
	}
	keys := make([]string, 0, len(ma))
	for k := range ma {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if ma[k].Broker != mb[k].Broker || ma[k].Epoch != mb[k].Epoch {
			return false
		}
	}
	return true
}

// --- group balancer tests ---

func TestBalanceGroups_FreshClusterDistributesEvenly(t *testing.T) {
	brokers := []string{"a", "b", "c"}
	groups := []GroupSpec{
		{GroupID: "payments"}, {GroupID: "billing"}, {GroupID: "telemetry"},
		{GroupID: "audit"}, {GroupID: "shipping"}, {GroupID: "inventory"},
	}

	out := BalanceGroups(nil, brokers, groups)

	if len(out) != 6 {
		t.Fatalf("want 6 group assignments, got %d", len(out))
	}
	counts := map[string]int{}
	for _, g := range out {
		counts[g.Broker]++
		if g.Epoch != 1 {
			t.Errorf("fresh group %s: epoch=%d, want 1", g.GroupID, g.Epoch)
		}
	}
	for _, b := range brokers {
		if counts[b] == 0 {
			t.Errorf("broker %s got no groups: %v", b, counts)
		}
	}
}

func TestBalanceGroups_StableAcrossNoOpRecompute(t *testing.T) {
	brokers := []string{"a", "b", "c"}
	groups := []GroupSpec{{GroupID: "x"}, {GroupID: "y"}, {GroupID: "z"}}

	first := BalanceGroups(nil, brokers, groups)
	prev := &kafkaapi.Assignment{ConsumerGroups: first}

	second := BalanceGroups(prev, brokers, groups)
	if !sameGroupAssignments(first, second) {
		t.Errorf("BalanceGroups not stable: first=%+v second=%+v", first, second)
	}
}

func TestBalanceGroups_BrokerDeathRebalancesAffectedOnly(t *testing.T) {
	brokers := []string{"a", "b", "c"}
	groups := []GroupSpec{{GroupID: "g0"}, {GroupID: "g1"}, {GroupID: "g2"}, {GroupID: "g3"}, {GroupID: "g4"}, {GroupID: "g5"}}

	first := BalanceGroups(nil, brokers, groups)
	prev := &kafkaapi.Assignment{ConsumerGroups: first}

	// b dies.
	second := BalanceGroups(prev, []string{"a", "c"}, groups)

	moved := 0
	for _, g := range second {
		var was kafkaapi.ConsumerGroupAssignment
		for _, q := range first {
			if q.GroupID == g.GroupID {
				was = q
				break
			}
		}
		if was.Broker != g.Broker {
			moved++
			if g.Epoch <= was.Epoch {
				t.Errorf("group %s moved %s→%s but epoch %d→%d", g.GroupID, was.Broker, g.Broker, was.Epoch, g.Epoch)
			}
			if g.Broker != "a" && g.Broker != "c" {
				t.Errorf("group %s landed on dead broker %q", g.GroupID, g.Broker)
			}
		} else if g.Epoch != was.Epoch {
			t.Errorf("untouched group %s epoch changed %d→%d", g.GroupID, was.Epoch, g.Epoch)
		}
	}
	if moved == 0 {
		t.Error("expected some groups to move when broker b died")
	}
	// Strict-stability: groups originally on a or c must stay put.
	for _, was := range first {
		if was.Broker == "a" || was.Broker == "c" {
			for _, g := range second {
				if g.GroupID == was.GroupID && g.Broker != was.Broker {
					t.Errorf("strict-stability violated: %s on alive broker %s moved to %s",
						was.GroupID, was.Broker, g.Broker)
				}
			}
		}
	}
}

func TestBalanceGroups_NewlyKnownGroupHashedAcrossAlive(t *testing.T) {
	brokers := []string{"a", "b"}
	prev := &kafkaapi.Assignment{ConsumerGroups: BalanceGroups(nil, brokers, []GroupSpec{{GroupID: "alpha"}})}

	out := BalanceGroups(prev, brokers, []GroupSpec{{GroupID: "alpha"}, {GroupID: "beta"}})
	if len(out) != 2 {
		t.Fatalf("want 2, got %d", len(out))
	}
	// alpha stable.
	for _, g := range out {
		if g.GroupID == "alpha" && g.Broker != prev.ConsumerGroups[0].Broker {
			t.Errorf("alpha should be stable %s, got %s", prev.ConsumerGroups[0].Broker, g.Broker)
		}
	}
	// beta lands on a known broker.
	for _, g := range out {
		if g.GroupID == "beta" && g.Broker != "a" && g.Broker != "b" {
			t.Errorf("beta landed on unknown broker %q", g.Broker)
		}
	}
}

func TestBalanceGroups_NoBrokersReturnsNil(t *testing.T) {
	if out := BalanceGroups(nil, nil, []GroupSpec{{GroupID: "x"}}); out != nil {
		t.Errorf("BalanceGroups with no brokers should be nil, got %+v", out)
	}
}

func TestRendezvousPickGroupConsistent(t *testing.T) {
	brokers := []string{"a", "b", "c"}
	first := rendezvousPickGroup("payments", brokers)
	scrambled := []string{"c", "a", "b"}
	if rendezvousPickGroup("payments", scrambled) != first {
		t.Errorf("rendezvous depends on broker order")
	}
}

// sameGroupAssignments returns true when two slices represent the same
// {groupID → (broker, epoch)} mapping, ignoring order.
func sameGroupAssignments(a, b []kafkaapi.ConsumerGroupAssignment) bool {
	if len(a) != len(b) {
		return false
	}
	ma := map[string]kafkaapi.ConsumerGroupAssignment{}
	mb := map[string]kafkaapi.ConsumerGroupAssignment{}
	for _, g := range a {
		ma[g.GroupID] = g
	}
	for _, g := range b {
		mb[g.GroupID] = g
	}
	for k := range ma {
		if ma[k].Broker != mb[k].Broker || ma[k].Epoch != mb[k].Epoch {
			return false
		}
	}
	return true
}

func itoa(v int32) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [10]byte
	n := 0
	for v > 0 {
		buf[n] = byte('0' + v%10)
		v /= 10
		n++
	}
	out := make([]byte, 0, n+1)
	if neg {
		out = append(out, '-')
	}
	for i := n - 1; i >= 0; i-- {
		out = append(out, buf[i])
	}
	return string(out)
}
