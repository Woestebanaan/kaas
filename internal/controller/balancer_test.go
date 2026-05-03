package controller

import (
	"sort"
	"testing"

	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

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
