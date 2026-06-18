package k8s

import (
	"testing"
)

func TestPreferredDistribution(t *testing.T) {
	// With N brokers and many partitions, each broker should be preferred for ~1/N partitions.
	const numBrokers = 3
	const numPartitions = 300

	counts := make([]int, numBrokers)
	for p := int32(0); p < numPartitions; p++ {
		for ord := int32(0); ord < numBrokers; ord++ {
			if Preferred("test-topic", p, ord, numBrokers) {
				counts[ord]++
			}
		}
	}

	// Verify every partition is preferred by exactly one broker.
	for p := int32(0); p < numPartitions; p++ {
		owners := 0
		for ord := int32(0); ord < numBrokers; ord++ {
			if Preferred("test-topic", p, ord, numBrokers) {
				owners++
			}
		}
		if owners != 1 {
			t.Errorf("partition %d preferred by %d brokers, want exactly 1", p, owners)
		}
	}

	// Verify distribution is roughly even (within 20% of expected).
	expected := numPartitions / numBrokers
	for i, c := range counts {
		if c < expected*80/100 || c > expected*120/100 {
			t.Errorf("broker %d gets %d partitions, expected ~%d", i, c, expected)
		}
	}
}

func TestPreferredSingleBroker(t *testing.T) {
	// With 1 broker, everything is preferred.
	for p := int32(0); p < 10; p++ {
		if !Preferred("topic", p, 0, 1) {
			t.Errorf("partition %d not preferred for single broker", p)
		}
	}
}

func TestPreferredZeroBrokers(t *testing.T) {
	// Edge case: 0 brokers → always preferred (avoid divide-by-zero, prefer over doing nothing).
	if !Preferred("topic", 0, 0, 0) {
		t.Error("expected Preferred=true when numBrokers=0")
	}
}

func TestPreferredDeterministic(t *testing.T) {
	// Same inputs always produce the same answer.
	for i := 0; i < 100; i++ {
		a := Preferred("payments", 5, 2, 4)
		b := Preferred("payments", 5, 2, 4)
		if a != b {
			t.Error("Preferred is not deterministic")
		}
	}
}
