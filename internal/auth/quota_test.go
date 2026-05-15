package auth

import "testing"

type fixedQuotaStore struct{ producer, consumer int64 }

func (s *fixedQuotaStore) LookupSCRAM(_ string) ([]byte, []byte, []byte, int, bool) {
	return nil, nil, nil, 0, false
}
func (s *fixedQuotaStore) LookupTLS(_ string) (string, bool) { return "", false }
func (s *fixedQuotaStore) LookupSA(_, _ string) bool         { return false }
func (s *fixedQuotaStore) LookupQuotas(_ string) *Quotas {
	p, c := s.producer, s.consumer
	return &Quotas{ProducerMaxByteRatePerBroker: &p, ConsumerMaxByteRatePerBroker: &c}
}

func TestQuotaUnderLimit(t *testing.T) {
	q := NewQuotaEnforcer(&fixedQuotaStore{producer: 1000, consumer: 1000})
	p := Principal{Name: "alice"}
	if ms := q.CheckProduce(p, 500); ms != 0 {
		t.Errorf("under limit should not throttle, got %d ms", ms)
	}
}

func TestQuotaOverLimit(t *testing.T) {
	q := NewQuotaEnforcer(&fixedQuotaStore{producer: 1000, consumer: 1000})
	p := Principal{Name: "alice"}
	if ms := q.CheckProduce(p, 5000); ms == 0 {
		t.Error("5x over quota should throttle")
	}
}

func TestQuotaUnlimited(t *testing.T) {
	q := NewQuotaEnforcer(&fixedQuotaStore{producer: 0, consumer: 0})
	p := Principal{Name: "bob"}
	if ms := q.CheckProduce(p, 10_000_000); ms != 0 {
		t.Errorf("zero rate = unlimited, got throttle %d", ms)
	}
}

func TestQuotaPerPrincipal(t *testing.T) {
	q := NewQuotaEnforcer(&fixedQuotaStore{producer: 1000, consumer: 1000})
	// Two different principals have independent buckets.
	q.CheckProduce(Principal{Name: "alice"}, 1000) // drains alice's bucket
	if ms := q.CheckProduce(Principal{Name: "bob"}, 500); ms != 0 {
		t.Errorf("bob should have a fresh bucket, got throttle %d", ms)
	}
}

// TestQuotaMultiClientContention pins gh #125. Two clients sharing
// the same principal must NOT each get an independent throttle when
// the bucket is empty — that lets the aggregate burst at N × rate.
// With the fix, the second client lands on a still-negative bucket
// from the first client's over-allocation and gets a strictly
// larger throttle, ensuring serialised drain.
func TestQuotaMultiClientContention(t *testing.T) {
	q := NewQuotaEnforcer(&fixedQuotaStore{producer: 1000, consumer: 1000})
	p := Principal{Name: "bench-perf"}

	// First request: drain the bucket exactly. throttle == 0 because
	// post-deduct balance is 0, not negative.
	if ms := q.CheckProduce(p, 1000); ms != 0 {
		t.Fatalf("first request (drain to 0): got throttle %d, want 0", ms)
	}
	// Second request, back-to-back: bucket is at 0, request needs
	// 1000 bytes. With the bug, throttle was 1000ms and tokens
	// clamped back to 0 — third client would see the same 1000ms.
	// With the fix, tokens go to -1000 and throttle is 1000ms.
	first := q.CheckProduce(p, 1000)
	if first <= 0 {
		t.Fatalf("second request: got throttle %d, want > 0", first)
	}
	// Third request, also back-to-back: under the buggy
	// implementation this would return the SAME throttle as the
	// second (independent client behavior). Under the fix it must
	// return a LARGER throttle because the bucket is now at -2000.
	second := q.CheckProduce(p, 1000)
	if second <= first {
		t.Errorf("third request throttle %d <= second %d — concurrent clients still burst (gh #125 regression)",
			second, first)
	}
}
