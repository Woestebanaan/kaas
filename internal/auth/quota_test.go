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
	return &Quotas{ProducerByteRate: &p, ConsumerByteRate: &c}
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
