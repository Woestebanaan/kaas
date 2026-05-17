package auth

import (
	"math"
	"sync"
	"time"
)

// QuotaEnforcer applies per-user token-bucket rate limits.
// Returns ThrottleTimeMs (0 = no throttle needed).
//
// gh #103 (KIP-546): runtime quota mutations from AlterClientQuotas land
// in `overrides`, which takes precedence over the store on bucket
// creation AND live-updates existing buckets so a `kafka-configs.sh
// --alter` change takes effect on the next Produce/Fetch without waiting
// for cache eviction. Persistence (write-back to KafkaUser CR) is a
// follow-up phase; today the override dies on broker restart.
type QuotaEnforcer struct {
	mu        sync.Mutex
	buckets   map[string]*tokenBucket
	overrides map[string]*Quotas
	store     CredentialStore
}

// QuotaLister is an optional capability for CredentialStore impls that
// can enumerate every user's quotas. DescribeClientQuotas (gh #103)
// uses this to answer "list all entities" requests; stubs that don't
// implement it just contribute the empty set.
type QuotaLister interface {
	ListAllQuotas() map[string]*Quotas
}

type tokenBucket struct {
	producerTokens float64
	consumerTokens float64
	lastRefill     time.Time
	producerRate   float64 // bytes/sec; 0 = unlimited
	consumerRate   float64
}

func NewQuotaEnforcer(store CredentialStore) *QuotaEnforcer {
	return &QuotaEnforcer{
		buckets:   make(map[string]*tokenBucket),
		overrides: make(map[string]*Quotas),
		store:     store,
	}
}

// SetUserQuota installs a runtime quota override for username and
// live-updates the existing bucket (if any) so the next CheckProduce /
// CheckFetch uses the new rate. Pass q==nil to clear the override and
// revert to the store-backed value.
//
// Wire entry-point for AlterClientQuotas (API 49). The override is in-
// memory only; broker restart drops it. The CR write-back path (gh #103
// phase 2) will keep operator-set quotas durable.
func (q *QuotaEnforcer) SetUserQuota(username string, qs *Quotas) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if qs == nil {
		delete(q.overrides, username)
	} else {
		q.overrides[username] = qs
	}
	// Live-update an existing bucket so the next call observes the new
	// rate. Without this, the bucket carries the old rate until the
	// user disconnects + reconnects (which forces a fresh getBucket).
	if b, ok := q.buckets[username]; ok {
		effective := qs
		if effective == nil && q.store != nil {
			effective = q.store.LookupQuotas(username)
		}
		applyQuotasToBucket(b, effective)
	}
}

// DescribeUserQuota returns the effective quota for username: the
// runtime override if set, otherwise the store-backed value. Returns
// nil when the user has no quota configured. Wire entry-point for
// DescribeClientQuotas (API 48) "exact-match user".
func (q *QuotaEnforcer) DescribeUserQuota(username string) *Quotas {
	q.mu.Lock()
	defer q.mu.Unlock()
	if qs, ok := q.overrides[username]; ok {
		return qs
	}
	if q.store != nil {
		return q.store.LookupQuotas(username)
	}
	return nil
}

// ListUserQuotas returns every (user, quota) pair the broker knows
// about — the union of runtime overrides and (if the store supports
// enumeration via QuotaLister) all store-backed entries. Overrides win
// on collision. Used by DescribeClientQuotas (API 48) for the "match
// all users" path that backs `kafka-configs.sh --describe --entity-type
// users` with no entity name.
func (q *QuotaEnforcer) ListUserQuotas() map[string]*Quotas {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]*Quotas)
	if lister, ok := q.store.(QuotaLister); ok && lister != nil {
		for u, qs := range lister.ListAllQuotas() {
			out[u] = qs
		}
	}
	// Runtime overrides win — a `--alter` after a CR-set quota must be
	// visible to the next `--describe`.
	for u, qs := range q.overrides {
		out[u] = qs
	}
	return out
}

// applyQuotasToBucket rewrites a live bucket's rate fields. A nil quotas
// pointer (or a field set to nil) reverts that rate to 0 = unlimited.
// Tokens are not reset — that would let a client that's currently in
// debt escape its throttle by re-issuing a SetUserQuota.
func applyQuotasToBucket(b *tokenBucket, q *Quotas) {
	if q != nil && q.ProducerMaxByteRatePerBroker != nil {
		b.producerRate = float64(*q.ProducerMaxByteRatePerBroker)
	} else {
		b.producerRate = 0
	}
	if q != nil && q.ConsumerMaxByteRatePerBroker != nil {
		b.consumerRate = float64(*q.ConsumerMaxByteRatePerBroker)
	} else {
		b.consumerRate = 0
	}
}

// CheckProduce deducts bytes from the producer bucket and returns ThrottleTimeMs.
func (q *QuotaEnforcer) CheckProduce(principal Principal, bytes int) int32 {
	return q.check(principal, bytes, true)
}

// CheckFetch deducts bytes from the consumer bucket and returns ThrottleTimeMs.
func (q *QuotaEnforcer) CheckFetch(principal Principal, bytes int) int32 {
	return q.check(principal, bytes, false)
}

func (q *QuotaEnforcer) check(principal Principal, bytes int, producer bool) int32 {
	q.mu.Lock()
	defer q.mu.Unlock()

	b := q.getBucket(principal.Name)
	now := time.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.lastRefill = now

	var rate, tokens *float64
	if producer {
		rate = &b.producerRate
		tokens = &b.producerTokens
	} else {
		rate = &b.consumerRate
		tokens = &b.consumerTokens
	}

	if *rate == 0 {
		return 0 // unlimited
	}

	// Refill tokens up to a cap of 1 second's worth.
	*tokens += *rate * elapsed
	if *tokens > *rate {
		*tokens = *rate
	}

	// gh #125: always deduct the requested bytes — let the bucket
	// carry negative balance forward as debt. The previous
	// implementation clamped at 0 on over-allocation, which made
	// concurrent clients sharing a bucket each see an independent
	// throttle delay and effectively burst at N × rate (two pods on
	// a 10 MB/s quota measured ~16-20 MB/s aggregate). With the debt
	// carried forward, the next request on the same bucket sees the
	// negative balance and gets a longer throttle, so the aggregate
	// converges back to `rate`. Mirrors Apache Kafka's quota
	// algorithm (KIP-13).
	*tokens -= float64(bytes)
	if *tokens >= 0 {
		return 0
	}
	// Negative balance → client must wait until refill brings tokens
	// back to 0 before sending again.
	throttleMs := int32(math.Ceil(-*tokens / *rate * 1000))
	return throttleMs
}

func (q *QuotaEnforcer) getBucket(username string) *tokenBucket {
	b, ok := q.buckets[username]
	if !ok {
		b = &tokenBucket{lastRefill: time.Now()}
		// Override (runtime / KIP-546) wins over the store-backed
		// value. Mirror SetUserQuota's resolution order.
		quotas := q.overrides[username]
		if quotas == nil && q.store != nil {
			quotas = q.store.LookupQuotas(username)
		}
		if quotas != nil {
			if quotas.ProducerMaxByteRatePerBroker != nil {
				b.producerRate = float64(*quotas.ProducerMaxByteRatePerBroker)
				b.producerTokens = b.producerRate
			}
			if quotas.ConsumerMaxByteRatePerBroker != nil {
				b.consumerRate = float64(*quotas.ConsumerMaxByteRatePerBroker)
				b.consumerTokens = b.consumerRate
			}
		}
		q.buckets[username] = b
	}
	return b
}
