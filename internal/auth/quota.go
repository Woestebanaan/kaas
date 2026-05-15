package auth

import (
	"math"
	"sync"
	"time"
)

// QuotaEnforcer applies per-user token-bucket rate limits.
// Returns ThrottleTimeMs (0 = no throttle needed).
type QuotaEnforcer struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	store   CredentialStore
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
		buckets: make(map[string]*tokenBucket),
		store:   store,
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
		if q.store != nil {
			if quotas := q.store.LookupQuotas(username); quotas != nil {
				if quotas.ProducerMaxByteRatePerBroker != nil {
					b.producerRate = float64(*quotas.ProducerMaxByteRatePerBroker)
					b.producerTokens = b.producerRate
				}
				if quotas.ConsumerMaxByteRatePerBroker != nil {
					b.consumerRate = float64(*quotas.ConsumerMaxByteRatePerBroker)
					b.consumerTokens = b.consumerRate
				}
			}
		}
		q.buckets[username] = b
	}
	return b
}
