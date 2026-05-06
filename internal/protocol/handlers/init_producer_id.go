package handlers

import (
	"sync/atomic"
	"time"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// InitProducerIdHandler answers API key 22.
//
// Stage A (gh #12): we hand out a fresh, monotonically increasing
// producer ID and epoch=0 on every call — enough so that Java/franz-go
// idempotent producers (the default since Kafka 3.0) get past their
// startup handshake. We do not yet enforce sequence-number semantics
// in the Produce path; that is Stage B of #12.
//
// Transactional producers (request carries a non-empty TransactionalID)
// are not supported. We currently accept the call and return a fresh
// PID anyway — they will misbehave on the first AddPartitionsToTxn,
// which is the surface that signals "transactions are unsupported"
// when those handlers are implemented. For Stage A this is good
// enough to unblock kafka-console-producer and kafka-verifiable-*
// scripts which use idempotent (non-transactional) producers.
//
// Cross-broker uniqueness: the seed is set from time.Now().UnixNano()
// at handler construction so two brokers booted at slightly different
// times almost certainly hand out non-overlapping ID ranges. This
// trades the strong guarantees of Apache Kafka's TransactionCoordinator
// for simplicity; revisit if/when transactions arrive.
type InitProducerIdHandler struct {
	next atomic.Int64
}

func NewInitProducerIdHandler() *InitProducerIdHandler {
	h := &InitProducerIdHandler{}
	// Seed at the wall-clock nanosecond. Positive int63 by construction.
	// 1<<62 sets a high bit so the IDs are clearly distinct from common
	// debug literals like 0/1/2 in logs and snapshots.
	h.next.Store(int64(uint64(time.Now().UnixNano()) | (1 << 62)))
	return h
}

func (h *InitProducerIdHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	if _, err := api.DecodeInitProducerIdRequest(r, version); err != nil {
		return nil, err
	}

	pid := h.next.Add(1)

	resp := &api.InitProducerIdResponse{
		ProducerID:    pid,
		ProducerEpoch: 0,
	}
	w := codec.NewWriter()
	api.EncodeInitProducerIdResponse(w, resp, version)
	return w.Bytes(), nil
}
