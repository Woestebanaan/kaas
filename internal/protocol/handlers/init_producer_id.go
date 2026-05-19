package handlers

import (
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// TxnOwnership reports whether this broker is the routed
// transactional coordinator for a given transactional.id (gh #91).
// Mirrors the GroupAssignmentSource shape consumed by the consumer-
// group coordinator (`internal/coordinator.GroupAssignmentSource.
// OwnsGroup`). Production wires `*broker.Coordinator` (its OwnsTxn
// method hashes txnID into the StatefulSet broker set, with an
// alive-subset deterministic fallback). Tests can substitute a fake.
//
// The gate is opt-in: when no ownership source is wired the handler
// keeps today's "every broker hands out a fresh PID" behaviour, so
// boot / local-dev / unit-test wiring stays uninterrupted.
type TxnOwnership interface {
	OwnsTxn(transactionalID string) bool
}

// TxnStateStore is the slim interface InitProducerIdHandler needs
// from the coordinator's TxnStateStore. Defined here to avoid an
// import cycle (handlers → coordinator → handlers via Manager's
// dependencies). Production wires the concrete
// coordinator.TxnStateStore; tests can substitute a fake.
type TxnStateStore interface {
	GetOrAllocate(txnID string, alloc func() int64) (int64, int16, error)
	// GetOrAllocateWithTimeout records the client's requested
	// transaction.timeout.ms so the gh #28 txn-timeout reaper has
	// the per-txn deadline to compare against. Same return shape as
	// GetOrAllocate.
	GetOrAllocateWithTimeout(txnID string, timeoutMs int32, alloc func() int64) (int64, int16, error)
}

// ProducerEpochFencer broadcasts an epoch bump to every partition
// the broker manages so a zombie batch from a previous session
// (still tagged with the old epoch) is fenced even on partitions
// the new session has not yet touched. gh #30. Production wires
// *storage.DiskStorageEngine; tests can substitute a fake.
type ProducerEpochFencer interface {
	FenceProducerEpoch(pid int64, epoch int16)
}

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
	next      atomic.Int64
	txnStore  TxnStateStore       // nil ⇒ stage A behaviour for transactional IDs (always fresh PID)
	fencer    ProducerEpochFencer // nil ⇒ gh #30 cross-partition fence is lazy (only on next Append)
	ownership TxnOwnership        // nil ⇒ gh #91 routing gate disabled (every broker accepts every txn ID)
}

func NewInitProducerIdHandler() *InitProducerIdHandler {
	h := &InitProducerIdHandler{}
	// Seed at the wall-clock nanosecond. Positive int63 by construction.
	// 1<<62 sets a high bit so the IDs are clearly distinct from common
	// debug literals like 0/1/2 in logs and snapshots.
	h.next.Store(int64(uint64(time.Now().UnixNano()) | (1 << 62)))
	return h
}

// WithTxnStateStore enables the gh #22 epoch-fence-on-rejoin path
// for non-empty transactional.id requests. Without this wired,
// transactional clients still get a PID but every InitProducerId
// returns epoch=0 — which means a zombie producer's writes are
// indistinguishable from a fresh one's. With it wired, the second
// call from the same transactional.id gets the same PID with
// epoch+1, and the storage-layer fence (gh #12 stage B) drops
// the zombie's writes with INVALID_PRODUCER_EPOCH (47).
func (h *InitProducerIdHandler) WithTxnStateStore(s TxnStateStore) *InitProducerIdHandler {
	h.txnStore = s
	return h
}

// WithFencer wires the gh #30 cross-partition fence callback. On
// every InitProducerId call where TxnStateStore returns
// epoch > 0 (a rejoin), the handler invokes
// fencer.FenceProducerEpoch(pid, epoch) so any in-flight write
// from the previous session — even on partitions the new session
// has not yet touched — is rejected with INVALID_PRODUCER_EPOCH.
// Without this, B + #22 still fence eventually (as soon as a
// new-epoch batch lands on each partition), but the gap before
// that first batch is a real correctness window.
func (h *InitProducerIdHandler) WithFencer(f ProducerEpochFencer) *InitProducerIdHandler {
	h.fencer = f
	return h
}

// WithTxnOwnership wires the gh #91 transactional-coordinator routing
// gate. When set, an InitProducerId carrying a non-empty
// transactional.id is rejected with NOT_COORDINATOR (16) on every
// broker except the one PickTxnCoordinator routes the txn ID to —
// the producer's Java client will markCoordinatorUnknown and retry
// FindCoordinator (KeyType=transaction; PR 3 wires that side), then
// land on the right broker. Without this, a transactional producer
// reconnecting to a different broker for the same txn ID gets a
// fresh PID with epoch=0 and the gh #22 fence-on-rejoin contract is
// silently broken (zombie writes from the previous session can't be
// distinguished from the new one's).
//
// Only the non-empty (transactional) path is gated. Empty
// transactional.id is the idempotent-producer case (no routing
// concept — there's no per-txnID state) and every broker continues
// to allocate a fresh PID locally.
func (h *InitProducerIdHandler) WithTxnOwnership(o TxnOwnership) *InitProducerIdHandler {
	h.ownership = o
	return h
}

func (h *InitProducerIdHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeInitProducerIdRequest(r, version)
	if err != nil {
		return nil, err
	}

	// gh #91 routing gate. Only the transactional path is routed —
	// idempotent producers (empty txn ID) have no per-key state to
	// pin and every broker can answer locally. Returning before
	// allocate() means the wrong-broker case never bumps the
	// monotonic PID counter or touches the TxnStateStore, so a
	// rapid-fire client retrying FindCoordinator can't burn through
	// the PID space or pollute the store with entries on every
	// broker it bounces off.
	if req.TransactionalID != "" && h.ownership != nil && !h.ownership.OwnsTxn(req.TransactionalID) {
		resp := &api.InitProducerIdResponse{
			ErrorCode:     int16(codec.ErrNotCoordinator),
			ProducerID:    -1,
			ProducerEpoch: -1,
		}
		w := codec.NewWriter()
		api.EncodeInitProducerIdResponse(w, resp, version)
		return w.Bytes(), nil
	}

	pid, epoch := h.allocate(req.TransactionalID, req.TransactionTimeoutMs)

	resp := &api.InitProducerIdResponse{
		ProducerID:    pid,
		ProducerEpoch: epoch,
	}
	w := codec.NewWriter()
	api.EncodeInitProducerIdResponse(w, resp, version)
	return w.Bytes(), nil
}

// allocate is the gh #22 decision point: empty transactional.id
// gets the stage-A "fresh PID, epoch=0" behaviour; a non-empty
// transactional.id with a wired TxnStateStore gets the bump-on-
// rejoin behaviour. A non-empty transactional.id WITHOUT a wired
// store falls back to fresh-PID-every-time and logs once at warn
// — that means the broker is mid-startup or running in a config
// that doesn't support transactions, and a zombie producer
// could still write under its old (PID, epoch).
func (h *InitProducerIdHandler) allocate(txnID string, timeoutMs int32) (int64, int16) {
	if txnID == "" || h.txnStore == nil {
		if txnID != "" && h.txnStore == nil {
			slog.Warn("InitProducerId received transactional.id but no TxnStateStore is wired; "+
				"epoch fence on rejoin disabled (gh #22)", "txn_id", txnID)
		}
		return h.next.Add(1), 0
	}
	pid, epoch, err := h.txnStore.GetOrAllocateWithTimeout(txnID, timeoutMs, func() int64 { return h.next.Add(1) })
	if err != nil {
		// Persistence failure on the txnStore is rare (disk full,
		// I/O error). The producer side has nothing to fall back
		// to, so we surface a fresh PID with epoch=0 and log —
		// the producer can still make progress, just without
		// rejoin fencing this connection.
		slog.Warn("InitProducerId txn store failed; falling back to fresh PID",
			"txn_id", txnID, "err", err)
		return h.next.Add(1), 0
	}
	// gh #30: epoch>0 means this was a rejoin that bumped from a
	// previous session's epoch. Broadcast the bump so any in-flight
	// zombie write is rejected even on partitions the new session
	// has not yet touched. epoch==0 covers two cases — first-ever
	// alloc, and post-overflow rotation (fresh PID) — neither
	// needs fencing because no earlier (PID, epoch) state exists
	// or the PID itself has changed.
	if epoch > 0 && h.fencer != nil {
		h.fencer.FenceProducerEpoch(pid, epoch)
	}
	return pid, epoch
}
