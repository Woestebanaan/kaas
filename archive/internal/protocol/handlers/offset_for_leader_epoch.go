package handlers

import (
	"errors"
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
	"github.com/woestebanaan/skafka/internal/storage"
)

// ---- OffsetForLeaderEpoch (gh #101, API key 23) ----
//
// KIP-101 epoch-aware truncation lookup. Java consumers issue this
// after every Fetch carrying a leader_epoch field, on assign(), and
// during seekToTimestamp to detect "this offset was written by a
// stale leader and never committed" and snap to a safe truncation
// boundary. Pre-#101 skafka responded UNSUPPORTED_VERSION which
// stalled the consumer's epoch-aware code paths.
//
// Skafka is RF=1 (CLAUDE.md non-goal: replication) so the underlying
// failure mode this API was designed for can't happen, but the lookup
// is still served correctly so the contract holds: storage maps the
// requested epoch to the offset just past the last record written
// under that epoch by walking the partition's epoch-prefixed segment
// list. See DiskStorageEngine.OffsetForLeaderEpoch for the full
// semantics.

// OffsetForLeaderEpochLookup is the minimal storage-side surface the
// handler depends on. *storage.DiskStorageEngine implements it via
// the OffsetForLeaderEpoch method; tests can substitute fakes.
type OffsetForLeaderEpochLookup interface {
	OffsetForLeaderEpoch(topic string, partition int32, leaderEpoch int32) (resultEpoch int32, endOffset int64, err error)
}

type OffsetForLeaderEpochHandler struct {
	store OffsetForLeaderEpochLookup
}

func NewOffsetForLeaderEpochHandler(s OffsetForLeaderEpochLookup) *OffsetForLeaderEpochHandler {
	return &OffsetForLeaderEpochHandler{store: s}
}

func (h *OffsetForLeaderEpochHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeOffsetForLeaderEpochRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("offset-for-leader-epoch decode: %w", err)
	}

	resp := &api.OffsetForLeaderEpochResponse{}
	for _, t := range req.Topics {
		tr := api.OffsetForLeaderEpochTopicResponse{Name: t.Name}
		for _, p := range t.Partitions {
			pr := api.OffsetForLeaderEpochPartitionResponse{
				PartitionIndex: p.PartitionIndex,
				LeaderEpoch:    -1,
				EndOffset:      -1,
			}
			if h.store == nil {
				// No storage wired — dev mode / tests. Mirror the
				// "nothing to truncate to" sentinel.
				tr.Partitions = append(tr.Partitions, pr)
				continue
			}
			respEpoch, endOff, lookupErr := h.store.OffsetForLeaderEpoch(t.Name, p.PartitionIndex, p.LeaderEpoch)
			switch {
			case lookupErr == nil:
				pr.LeaderEpoch = respEpoch
				pr.EndOffset = endOff
			case errors.Is(lookupErr, storage.ErrEpochFenced):
				// 74 — FENCED_LEADER_EPOCH: the client's epoch is
				// FROM the future. Consumer must Metadata-refresh
				// and retry against the lower epoch.
				pr.ErrorCode = 74
			case errors.Is(lookupErr, storage.ErrEpochTooOld):
				// 73 — UNKNOWN_LEADER_EPOCH: nothing to truncate to;
				// the requested epoch has aged out of retention.
				// Apache returns (-1, -1) with no error, but the
				// consumer is better served by the typed code.
				pr.ErrorCode = 73
			case errors.Is(lookupErr, storage.ErrUnknownPartition):
				pr.ErrorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
			default:
				pr.ErrorCode = -1 // UNKNOWN_SERVER_ERROR
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}

	w := codec.NewWriter()
	api.EncodeOffsetForLeaderEpochResponse(w, resp, version)
	return w.Bytes(), nil
}
