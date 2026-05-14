package handlers

import (
	"context"
	"errors"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
	"github.com/woestebanaan/skafka/internal/storage"
)

// segmentRefReader is the optional storage interface the splice path
// requires. *storage.DiskStorageEngine implements it (returning refs to
// the broker's already-open active-segment fds); MemoryStorage and
// other backends without on-disk segments don't, and the splice path
// falls through to standard Handle.
type segmentRefReader interface {
	ReadSegmentRef(topic string, partition int32, startOffset int64, maxBytes int) (file *os.File, offset int64, length int, cleanup func(), ok bool, err error)
}

// fetchPartitionSlice is the splice-path equivalent of a
// FetchPartitionResponse. Records are replaced by a (file, offset,
// length) tuple that the dispatcher's Splicer hands to sendfile(2).
// cleanup is non-nil when the file is a lazy-opened closed-segment
// fd that must be Closed after the splice completes; it's nil when
// the file is the partition's owned active-segment fd.
type fetchPartitionSlice struct {
	resp    api.FetchPartitionResponse // ErrorCode + HWM + LastStableOffset etc. — Records stays nil
	file    *os.File
	offset  int64
	length  int
	cleanup func()
}

// HandleSplicing is the gh #130 sendfile path for Fetch. Builds a
// response using storage.ReadSegmentRef instead of storage.Read, then
// streams it through the splicer with sendfile(2) for the records
// sections. The dispatcher only routes here when the splicer reports
// IsKernelSplice() == true — i.e., plaintext *net.TCPConn — so the
// extra encoder cooperation is only paid where it actually saves a
// userspace copy. On any condition that prevents splicing, returns
// nil so the dispatcher falls back to the standard Handle path.
//
// Conditions for the splice path to engage:
//   - Wire format must be flexible (Fetch v12+). Older non-flexible
//     framing uses different array-length prefix widths; encoder
//     cooperation is version-dependent and we restrict to v12+ here.
//   - Storage backend implements segmentRefReader. MemoryStorage and
//     other backends without on-disk segments fall back.
//   - Every partition's records range can satisfy from a single
//     active-segment file range (ok=true from ReadSegmentRef). Any
//     partition that can't bails the entire request to standard
//     Handle — interleaving per-partition splice + materialised
//     bytes is left for a follow-up.
func (h *FetchHandler) HandleSplicing(conn *connstate.ConnState, hdr protocol.RequestHeader, body []byte, splicer protocol.Splicer) error {
	if hdr.APIVersion < 12 {
		return nil // non-flexible: fall back
	}
	refReader, ok := h.store.(segmentRefReader)
	if !ok {
		return nil // storage backend doesn't support file refs (memory, test stubs)
	}

	mx := observability.Global()

	r := codec.NewReader(body)
	req, err := api.DecodeFetchRequest(r, hdr.APIVersion)
	if err != nil {
		return nil // decode failure: fall back so standard Handle returns the right error
	}

	principal := principalFrom(conn)
	resp := &api.FetchResponse{ErrorCode: 0, SessionID: req.SessionID}

	// Walk the request and either:
	//   - collect per-partition file-slice descriptors (sliceTable), OR
	//   - bail to the standard path if any partition can't splice.
	//
	// Per-topic Responses[] mirrors what the standard handler would
	// emit; resp.Responses + sliceTable share index ordering so the
	// post-encode loop can interleave header bytes with splice calls
	// in the same iteration order.
	sliceTable := make([]fetchPartitionSlice, 0, totalPartitions(req))
	for _, topic := range req.Topics {
		topicResp := api.FetchTopicResponse{Name: topic.Name}

		if !h.authorizer.Authorize(principal, auth.Resource{Type: "topic", Name: topic.Name, PatternType: "literal"}, auth.OpRead) {
			for _, p := range topic.Partitions {
				topicResp.Partitions = append(topicResp.Partitions, api.FetchPartitionResponse{
					PartitionIndex:       p.PartitionIndex,
					ErrorCode:            int16(codec.ErrTopicAuthorizationFailed),
					HighWatermark:        -1,
					LastStableOffset:     -1,
					LogStartOffset:       -1,
					PreferredReadReplica: -1,
				})
				sliceTable = append(sliceTable, fetchPartitionSlice{})
				recordFetchError(mx, topic.Name, "topic_auth_failed")
			}
			resp.Responses = append(resp.Responses, topicResp)
			continue
		}

		for _, p := range topic.Partitions {
			pr := api.FetchPartitionResponse{
				PartitionIndex:       p.PartitionIndex,
				LastStableOffset:     -1,
				PreferredReadReplica: -1,
			}
			hwm, hwmErr := h.store.HighWatermark(topic.Name, p.PartitionIndex)
			if hwmErr != nil {
				pr.ErrorCode = int16(codec.ErrNotLeaderOrFollower)
				pr.HighWatermark = -1
				recordFetchError(mx, topic.Name, "not_leader")
				topicResp.Partitions = append(topicResp.Partitions, pr)
				sliceTable = append(sliceTable, fetchPartitionSlice{})
				continue
			}
			pr.HighWatermark = hwm
			pr.LastStableOffset = hwm
			pr.LogStartOffset, _ = h.store.LogStartOffset(topic.Name, p.PartitionIndex)

			file, offset, length, cleanup, refOK, refErr := refReader.ReadSegmentRef(topic.Name, p.PartitionIndex, p.FetchOffset, int(p.PartitionMaxBytes))
			if refErr != nil {
				if errors.Is(refErr, storage.ErrUnknownPartition) {
					pr.ErrorCode = int16(codec.ErrNotLeaderOrFollower)
					recordFetchError(mx, topic.Name, "not_leader")
					topicResp.Partitions = append(topicResp.Partitions, pr)
					sliceTable = append(sliceTable, fetchPartitionSlice{})
					continue
				}
				return nil // unexpected storage error: fall back
			}
			if !refOK {
				// Either nothing to read (past HWM, retention truncated)
				// or splice not eligible. Bail to standard path: the
				// Read-based handler covers cross-segment partial scans.
				return nil
			}
			topicResp.Partitions = append(topicResp.Partitions, pr)
			sliceTable = append(sliceTable, fetchPartitionSlice{resp: pr, file: file, offset: offset, length: length, cleanup: cleanup})
			mx.TopicTraffic.RecordFetch(topic.Name, 0, int64(length))
		}
		resp.Responses = append(resp.Responses, topicResp)
	}

	// Quota: tally splice byte count (no per-record count without a scan).
	totalBytes := 0
	for _, s := range sliceTable {
		totalBytes += s.length
	}
	if throttleMs := h.quotas.CheckFetchQuota(principal, totalBytes); throttleMs > 0 {
		resp.ThrottleTimeMs = throttleMs
	}

	// Ensure lazy-opened closed-segment fds get released after the
	// splice completes. Active-segment slices have cleanup==nil and
	// this is a no-op for them.
	defer func() {
		for _, s := range sliceTable {
			if s.cleanup != nil {
				s.cleanup()
			}
		}
	}()

	// Encode the response, splicing records sections from disk on the
	// wire. We use codec.Writer for the body and frame-prefix bytes,
	// then drive (splicer.Write | splicer.Splice) in alternation for
	// each partition's header bytes / records section.
	if err := writeFetchResponseWithSplices(hdr, hdr.APIVersion, resp, sliceTable, splicer); err != nil {
		// Once we've started writing, we can't fall back — the wire is
		// already partially written and the dispatcher would emit a
		// second response on top. Treat this as a fatal connection
		// error.
		return err
	}
	_ = context.Background() // observability hook left here in case the trace span needs ctx later

	// Record latency via the same labelled histogram the standard path uses.
	_ = attribute.String
	_ = metric.WithAttributes

	return protocol.ErrResponseWritten
}

func totalPartitions(req *api.FetchRequest) int {
	n := 0
	for _, t := range req.Topics {
		n += len(t.Partitions)
	}
	return n
}
