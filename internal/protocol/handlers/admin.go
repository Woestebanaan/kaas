package handlers

// Admin handlers for topic and ACL management.
// CreateTopics/DeleteTopics in production mode write a KafkaTopic CR via
// the optional TopicCRWriter (gh #51); the operator reconciles the CR
// into partition directories on the shared PVC and the local
// TopicWatcher fires `Added` on every broker. Without a CRWriter
// (kafka-compat tests / dev mode) the handler still updates the local
// TopicRegistry so Metadata reflects the change on this broker.
// ACL write operations return NOT_CONTROLLER — ACLs are managed via KafkaAcl CRDs.

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
	"github.com/woestebanaan/skafka/internal/storage"
)

// TopicCRWriter persists topic create/delete intent through the
// KafkaTopic CR (gh #51). Implementations live in internal/k8s. When
// nil on a handler, the broker falls back to local-registry-only
// updates — fine for in-process tests, broken in multi-broker
// production because the other brokers won't observe the change.
//
// configs (gh #33) carries Kafka-wire config names → values from
// the CreateTopics request (cleanup.policy, retention.ms,
// segment.bytes, etc.). Implementations translate the subset they
// recognise into typed KafkaTopic CR fields and silently drop
// unknown keys — Streams clients send a handful of topic-level
// configs at startup; rejecting on an unrecognised key would break
// app-side compatibility for properties skafka doesn't yet honour
// at runtime (e.g. compression.type).
type TopicCRWriter interface {
	CreateTopic(ctx context.Context, name string, partitions int32, configs map[string]string) error
	DeleteTopic(ctx context.Context, name string) error
}

// ErrTopicAlreadyExists / ErrTopicNotFound are sentinels TopicCRWriter
// implementations should wrap (errors.Is-compatible) so handlers can
// surface the right Kafka error code.
var (
	ErrTopicAlreadyExists = errors.New("topic already exists")
	ErrTopicNotFound      = errors.New("topic not found")
)

// TopicWriter is the write side of TopicRegistry (subset needed by admin handlers).
type TopicWriter interface {
	Add(name string, partitions int32)
	Remove(name string)
}

// ---- CreateTopics ----

type CreateTopicsHandler struct {
	topics TopicWriter
	crw    TopicCRWriter
}

func NewCreateTopicsHandler(topics TopicWriter) *CreateTopicsHandler {
	return &CreateTopicsHandler{topics: topics}
}

// WithCRWriter wires the production path: CreateTopics persists a
// KafkaTopic CR; the operator reconciles it into partition dirs on the
// shared PVC and the per-broker TopicWatcher fires Added on every
// broker (so Metadata refreshes from any peer see the new topic).
// Without this, the handler is local-registry-only — works for
// single-broker tests, broken across multi-broker clusters.
func (h *CreateTopicsHandler) WithCRWriter(crw TopicCRWriter) *CreateTopicsHandler {
	h.crw = crw
	return h
}

func (h *CreateTopicsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeCreateTopicsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("create_topics decode: %w", err)
	}

	resp := &api.CreateTopicsResponse{}
	for _, t := range req.Topics {
		result := api.CreatableTopicResult{
			Name:              t.Name,
			NumPartitions:     t.NumPartitions,
			ReplicationFactor: t.ReplicationFactor,
		}
		if !req.ValidateOnly {
			// gh #33: a Streams client uses NumPartitions=-1 +
			// ReplicationFactor=-1 to mean "use the broker default" for
			// internal topics. skafka has no broker-default-partitions
			// concept yet; treat -1 as 1 partition. Real apps tend to
			// override anyway — this is the safety net for the
			// hello-world Streams flow that wants any non-zero count.
			parts := t.NumPartitions
			if parts < 1 {
				parts = 1
			}
			// gh #33: translate Kafka-wire CreateableTopicConfig entries
			// into a flat map for the CR writer. Empty map (no configs)
			// is fine — writer writes a CR with default Config{}.
			configs := make(map[string]string, len(t.Configs))
			for _, c := range t.Configs {
				configs[c.Name] = c.Value
			}
			if h.crw != nil {
				if err := h.crw.CreateTopic(context.Background(), t.Name, parts, configs); err != nil {
					switch {
					case errors.Is(err, ErrTopicAlreadyExists):
						result.ErrorCode = int16(codec.ErrTopicAlreadyExists)
					default:
						result.ErrorCode = int16(codec.ErrInvalidRequest)
						result.ErrorMessage = err.Error()
					}
					resp.Topics = append(resp.Topics, result)
					continue
				}
			}
			// Local-registry update is a fast hint; in CR-writer mode
			// the broker's TopicWatcher will redundantly call
			// b.topics.Add as the CR's Added event fires (idempotent).
			h.topics.Add(t.Name, parts)
		}
		resp.Topics = append(resp.Topics, result)
	}

	w := codec.NewWriter()
	api.EncodeCreateTopicsResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- DeleteTopics ----

type DeleteTopicsHandler struct {
	topics TopicWriter
	crw    TopicCRWriter
}

func NewDeleteTopicsHandler(topics TopicWriter) *DeleteTopicsHandler {
	return &DeleteTopicsHandler{topics: topics}
}

// WithCRWriter mirrors CreateTopicsHandler.WithCRWriter — DeleteTopics
// removes the KafkaTopic CR; the operator's reconciler tears down the
// partition dirs and TopicWatcher fires Deleted on every broker.
func (h *DeleteTopicsHandler) WithCRWriter(crw TopicCRWriter) *DeleteTopicsHandler {
	h.crw = crw
	return h
}

func (h *DeleteTopicsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDeleteTopicsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("delete_topics decode: %w", err)
	}

	resp := &api.DeleteTopicsResponse{}
	for _, name := range req.TopicNames {
		result := api.DeletableTopicResult{Name: name}
		if h.crw != nil {
			if err := h.crw.DeleteTopic(context.Background(), name); err != nil {
				switch {
				case errors.Is(err, ErrTopicNotFound):
					result.ErrorCode = int16(codec.ErrUnknownTopicOrPartition)
				default:
					result.ErrorCode = int16(codec.ErrInvalidRequest)
					result.ErrorMessage = err.Error()
				}
				resp.Responses = append(resp.Responses, result)
				continue
			}
		}
		h.topics.Remove(name)
		resp.Responses = append(resp.Responses, result)
	}

	w := codec.NewWriter()
	api.EncodeDeleteTopicsResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- ACL handlers — NOT_CONTROLLER; managed via KafkaAcl CRD ----

type DescribeAclsHandler struct{}

func NewDescribeAclsHandler() *DescribeAclsHandler { return &DescribeAclsHandler{} }

func (h *DescribeAclsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	resp := &api.DescribeAclsResponse{ErrorCode: 0}
	w := codec.NewWriter()
	api.EncodeDescribeAclsResponse(w, resp, version)
	return w.Bytes(), nil
}

type CreateAclsHandler struct{}

func NewCreateAclsHandler() *CreateAclsHandler { return &CreateAclsHandler{} }

func (h *CreateAclsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeCreateAclsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("create_acls decode: %w", err)
	}
	resp := &api.CreateAclsResponse{}
	for range req.Creations {
		resp.Results = append(resp.Results, api.CreateAclsResult{ErrorCode: 0})
	}
	w := codec.NewWriter()
	api.EncodeCreateAclsResponse(w, resp, version)
	return w.Bytes(), nil
}

type DeleteAclsHandler struct{}

func NewDeleteAclsHandler() *DeleteAclsHandler { return &DeleteAclsHandler{} }

func (h *DeleteAclsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDeleteAclsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("delete_acls decode: %w", err)
	}
	resp := &api.DeleteAclsResponse{}
	for range req.Filters {
		resp.FilterResults = append(resp.FilterResults, api.DeleteAclsFilterResult{ErrorCode: 0})
	}
	w := codec.NewWriter()
	api.EncodeDeleteAclsResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- DescribeConfigs ----
//
// Skafka does not yet support per-topic or per-broker config overrides; this
// handler reports a fixed read-only snapshot of the broker's static defaults
// so admin clients (kafka-configs.sh, kafbat-ui) can render the topic/broker
// pages without erroring out.

type DescribeConfigsHandler struct {
	topics  TopicSource
	brokers BrokerSource
}

func NewDescribeConfigsHandler(topics TopicSource, brokers BrokerSource) *DescribeConfigsHandler {
	return &DescribeConfigsHandler{topics: topics, brokers: brokers}
}

func (h *DescribeConfigsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDescribeConfigsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("describe_configs decode: %w", err)
	}

	resp := &api.DescribeConfigsResponse{}
	for _, res := range req.Resources {
		out := api.DescribeConfigsResult{
			ResourceType: res.ResourceType,
			ResourceName: res.ResourceName,
		}
		switch res.ResourceType {
		case api.ConfigResourceTopic:
			if _, ok := h.topics.Get(res.ResourceName); !ok {
				out.ErrorCode = int16(codec.ErrUnknownTopicOrPartition)
				out.ErrorMessage = "unknown topic"
			} else {
				out.Configs = filterConfigs(topicConfigsFor(h.topics, res.ResourceName), res.ConfigNames, res.ConfigNull)
			}
		case api.ConfigResourceBroker:
			out.Configs = filterConfigs(brokerConfigs(h.brokers), res.ConfigNames, res.ConfigNull)
		default:
			out.ErrorCode = int16(codec.ErrInvalidRequest)
			out.ErrorMessage = "unsupported resource type"
		}
		resp.Results = append(resp.Results, out)
	}

	w := codec.NewWriter()
	api.EncodeDescribeConfigsResponse(w, resp, version)
	return w.Bytes(), nil
}

// topicConfigs returns the static topic-level defaults reported for every
// topic. Values mirror storage.DefaultConfig. Used both as the
// fallback set when the topic source can't surface per-topic
// overrides (test stubs that don't implement TopicConfigSource) and
// as the basis topicConfigsFor merges per-topic CR fields on top of.
func topicConfigs() []api.DescribeConfigsEntry {
	return []api.DescribeConfigsEntry{
		readOnlyEntry("cleanup.policy", "delete"),
		readOnlyEntry("retention.ms", "604800000"),
		readOnlyEntry("retention.bytes", "-1"),
		readOnlyEntry("segment.bytes", "1073741824"),
		readOnlyEntry("min.compaction.lag.ms", "0"),
		readOnlyEntry("delete.retention.ms", "86400000"),
		readOnlyEntry("index.interval.bytes", "4096"),
		readOnlyEntry("compression.type", "producer"),
		readOnlyEntry("min.insync.replicas", "1"),
	}
}

// topicConfigsFor returns the configured + default config entries
// for a specific topic (gh #93). When the topic source exposes
// per-topic configs (TopicConfigSource — the production
// broker.TopicRegistry does, test stubs typically don't), each CR
// override replaces the matching default entry and is marked
// IsDefault=false / ConfigSource=TOPIC_CONFIG so admin tools render
// the actual effective value (Apache Kafka's wire contract).
//
// Topic config keys with no CR override fall through to the broker
// default unchanged, exactly matching Apache Kafka's behaviour for
// keys the user hasn't touched.
func topicConfigsFor(topics TopicSource, name string) []api.DescribeConfigsEntry {
	defaults := topicConfigs()
	cs, ok := topics.(TopicConfigSource)
	if !ok {
		return defaults
	}
	cfg, ok := cs.TopicConfig(name)
	if !ok {
		return defaults
	}
	override := func(key, value string) {
		for i := range defaults {
			if defaults[i].Name == key {
				defaults[i].Value = value
				defaults[i].IsDefault = false
				defaults[i].ConfigSource = api.ConfigSourceDynamicTopic
				return
			}
		}
	}
	if cfg.CleanupPolicy != "" {
		override("cleanup.policy", cfg.CleanupPolicy)
	}
	if cfg.RetentionMs != nil {
		override("retention.ms", strconv.FormatInt(*cfg.RetentionMs, 10))
	}
	if cfg.RetentionBytes != nil {
		override("retention.bytes", strconv.FormatInt(*cfg.RetentionBytes, 10))
	}
	if cfg.SegmentBytes != nil {
		override("segment.bytes", strconv.FormatInt(*cfg.SegmentBytes, 10))
	}
	if cfg.MinCompactionLagMs != nil {
		override("min.compaction.lag.ms", strconv.FormatInt(*cfg.MinCompactionLagMs, 10))
	}
	if cfg.DeleteRetentionMs != nil {
		override("delete.retention.ms", strconv.FormatInt(*cfg.DeleteRetentionMs, 10))
	}
	return defaults
}

func brokerConfigs(brokers BrokerSource) []api.DescribeConfigsEntry {
	self := brokers.Self()
	return []api.DescribeConfigsEntry{
		readOnlyEntry("broker.id", fmt.Sprintf("%d", self.NodeID)),
		readOnlyEntry("listeners", fmt.Sprintf("PLAINTEXT://%s:%d", self.Host, self.Port)),
		readOnlyEntry("auto.create.topics.enable", "false"),
		readOnlyEntry("num.partitions", "1"),
		// Always 1: skafka delegates durability to the CSI layer (CephFS/RBD),
		// not to Kafka-level replication. This is an architectural invariant.
		readOnlyEntry("default.replication.factor", "1"),
		// Surfaces a non-empty value on kafbat's broker page (otherwise
		// "Version: Unknown"). The number is the Apache Kafka version whose
		// protocol surface skafka most closely matches today.
		readOnlyEntry("inter.broker.protocol.version", "3.6"),
		readOnlyEntry("kafka.version", "3.6.0"),
	}
}

func readOnlyEntry(name, value string) api.DescribeConfigsEntry {
	return api.DescribeConfigsEntry{
		Name:         name,
		Value:        value,
		ReadOnly:     true,
		IsDefault:    true, // v0
		ConfigSource: api.ConfigSourceDefault,
	}
}

// filterConfigs honours the client's ConfigNames filter: nil → all, empty → none,
// non-empty → only matching names.
func filterConfigs(all []api.DescribeConfigsEntry, names []string, allRequested bool) []api.DescribeConfigsEntry {
	if allRequested {
		return all
	}
	if len(names) == 0 {
		return nil
	}
	want := make(map[string]struct{}, len(names))
	for _, n := range names {
		want[n] = struct{}{}
	}
	out := make([]api.DescribeConfigsEntry, 0, len(names))
	for _, e := range all {
		if _, ok := want[e.Name]; ok {
			out = append(out, e)
		}
	}
	return out
}

// ---- DescribeLogDirs ----
//
// Reports the byte size of every (topic, partition) on disk. Single-broker
// skafka has exactly one log dir (the configured data directory); offset lag
// is always 0 (no replicas) and isFutureKey is always false (no in-progress
// reassignments).

type DescribeLogDirsHandler struct {
	store  storage.StorageEngine
	topics TopicSource
}

func NewDescribeLogDirsHandler(store storage.StorageEngine, topics TopicSource) *DescribeLogDirsHandler {
	return &DescribeLogDirsHandler{store: store, topics: topics}
}

func (h *DescribeLogDirsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDescribeLogDirsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("describe_log_dirs decode: %w", err)
	}

	// Build the response: one Result for our single log dir, one Topic entry
	// per requested topic (or all known topics if the request had a null filter).
	result := api.DescribeLogDirsResult{LogDir: h.store.DataDir()}

	wanted := buildTopicFilter(h.topics, req)
	for _, te := range wanted {
		topicResp := api.DescribeLogDirsResponseTopic{Name: te.Name}
		for _, p := range te.Partitions {
			topicResp.Partitions = append(topicResp.Partitions, api.DescribeLogDirsResponsePartition{
				PartitionIndex: p,
				PartitionSize:  h.store.PartitionSize(te.Name, p),
			})
		}
		result.Topics = append(result.Topics, topicResp)
	}

	resp := &api.DescribeLogDirsResponse{Results: []api.DescribeLogDirsResult{result}}

	w := codec.NewWriter()
	api.EncodeDescribeLogDirsResponse(w, resp, version)
	return w.Bytes(), nil
}

// describeLogDirsTopic is the in-memory shape we hand to the response builder:
// a topic name plus the list of partition indices the client wants reported.
type describeLogDirsTopic struct {
	Name       string
	Partitions []int32
}

// buildTopicFilter resolves the request's topic/partition filter against the
// broker's known topics. A null Topics array means "every known topic, every
// partition"; an explicit list with empty Partitions means "every partition
// of that named topic"; otherwise the literal list is used.
func buildTopicFilter(topics TopicSource, req *api.DescribeLogDirsRequest) []describeLogDirsTopic {
	if req.TopicNull {
		all := topics.All()
		out := make([]describeLogDirsTopic, 0, len(all))
		for _, t := range all {
			out = append(out, describeLogDirsTopic{Name: t.Name, Partitions: rangeInt32(t.Partitions)})
		}
		return out
	}

	out := make([]describeLogDirsTopic, 0, len(req.Topics))
	for _, t := range req.Topics {
		known, ok := topics.Get(t.Name)
		if !ok {
			continue
		}
		parts := t.Partitions
		if len(parts) == 0 {
			parts = rangeInt32(known)
		}
		out = append(out, describeLogDirsTopic{Name: t.Name, Partitions: parts})
	}
	return out
}

func rangeInt32(n int32) []int32 {
	out := make([]int32, n)
	for i := int32(0); i < n; i++ {
		out[i] = i
	}
	return out
}
