package handlers

// Admin handlers for topic and ACL management.
// CreateTopics/DeleteTopics in production mode write a KafkaTopic CR via
// the optional TopicCRWriter (gh #51); the operator reconciles the CR
// into partition directories on the shared PVC and the local
// TopicWatcher fires `Added` on every broker. Without a CRWriter
// (kafka-compat tests / dev mode) the handler still updates the local
// TopicRegistry so Metadata reflects the change on this broker.
// ACL write operations return NOT_CONTROLLER — ACLs are managed on the
// KafkaUser CR's spec.authorization.acls list (gh #135), not via the
// admin wire protocol.

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
	// ExpandTopic grows the partition count on an existing KafkaTopic
	// CR (gh #52, KIP-195). The operator's reconciler picks up the
	// new count and creates the additional partition directories.
	// Implementations should return ErrTopicNotFound when the CR
	// doesn't exist, and ErrInvalidPartitionCount when the requested
	// count <= the existing one (Apache's contract: CreatePartitions
	// can only grow, never shrink).
	ExpandTopic(ctx context.Context, name string, newCount int32) error
}

// ErrInvalidPartitionCount is returned by TopicCRWriter.ExpandTopic
// when the requested count is not strictly greater than the existing
// partition count. Mirrors Apache's INVALID_PARTITIONS (37) wire code.
var ErrInvalidPartitionCount = errors.New("requested partition count must be greater than existing")

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

// ---- ACL handlers (gh #107) ----
//
// ACLs are authored inline on each KafkaUser CR's
// spec.authorization.acls list (gh #135). The handlers translate the
// AdminClient wire shape (int8 enum codes) into the CR-side string
// representation, then delegate to an ACLCRWriter that patches the
// KafkaUser CR; the operator's reconcileACLs rebuilds /data/__cluster/
// acls.json on next reconcile, and every broker's AclEngine hot-reloads.
//
// Without an ACLCRWriter wired (kafka-compat tests, dev mode without an
// apiserver) the handlers degrade to per-entry NONE_OF_THE_ABOVE errors
// so test paths don't silently lose data; only the wired path produces
// real success.

// ACLBinding is the broker-side representation of one ACL row. Strings
// rather than int8s so the writer can patch directly into the
// v1alpha1.KafkaUserACL fields without re-mapping.
type ACLBinding struct {
	Principal    string // "User:alice"
	ResourceType string // "topic" | "group" | "cluster" | "transactionalId"
	ResourceName string
	PatternType  string // "literal" | "prefix"
	Operation    string // capitalised: "Read", "Write", "All", ...
	Permission   string // "Allow" | "Deny"
	Host         string // round-tripped verbatim; broker ignores
}

// ACLFilter is the filter shape used by Describe and Delete. Empty
// strings mean "any" along that axis (the wire-level ANY=1 codes
// collapse to empty here). For PatternType, "" matches every entry;
// "literal"/"prefix" matches exactly; "match" expands to literal+prefix
// (the operator-side matchesResource semantics — see
// internal/auth/acl.go).
type ACLFilter struct {
	Principal    string
	ResourceType string
	ResourceName string
	PatternType  string
	Operation    string
	Permission   string
	Host         string
}

// ACLCRWriter is the persistence interface for CreateAcls / DeleteAcls
// / DescribeAcls (gh #107). Implementations patch
// KafkaUser.spec.authorization.acls in place; the operator reconciler
// is responsible for materialising acls.json on the shared PVC.
type ACLCRWriter interface {
	CreateACL(ctx context.Context, b ACLBinding) error
	DeleteACLs(ctx context.Context, f ACLFilter) ([]ACLBinding, error)
	ListACLs(ctx context.Context, f ACLFilter) ([]ACLBinding, error)
}

// ErrUnknownPrincipal is returned by ACLCRWriter.CreateACL when the
// binding's principal doesn't correspond to an existing KafkaUser CR.
// Mirrors ErrKafkaUserNotFound for the gh #103/#104 writers — the
// operator owns CR lifecycle; the admin protocol does not auto-create.
var ErrUnknownPrincipal = errors.New("no KafkaUser CR for principal")

// ErrInvalidPrincipal is returned by the writer when the binding's
// principal isn't of the form "User:<name>". Apache Kafka admits
// "Group:" and "ServiceAccount:" prefixes too; skafka maps only to
// KafkaUser today, so non-User principals are rejected.
var ErrInvalidPrincipal = errors.New("principal must be of the form User:<name>")

type DescribeAclsHandler struct {
	crw ACLCRWriter
}

func NewDescribeAclsHandler() *DescribeAclsHandler { return &DescribeAclsHandler{} }

// WithCRWriter wires the production path. nil → empty response (the
// pre-gh #107 stub behavior, preserved so the kafka-compat tests that
// don't run K8s machinery still pass).
func (h *DescribeAclsHandler) WithCRWriter(w ACLCRWriter) *DescribeAclsHandler {
	h.crw = w
	return h
}

func (h *DescribeAclsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDescribeAclsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("describe_acls decode: %w", err)
	}

	resp := &api.DescribeAclsResponse{}
	if h.crw == nil {
		w := codec.NewWriter()
		api.EncodeDescribeAclsResponse(w, resp, version)
		return w.Bytes(), nil
	}

	filter, ferr := wireFilterToACLFilter(req.AclFilter, version)
	if ferr != nil {
		resp.ErrorCode = int16(codec.ErrInvalidRequest)
		resp.ErrorMessage = ferr.Error()
		w := codec.NewWriter()
		api.EncodeDescribeAclsResponse(w, resp, version)
		return w.Bytes(), nil
	}

	bindings, err := h.crw.ListACLs(context.Background(), filter)
	if err != nil {
		resp.ErrorCode = int16(codec.ErrUnknownServerError)
		resp.ErrorMessage = err.Error()
		w := codec.NewWriter()
		api.EncodeDescribeAclsResponse(w, resp, version)
		return w.Bytes(), nil
	}

	resp.Resources = groupBindingsByResource(bindings)
	w := codec.NewWriter()
	api.EncodeDescribeAclsResponse(w, resp, version)
	return w.Bytes(), nil
}

type CreateAclsHandler struct {
	crw ACLCRWriter
}

func NewCreateAclsHandler() *CreateAclsHandler { return &CreateAclsHandler{} }

// WithCRWriter wires the production path. nil → per-entry success
// without persistence (the pre-gh #107 stub, kept for kafka-compat
// tests; not safe for production).
func (h *CreateAclsHandler) WithCRWriter(w ACLCRWriter) *CreateAclsHandler {
	h.crw = w
	return h
}

func (h *CreateAclsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeCreateAclsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("create_acls decode: %w", err)
	}
	resp := &api.CreateAclsResponse{}
	for _, b := range req.Creations {
		result := api.CreateAclsResult{}
		if h.crw == nil {
			resp.Results = append(resp.Results, result)
			continue
		}
		binding, terr := wireBindingToACLBinding(b, version)
		if terr != nil {
			result.ErrorCode = int16(codec.ErrInvalidRequest)
			result.ErrorMessage = terr.Error()
			resp.Results = append(resp.Results, result)
			continue
		}
		if err := h.crw.CreateACL(context.Background(), binding); err != nil {
			switch {
			case errors.Is(err, ErrUnknownPrincipal):
				result.ErrorCode = int16(codec.ErrInvalidRequest)
				result.ErrorMessage = err.Error()
			case errors.Is(err, ErrInvalidPrincipal):
				result.ErrorCode = int16(codec.ErrInvalidRequest)
				result.ErrorMessage = err.Error()
			default:
				result.ErrorCode = int16(codec.ErrUnknownServerError)
				result.ErrorMessage = err.Error()
			}
		}
		resp.Results = append(resp.Results, result)
	}
	w := codec.NewWriter()
	api.EncodeCreateAclsResponse(w, resp, version)
	return w.Bytes(), nil
}

type DeleteAclsHandler struct {
	crw ACLCRWriter
}

func NewDeleteAclsHandler() *DeleteAclsHandler { return &DeleteAclsHandler{} }

func (h *DeleteAclsHandler) WithCRWriter(w ACLCRWriter) *DeleteAclsHandler {
	h.crw = w
	return h
}

func (h *DeleteAclsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDeleteAclsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("delete_acls decode: %w", err)
	}
	resp := &api.DeleteAclsResponse{}
	for _, f := range req.Filters {
		fr := api.DeleteAclsFilterResult{}
		if h.crw == nil {
			resp.FilterResults = append(resp.FilterResults, fr)
			continue
		}
		filter, ferr := wireFilterToACLFilter(f, version)
		if ferr != nil {
			fr.ErrorCode = int16(codec.ErrInvalidRequest)
			fr.ErrorMessage = ferr.Error()
			resp.FilterResults = append(resp.FilterResults, fr)
			continue
		}
		matched, err := h.crw.DeleteACLs(context.Background(), filter)
		if err != nil {
			fr.ErrorCode = int16(codec.ErrUnknownServerError)
			fr.ErrorMessage = err.Error()
			resp.FilterResults = append(resp.FilterResults, fr)
			continue
		}
		for _, m := range matched {
			fr.MatchingACLs = append(fr.MatchingACLs, api.DeleteAclsMatchingACL{
				AclBinding: aclBindingToWire(m, version),
			})
		}
		resp.FilterResults = append(resp.FilterResults, fr)
	}
	w := codec.NewWriter()
	api.EncodeDeleteAclsResponse(w, resp, version)
	return w.Bytes(), nil
}

// wireBindingToACLBinding translates a CreateAcls wire entry into the
// CR-side string shape. Rejects UNKNOWN/ANY enum codes and unsupported
// resource types (DELEGATION_TOKEN, USER) with INVALID_REQUEST since
// CreateAcls callers must specify concrete bindings.
func wireBindingToACLBinding(b api.AclBinding, version int16) (ACLBinding, error) {
	rt, ok := api.ResourceTypeToCR(b.ResourceType)
	if !ok {
		return ACLBinding{}, fmt.Errorf("unsupported resource type: %d", b.ResourceType)
	}
	// v0 has no PatternType field; the wire decoder leaves it as zero.
	// Apache Kafka's v0 semantic is "literal", so we default there.
	patternCode := b.PatternType
	if version < 1 {
		patternCode = api.PatternTypeLiteral
	}
	pt, ok := api.PatternTypeToCR(patternCode)
	if !ok {
		return ACLBinding{}, fmt.Errorf("unsupported pattern type: %d", patternCode)
	}
	if pt == "" {
		// PatternTypeAny isn't valid for Create — it's a filter wildcard.
		return ACLBinding{}, fmt.Errorf("pattern type ANY not valid for create")
	}
	op, ok := api.OperationToCR(b.Operation)
	if !ok || op == "" {
		return ACLBinding{}, fmt.Errorf("unsupported operation: %d", b.Operation)
	}
	perm, ok := api.PermissionToCR(b.Permission)
	if !ok || perm == "" {
		return ACLBinding{}, fmt.Errorf("unsupported permission: %d", b.Permission)
	}
	return ACLBinding{
		Principal:    b.Principal,
		ResourceType: rt,
		ResourceName: b.ResourceName,
		PatternType:  pt,
		Operation:    op,
		Permission:   perm,
		Host:         b.Host,
	}, nil
}

// wireFilterToACLFilter translates a DescribeAcls/DeleteAcls wire
// filter to the CR-side string shape. ANY codes (UNKNOWN/ANY=0/1) and
// nullable empty strings collapse to "" — the writer treats those as
// wildcards.
func wireFilterToACLFilter(f api.AclFilter, version int16) (ACLFilter, error) {
	out := ACLFilter{
		Principal:    f.PrincipalFilter,
		ResourceName: f.ResourceNameFilter,
		Host:         f.HostFilter,
	}
	if f.ResourceTypeFilter != api.ResourceTypeUnknown && f.ResourceTypeFilter != api.ResourceTypeAny {
		rt, ok := api.ResourceTypeToCR(f.ResourceTypeFilter)
		if !ok {
			return ACLFilter{}, fmt.Errorf("unsupported resource type filter: %d", f.ResourceTypeFilter)
		}
		out.ResourceType = rt
	}
	patternCode := f.PatternTypeFilter
	if version < 1 {
		// v0 has no PatternType filter; pre-KIP-290 semantics treated
		// every entry as literal. Filter on literal so callers using v0
		// don't accidentally match prefixed entries.
		patternCode = api.PatternTypeLiteral
	}
	if patternCode != api.PatternTypeUnknown && patternCode != api.PatternTypeAny {
		pt, ok := api.PatternTypeToCR(patternCode)
		if !ok {
			return ACLFilter{}, fmt.Errorf("unsupported pattern type filter: %d", patternCode)
		}
		out.PatternType = pt
	}
	if f.Operation != api.AclOperationUnknown && f.Operation != api.AclOperationAny {
		op, ok := api.OperationToCR(f.Operation)
		if !ok || op == "" {
			return ACLFilter{}, fmt.Errorf("unsupported operation filter: %d", f.Operation)
		}
		out.Operation = op
	}
	if f.PermissionType != api.PermissionTypeUnknown && f.PermissionType != api.PermissionTypeAny {
		p, ok := api.PermissionToCR(f.PermissionType)
		if !ok || p == "" {
			return ACLFilter{}, fmt.Errorf("unsupported permission filter: %d", f.PermissionType)
		}
		out.Permission = p
	}
	return out, nil
}

// aclBindingToWire translates a CR-side binding back into the
// wire-level shape for DescribeAcls / DeleteAcls responses.
func aclBindingToWire(b ACLBinding, _ int16) api.AclBinding {
	return api.AclBinding{
		ResourceType: api.ResourceTypeFromCR(b.ResourceType),
		ResourceName: b.ResourceName,
		PatternType:  api.PatternTypeFromCR(b.PatternType),
		Principal:    b.Principal,
		Host:         b.Host,
		Operation:    api.OperationFromCR(b.Operation),
		Permission:   api.PermissionFromCR(b.Permission),
	}
}

// groupBindingsByResource folds a flat binding list into the
// DescribeAclsResource shape Apache Kafka clients expect: one Resource
// row per (type, name, pattern), with N MatchingACL rows inside.
func groupBindingsByResource(bindings []ACLBinding) []api.DescribeAclsResource {
	type key struct {
		rt, name, pt string
	}
	idx := make(map[key]int)
	out := make([]api.DescribeAclsResource, 0, len(bindings))
	for _, b := range bindings {
		k := key{rt: b.ResourceType, name: b.ResourceName, pt: b.PatternType}
		if _, ok := idx[k]; !ok {
			idx[k] = len(out)
			out = append(out, api.DescribeAclsResource{
				ResourceType: api.ResourceTypeFromCR(b.ResourceType),
				ResourceName: b.ResourceName,
				PatternType:  api.PatternTypeFromCR(b.PatternType),
			})
		}
		i := idx[k]
		out[i].ACLs = append(out[i].ACLs, api.MatchingACL{
			Principal:  b.Principal,
			Host:       b.Host,
			Operation:  api.OperationFromCR(b.Operation),
			Permission: api.PermissionFromCR(b.Permission),
		})
	}
	return out
}

// ---- CreatePartitions (gh #52) ----

// CreatePartitionsHandler grows a topic's partition count at runtime
// via the admin protocol (KIP-195). Mirrors the KafkaTopic CR-write
// flow CreateTopicsHandler uses — the operator's reconciler picks up
// the new count and creates the additional partition directories.
// Without a CRWriter wired (kafka-compat tests, dev mode) the
// handler reports per-topic INVALID_REQUEST so clients see a clean
// error rather than silent success.
type CreatePartitionsHandler struct {
	topics TopicSource
	crw    TopicCRWriter
}

func NewCreatePartitionsHandler(topics TopicSource) *CreatePartitionsHandler {
	return &CreatePartitionsHandler{topics: topics}
}

func (h *CreatePartitionsHandler) WithCRWriter(crw TopicCRWriter) *CreatePartitionsHandler {
	h.crw = crw
	return h
}

func (h *CreatePartitionsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeCreatePartitionsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("create_partitions decode: %w", err)
	}

	resp := &api.CreatePartitionsResponse{}
	for _, t := range req.Topics {
		result := api.CreatePartitionsResult{Name: t.Name}
		// Apache requires the new count > existing — enforced by the
		// writer's ExpandTopic path, but we also check here so the
		// no-CRWriter dev/test path surfaces something sane.
		if t.Count < 1 {
			result.ErrorCode = int16(codec.ErrInvalidRequest)
			result.ErrorMessage = "partition count must be >= 1"
			resp.Results = append(resp.Results, result)
			continue
		}
		if req.ValidateOnly {
			// validate_only never writes; assume success for valid
			// shapes (Apache's contract for the admin tool's
			// --dry-run).
			resp.Results = append(resp.Results, result)
			continue
		}
		if h.crw == nil {
			// Dev/test path: report INVALID_REQUEST so the caller
			// doesn't think their grow succeeded.
			result.ErrorCode = int16(codec.ErrInvalidRequest)
			result.ErrorMessage = "broker not configured for runtime topic mutation"
			resp.Results = append(resp.Results, result)
			continue
		}
		if err := h.crw.ExpandTopic(context.Background(), t.Name, t.Count); err != nil {
			switch {
			case errors.Is(err, ErrTopicNotFound):
				result.ErrorCode = int16(codec.ErrUnknownTopicOrPartition)
			case errors.Is(err, ErrInvalidPartitionCount):
				// Apache's INVALID_PARTITIONS (37). The codec package
				// doesn't name this constant yet; use the raw value
				// (mirrors what the AlterPartitionReassignments path
				// did before its constant was added).
				result.ErrorCode = 37
				result.ErrorMessage = err.Error()
			default:
				result.ErrorCode = int16(codec.ErrInvalidRequest)
				result.ErrorMessage = err.Error()
			}
		}
		resp.Results = append(resp.Results, result)
	}
	w := codec.NewWriter()
	api.EncodeCreatePartitionsResponse(w, resp, version)
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

	// gh #109: live broker config the response advertises. Defaults
	// match Apache 3.7's ServerLogConfigs / KafkaConfig.Defaults so
	// admin clients see the same values whether or not WithBrokerConfig
	// was called.
	autoCreateTopics bool
	numPartitions    int32
}

func NewDescribeConfigsHandler(topics TopicSource, brokers BrokerSource) *DescribeConfigsHandler {
	return &DescribeConfigsHandler{
		topics:           topics,
		brokers:          brokers,
		autoCreateTopics: true, // matches Apache 3.7 default + skafka's gh #109 default
		numPartitions:    1,
	}
}

// WithBrokerConfig wires the live broker config values DescribeConfigs
// advertises. Both keys are surfaced as ConfigSource=DEFAULT_CONFIG
// (read-only; skafka doesn't yet support runtime broker-config
// mutation via AlterConfigs).
func (h *DescribeConfigsHandler) WithBrokerConfig(autoCreateTopics bool, numPartitions int32) *DescribeConfigsHandler {
	h.autoCreateTopics = autoCreateTopics
	if numPartitions < 1 {
		numPartitions = 1
	}
	h.numPartitions = numPartitions
	return h
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
			out.Configs = filterConfigs(brokerConfigs(h.brokers, h.autoCreateTopics, h.numPartitions), res.ConfigNames, res.ConfigNull)
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

func brokerConfigs(brokers BrokerSource, autoCreateTopics bool, numPartitions int32) []api.DescribeConfigsEntry {
	self := brokers.Self()
	return []api.DescribeConfigsEntry{
		readOnlyEntry("broker.id", fmt.Sprintf("%d", self.NodeID)),
		readOnlyEntry("listeners", fmt.Sprintf("PLAINTEXT://%s:%d", self.Host, self.Port)),
		readOnlyEntry("auto.create.topics.enable", strconv.FormatBool(autoCreateTopics)),
		readOnlyEntry("num.partitions", strconv.Itoa(int(numPartitions))),
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
