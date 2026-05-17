package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// KafkaUserWriter persists AlterClientQuotas mutations to the KafkaUser
// CR (gh #103 phase 2). Optional — when wired, .Handle calls
// UpdateQuotas synchronously per entry; when nil, mutations live only
// in QuotaEnforcer's in-memory override map and die on broker restart.
// Implemented by internal/k8s.KafkaUserWriter.
type KafkaUserWriter interface {
	UpdateQuotas(ctx context.Context, username string, q *auth.Quotas) error
}

// ErrKafkaUserNotFound is the sentinel KafkaUserWriter impls return
// when the named KafkaUser CR doesn't exist. The handler maps this to
// the wire-level error code so a typo in --entity-name surfaces as
// "user not found" rather than a generic UNKNOWN_SERVER_ERROR.
var ErrKafkaUserNotFound = errors.New("kafka user CR not found")

// ---- KIP-546 client-quota wire surface (gh #103) ----
//
// DescribeClientQuotas (key 48) and AlterClientQuotas (key 49) drive
// kafka-configs.sh --entity-type users --describe/--alter and
// AdminClient.{describe,alter}ClientQuotas(). Skafka models the user-
// entity slice today; client-id / ip / compound entities return empty
// on describe and INVALID_REQUEST on alter — Apache rejects unsupported
// entity types similarly when no plugin is configured.
//
// Persistence: mutations land in QuotaEnforcer's in-memory override
// map. They survive across connections but die on broker restart. The
// CR-write-back phase (gh #103 phase 2) will mirror operator-set
// quotas to the KafkaUser CR like CreateTopics → KafkaTopic CR.

// QuotaManager is the runtime-mutation surface needed by the two
// quota handlers. *auth.QuotaEnforcer implements it; tests can
// substitute their own.
type QuotaManager interface {
	DescribeUserQuota(username string) *auth.Quotas
	ListUserQuotas() map[string]*auth.Quotas
	SetUserQuota(username string, q *auth.Quotas)
}

// Wire-format quota keys. The translator between these and the
// auth.Quotas struct lives below in quotaValuesFromAuth /
// quotasFromOps.
const (
	quotaKeyProducerByteRate = "producer_byte_rate"
	quotaKeyConsumerByteRate = "consumer_byte_rate"
	quotaKeyRequestPercent   = "request_percentage"
)

const entityTypeUser = "user"

// Match-type constants from KIP-546.
const (
	quotaMatchExact   int8 = 0
	quotaMatchDefault int8 = 1
	quotaMatchAny     int8 = 2
)

// ---- DescribeClientQuotas (key 48) ----

type DescribeClientQuotasHandler struct {
	quotas     QuotaManager
	authorizer auth.Authorizer
}

func NewDescribeClientQuotasHandler(q QuotaManager, az auth.Authorizer) *DescribeClientQuotasHandler {
	return &DescribeClientQuotasHandler{quotas: q, authorizer: az}
}

func (h *DescribeClientQuotasHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDescribeClientQuotasRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("describe client quotas decode: %w", err)
	}
	resp := &api.DescribeClientQuotasResponse{}
	respond := func() ([]byte, error) {
		w := codec.NewWriter()
		api.EncodeDescribeClientQuotasResponse(w, resp, version)
		return w.Bytes(), nil
	}

	// Cluster:Describe gate. AdminClient maps DescribeClientQuotas to
	// "Describe on Cluster:kafka-cluster" — same scope as DescribeCluster.
	if h.authorizer != nil {
		principal := principalFrom(conn)
		if !h.authorizer.Authorize(principal, auth.Resource{Type: "cluster", Name: "kafka-cluster", PatternType: "literal"}, auth.OpDescribe) {
			resp.ErrorCode = int16(codec.ErrClusterAuthorizationFailed)
			return respond()
		}
	}

	if h.quotas == nil {
		// No quota engine wired (auth disabled): empty entries, no
		// error. AdminClient surfaces "no entities" rather than a
		// confusing UNSUPPORTED.
		return respond()
	}

	// Filter pipeline: build (username, *Quotas) tuples that satisfy
	// every component. Today we only honour user-entity components;
	// compound queries (user + client-id) or non-user entity types
	// reduce to "nothing matches" rather than erroring.
	candidates := h.candidatesForRequest(req)
	for username, q := range candidates {
		if q == nil {
			continue
		}
		entry := api.QuotaEntry{
			Entity: []api.QuotaEntity{{Type: entityTypeUser, Name: username}},
			Values: quotaValuesFromAuth(q),
		}
		if len(entry.Values) == 0 {
			continue
		}
		resp.Entries = append(resp.Entries, entry)
	}
	return respond()
}

// candidatesForRequest resolves the requested filter into a snapshot of
// (username → quotas). Returns nil when the request can't match any
// user (e.g. asks for client-id only).
func (h *DescribeClientQuotasHandler) candidatesForRequest(req *api.DescribeClientQuotasRequest) map[string]*auth.Quotas {
	// No components → match every user (Apache "--describe --entity-type
	// users" with no name).
	if len(req.Components) == 0 {
		return h.quotas.ListUserQuotas()
	}

	// Walk components. Skafka supports a single "user" component; an
	// exact match resolves to one user, default/any resolves to all
	// users. Strict==true with mixed types means "must satisfy every
	// component" — if any component is non-user, no user can satisfy
	// it, so we return nothing.
	var userMatch string
	userAny := false
	for _, c := range req.Components {
		if c.EntityType != entityTypeUser {
			if req.Strict {
				return nil
			}
			// Non-strict: ignore non-user components; we don't model them.
			continue
		}
		switch c.MatchType {
		case quotaMatchExact:
			userMatch = c.MatchName
		case quotaMatchDefault:
			// Default user (== global default) — skafka has no default
			// scope yet, so this matches nothing.
			return nil
		case quotaMatchAny:
			userAny = true
		}
	}
	if userMatch != "" {
		if q := h.quotas.DescribeUserQuota(userMatch); q != nil {
			return map[string]*auth.Quotas{userMatch: q}
		}
		return nil
	}
	if userAny {
		return h.quotas.ListUserQuotas()
	}
	// Components present but no user component matched.
	return nil
}

// quotaValuesFromAuth emits the wire-format (key, value) pairs that a
// non-nil auth.Quotas exposes. Skips fields that are nil (== unset).
func quotaValuesFromAuth(q *auth.Quotas) []api.QuotaValue {
	if q == nil {
		return nil
	}
	var out []api.QuotaValue
	if q.ProducerMaxByteRatePerBroker != nil {
		out = append(out, api.QuotaValue{Key: quotaKeyProducerByteRate, Value: float64(*q.ProducerMaxByteRatePerBroker)})
	}
	if q.ConsumerMaxByteRatePerBroker != nil {
		out = append(out, api.QuotaValue{Key: quotaKeyConsumerByteRate, Value: float64(*q.ConsumerMaxByteRatePerBroker)})
	}
	if q.RequestPercentage != nil {
		out = append(out, api.QuotaValue{Key: quotaKeyRequestPercent, Value: float64(*q.RequestPercentage)})
	}
	return out
}

// ---- AlterClientQuotas (key 49) ----

type AlterClientQuotasHandler struct {
	quotas     QuotaManager
	authorizer auth.Authorizer
	// crw persists mutations to the KafkaUser CR so they survive
	// broker restart. Nil keeps the handler in-memory-only mode —
	// useful for local-dev and tests where there's no apiserver.
	crw KafkaUserWriter
}

func NewAlterClientQuotasHandler(q QuotaManager, az auth.Authorizer) *AlterClientQuotasHandler {
	return &AlterClientQuotasHandler{quotas: q, authorizer: az}
}

// WithCRWriter wires the CR-write-back path (gh #103 phase 2). The
// returned handler patches KafkaUser.spec.quotas on every successful
// alter; the operator's reconciler then materialises the change into
// credentials.json on the shared PVC, closing the GitOps loop.
func (h *AlterClientQuotasHandler) WithCRWriter(crw KafkaUserWriter) *AlterClientQuotasHandler {
	h.crw = crw
	return h
}

func (h *AlterClientQuotasHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeAlterClientQuotasRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("alter client quotas decode: %w", err)
	}

	resp := &api.AlterClientQuotasResponse{}
	respond := func() ([]byte, error) {
		w := codec.NewWriter()
		api.EncodeAlterClientQuotasResponse(w, resp, version)
		return w.Bytes(), nil
	}

	// Cluster:Alter gate. Apache scopes AlterClientQuotas under
	// "Alter on Cluster:kafka-cluster" — same as AlterConfigs.
	principal := principalFrom(conn)
	if h.authorizer != nil {
		if !h.authorizer.Authorize(principal, auth.Resource{Type: "cluster", Name: "kafka-cluster", PatternType: "literal"}, auth.OpAlter) {
			for _, e := range req.Entries {
				resp.Entries = append(resp.Entries, api.AlterQuotaEntryResponse{
					ErrorCode:    int16(codec.ErrClusterAuthorizationFailed),
					ErrorMessage: "cluster Alter required",
					Entity:       e.Entity,
				})
			}
			return respond()
		}
	}

	if h.quotas == nil {
		for _, e := range req.Entries {
			resp.Entries = append(resp.Entries, api.AlterQuotaEntryResponse{
				ErrorCode:    int16(codec.ErrUnsupportedVersion),
				ErrorMessage: "quota engine not wired (auth disabled)",
				Entity:       e.Entity,
			})
		}
		return respond()
	}

	for _, e := range req.Entries {
		username, ok := userEntityName(e.Entity)
		if !ok {
			resp.Entries = append(resp.Entries, api.AlterQuotaEntryResponse{
				ErrorCode:    int16(codec.ErrInvalidRequest),
				ErrorMessage: "only single user-entity entries are supported (skafka has no client-id/ip quota scope)",
				Entity:       e.Entity,
			})
			continue
		}
		// Start from the user's current quota so partial ops (set
		// only producer_byte_rate, leave consumer alone) preserve
		// the other fields. Apache's quota-mutation semantics
		// merge, not replace.
		next := cloneQuotas(h.quotas.DescribeUserQuota(username))
		opErr, opMsg := applyOps(&next, e.Ops)
		if opErr != 0 {
			resp.Entries = append(resp.Entries, api.AlterQuotaEntryResponse{
				ErrorCode:    opErr,
				ErrorMessage: opMsg,
				Entity:       e.Entity,
			})
			continue
		}
		if !req.ValidateOnly {
			effective := quotasOrNil(next)
			// CR write goes first: a write-back failure must NOT
			// leave the in-memory map ahead of the persisted CR.
			// On restart the CR is the source of truth — diverging
			// here would surface as a confusing post-restart revert.
			if h.crw != nil {
				if err := h.crw.UpdateQuotas(context.Background(), username, effective); err != nil {
					code := int16(codec.ErrUnknownServerError)
					msg := err.Error()
					if errors.Is(err, ErrKafkaUserNotFound) {
						// The KafkaUser CR has to exist before quotas
						// can be set. Mirroring "create user, then set
						// quotas" is the operator's job; surface a
						// distinct error so kafka-configs.sh prints
						// something actionable.
						code = int16(codec.ErrInvalidRequest)
						msg = fmt.Sprintf("KafkaUser %q does not exist; create the CR first", username)
					}
					resp.Entries = append(resp.Entries, api.AlterQuotaEntryResponse{
						ErrorCode:    code,
						ErrorMessage: msg,
						Entity:       e.Entity,
					})
					continue
				}
			}
			h.quotas.SetUserQuota(username, effective)
		}
		resp.Entries = append(resp.Entries, api.AlterQuotaEntryResponse{
			Entity: e.Entity,
		})
	}
	return respond()
}

// userEntityName returns the username from a single-entity (user-only)
// entry. Compound entities (e.g. user + client-id) and non-user types
// fail the check so the caller emits INVALID_REQUEST.
func userEntityName(entity []api.QuotaEntity) (string, bool) {
	if len(entity) != 1 {
		return "", false
	}
	if entity[0].Type != entityTypeUser {
		return "", false
	}
	if entity[0].Name == "" {
		// Default-user entity (Name nullable). Skafka has no global
		// default quota scope yet.
		return "", false
	}
	return entity[0].Name, true
}

// applyOps mutates target in place applying each op. Returns
// (errCode, message) on the first failure; on success returns (0, "").
func applyOps(target **auth.Quotas, ops []api.AlterQuotaOp) (int16, string) {
	if *target == nil {
		*target = &auth.Quotas{}
	}
	q := *target
	for _, op := range ops {
		switch op.Key {
		case quotaKeyProducerByteRate:
			if op.Remove {
				q.ProducerMaxByteRatePerBroker = nil
				continue
			}
			v := int64(op.Value)
			q.ProducerMaxByteRatePerBroker = &v
		case quotaKeyConsumerByteRate:
			if op.Remove {
				q.ConsumerMaxByteRatePerBroker = nil
				continue
			}
			v := int64(op.Value)
			q.ConsumerMaxByteRatePerBroker = &v
		case quotaKeyRequestPercent:
			if op.Remove {
				q.RequestPercentage = nil
				continue
			}
			v := int32(op.Value)
			q.RequestPercentage = &v
		default:
			return int16(codec.ErrInvalidRequest), fmt.Sprintf("unsupported quota key %q", op.Key)
		}
	}
	return 0, ""
}

// cloneQuotas makes a shallow copy so a mutation doesn't bleed into
// the store-backed original (CredentialLoader returns the live struct
// for LookupQuotas).
func cloneQuotas(q *auth.Quotas) *auth.Quotas {
	if q == nil {
		return nil
	}
	c := *q
	return &c
}

// quotasOrNil collapses an all-fields-nil Quotas to a nil pointer so
// SetUserQuota interprets it as a "clear the override" signal. Without
// this an alter that removes every key would leave a phantom empty
// quota that masks the store-backed value forever.
func quotasOrNil(q *auth.Quotas) *auth.Quotas {
	if q == nil {
		return nil
	}
	if q.ProducerMaxByteRatePerBroker == nil && q.ConsumerMaxByteRatePerBroker == nil && q.RequestPercentage == nil {
		return nil
	}
	return q
}
