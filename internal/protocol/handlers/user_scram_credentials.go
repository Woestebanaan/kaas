package handlers

import (
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// ---- KIP-554 user-SCRAM-credential wire surface (gh #104) ----
//
// DescribeUserScramCredentials (key 50) and AlterUserScramCredentials
// (key 51) drive kafka-configs.sh --entity-type users for SCRAM-cred
// management and AdminClient.{describe,alter}UserScramCredentials().
//
// Skafka models SCRAM-SHA-512 only; mechanism 1 (SHA-256) returns
// UNSUPPORTED_SASL_MECHANISM. The wire-level SaltedPassword (==
// PBKDF2(password, salt, iterations)) is collapsed by the broker into
// stored_key + server_key per the SCRAM spec and persisted via the
// KafkaUserWriter — the operator's reconciler then materialises the
// new credential into credentials.json, and every broker's
// CredentialLoader hot-reloads via inotify.
//
// AdminClient.alterUserScramCredentials is the path operators take
// for credential rotation. The plaintext password never reaches the
// broker; only the PBKDF2 output does, so the wire is reasonably safe
// over TLS (the salt + iteration count are both client-known and
// re-broadcast in the SCRAM exchange anyway).

const (
	scramMechanismSHA512 int8 = 2
)

// SCRAMCredentialStore is the read-only enumeration surface the
// describe handler needs. *auth.CredentialLoader implements it; the
// concrete interface lets tests substitute fakes without spinning up
// a real loader.
type SCRAMCredentialStore interface {
	LookupSCRAM(username string) (storedKey, serverKey, salt []byte, iterations int, ok bool)
	ListAllSCRAMUsers() map[string]auth.SCRAMInfo
}

// SCRAMCredentialWriter persists AlterUserScramCredentials results to
// the KafkaUser CR (gh #104). *internal/k8s.KafkaUserWriter implements
// it via UpsertScramCredential / DeleteScramCredential — the operator's
// reconciler then materialises the change into credentials.json.
type SCRAMCredentialWriter interface {
	UpsertScramCredential(ctx context.Context, username string, salt, storedKey, serverKey []byte, iterations int) error
	DeleteScramCredential(ctx context.Context, username string) error
}

// ---- DescribeUserScramCredentials (key 50) ----

type DescribeUserScramCredentialsHandler struct {
	store      SCRAMCredentialStore
	authorizer auth.Authorizer
}

func NewDescribeUserScramCredentialsHandler(s SCRAMCredentialStore, az auth.Authorizer) *DescribeUserScramCredentialsHandler {
	return &DescribeUserScramCredentialsHandler{store: s, authorizer: az}
}

func (h *DescribeUserScramCredentialsHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDescribeUserScramCredentialsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("describe user scram credentials decode: %w", err)
	}
	resp := &api.DescribeUserScramCredentialsResponse{}
	respond := func() ([]byte, error) {
		w := codec.NewWriter()
		api.EncodeDescribeUserScramCredentialsResponse(w, resp, version)
		return w.Bytes(), nil
	}

	// Cluster:Describe ACL — same scope as DescribeClientQuotas.
	if h.authorizer != nil {
		principal := principalFrom(conn)
		if !h.authorizer.Authorize(principal, auth.Resource{Type: "cluster", Name: "kafka-cluster", PatternType: "literal"}, auth.OpDescribe) {
			resp.ErrorCode = int16(codec.ErrClusterAuthorizationFailed)
			return respond()
		}
	}
	if h.store == nil {
		// No credential store (auth disabled): empty results, no error.
		return respond()
	}

	// Build the username set to describe. UsersNil OR an empty list
	// per Apache's contract both mean "all users". The handler
	// distinguishes them only via the request decoder for round-trip
	// safety; the response is identical.
	var names []string
	if req.UsersNil || len(req.Users) == 0 {
		for u := range h.store.ListAllSCRAMUsers() {
			names = append(names, u)
		}
	} else {
		for _, u := range req.Users {
			names = append(names, u.Name)
		}
	}

	for _, name := range names {
		_, _, _, iterations, ok := h.store.LookupSCRAM(name)
		if !ok {
			// RESOURCE_NOT_FOUND (Apache uses error code 83 for this;
			// skafka tracks it as ErrInvalidRequest to avoid adding a
			// new code for one site). AdminClient surfaces this as
			// ResourceNotFoundException either way.
			resp.Results = append(resp.Results, api.DescribeUserScramCredentialsResult{
				User:         name,
				ErrorCode:    int16(codec.ErrInvalidRequest),
				ErrorMessage: fmt.Sprintf("user %q has no SCRAM credentials", name),
			})
			continue
		}
		resp.Results = append(resp.Results, api.DescribeUserScramCredentialsResult{
			User: name,
			Credentials: []api.ScramCredentialInfo{
				{Mechanism: scramMechanismSHA512, Iterations: int32(iterations)},
			},
		})
	}
	return respond()
}

// ---- AlterUserScramCredentials (key 51) ----

type AlterUserScramCredentialsHandler struct {
	authorizer auth.Authorizer
	crw        SCRAMCredentialWriter
}

func NewAlterUserScramCredentialsHandler(az auth.Authorizer) *AlterUserScramCredentialsHandler {
	return &AlterUserScramCredentialsHandler{authorizer: az}
}

// WithCRWriter wires the KafkaUser-CR mutation path. Without a writer
// the handler degrades to "unsupported" on every entry — the runtime
// SCRAM-rotation surface is meaningless without persistence (the
// plaintext password never reaches the broker, so an in-memory
// override would die on restart).
func (h *AlterUserScramCredentialsHandler) WithCRWriter(w SCRAMCredentialWriter) *AlterUserScramCredentialsHandler {
	h.crw = w
	return h
}

func (h *AlterUserScramCredentialsHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeAlterUserScramCredentialsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("alter user scram credentials decode: %w", err)
	}
	resp := &api.AlterUserScramCredentialsResponse{}
	respond := func() ([]byte, error) {
		w := codec.NewWriter()
		api.EncodeAlterUserScramCredentialsResponse(w, resp, version)
		return w.Bytes(), nil
	}

	// Cluster:Alter gate. Apache scopes AlterUserScramCredentials under
	// Cluster Alter — same as AlterConfigs / AlterClientQuotas.
	principal := principalFrom(conn)
	if h.authorizer != nil {
		if !h.authorizer.Authorize(principal, auth.Resource{Type: "cluster", Name: "kafka-cluster", PatternType: "literal"}, auth.OpAlter) {
			for _, d := range req.Deletions {
				resp.Results = append(resp.Results, api.AlterUserScramCredentialsResult{
					User:         d.Name,
					ErrorCode:    int16(codec.ErrClusterAuthorizationFailed),
					ErrorMessage: "cluster Alter required",
				})
			}
			for _, u := range req.Upsertions {
				resp.Results = append(resp.Results, api.AlterUserScramCredentialsResult{
					User:         u.Name,
					ErrorCode:    int16(codec.ErrClusterAuthorizationFailed),
					ErrorMessage: "cluster Alter required",
				})
			}
			return respond()
		}
	}

	if h.crw == nil {
		// No CR writer — the plaintext password never reaches the
		// broker, so without persistence the rotation can't survive
		// the response round-trip. Surface UNSUPPORTED_VERSION
		// (Apache returns the same when the broker doesn't have a
		// credential plugin wired).
		for _, d := range req.Deletions {
			resp.Results = append(resp.Results, api.AlterUserScramCredentialsResult{
				User:         d.Name,
				ErrorCode:    int16(codec.ErrUnsupportedVersion),
				ErrorMessage: "scram-credential writer not wired",
			})
		}
		for _, u := range req.Upsertions {
			resp.Results = append(resp.Results, api.AlterUserScramCredentialsResult{
				User:         u.Name,
				ErrorCode:    int16(codec.ErrUnsupportedVersion),
				ErrorMessage: "scram-credential writer not wired",
			})
		}
		return respond()
	}

	ctx := context.Background()

	for _, d := range req.Deletions {
		if d.Mechanism != scramMechanismSHA512 {
			resp.Results = append(resp.Results, api.AlterUserScramCredentialsResult{
				User:         d.Name,
				ErrorCode:    int16(codec.ErrUnsupportedSaslMechanism),
				ErrorMessage: fmt.Sprintf("mechanism %d not supported (skafka models SCRAM-SHA-512 only)", d.Mechanism),
			})
			continue
		}
		if err := h.crw.DeleteScramCredential(ctx, d.Name); err != nil {
			resp.Results = append(resp.Results, api.AlterUserScramCredentialsResult{
				User:         d.Name,
				ErrorCode:    mapWriterErr(err),
				ErrorMessage: err.Error(),
			})
			continue
		}
		resp.Results = append(resp.Results, api.AlterUserScramCredentialsResult{User: d.Name})
	}

	for _, u := range req.Upsertions {
		if u.Mechanism != scramMechanismSHA512 {
			resp.Results = append(resp.Results, api.AlterUserScramCredentialsResult{
				User:         u.Name,
				ErrorCode:    int16(codec.ErrUnsupportedSaslMechanism),
				ErrorMessage: fmt.Sprintf("mechanism %d not supported (skafka models SCRAM-SHA-512 only)", u.Mechanism),
			})
			continue
		}
		if len(u.SaltedPassword) == 0 || len(u.Salt) == 0 || u.Iterations <= 0 {
			resp.Results = append(resp.Results, api.AlterUserScramCredentialsResult{
				User:         u.Name,
				ErrorCode:    int16(codec.ErrInvalidRequest),
				ErrorMessage: "salt, salted_password, and iterations are required",
			})
			continue
		}
		// SCRAM spec: storedKey = H(HMAC(saltedPassword, "Client Key")),
		// serverKey = HMAC(saltedPassword, "Server Key"). For SHA-512
		// the H is SHA-512. The salted password is never persisted.
		storedKey, serverKey := deriveScramKeys(u.SaltedPassword, sha512.New)
		if err := h.crw.UpsertScramCredential(ctx, u.Name, u.Salt, storedKey, serverKey, int(u.Iterations)); err != nil {
			resp.Results = append(resp.Results, api.AlterUserScramCredentialsResult{
				User:         u.Name,
				ErrorCode:    mapWriterErr(err),
				ErrorMessage: err.Error(),
			})
			continue
		}
		resp.Results = append(resp.Results, api.AlterUserScramCredentialsResult{User: u.Name})
	}
	return respond()
}

// deriveScramKeys runs the SCRAM key-derivation step (RFC 5802 §3) on
// a salted password: stored_key = H(HMAC(salted_password, "Client Key"))
// and server_key = HMAC(salted_password, "Server Key"). h is the hash
// constructor — SHA-512 for SCRAM-SHA-512.
func deriveScramKeys(saltedPassword []byte, h func() hash.Hash) (storedKey, serverKey []byte) {
	mac := hmac.New(h, saltedPassword)
	mac.Write([]byte("Client Key"))
	clientKey := mac.Sum(nil)

	hh := h()
	hh.Write(clientKey)
	storedKey = hh.Sum(nil)

	mac = hmac.New(h, saltedPassword)
	mac.Write([]byte("Server Key"))
	serverKey = mac.Sum(nil)
	return
}

// mapWriterErr translates k8s/CR-side failures into wire-level codes.
// ErrKafkaUserNotFound surfaces as INVALID_REQUEST with the writer's
// own message — same shape as AlterClientQuotas (gh #103 phase 2).
func mapWriterErr(err error) int16 {
	if errors.Is(err, ErrKafkaUserNotFound) {
		return int16(codec.ErrInvalidRequest)
	}
	return int16(codec.ErrUnknownServerError)
}

// b64 is a small convenience used by the test-side stub writer; keeps
// the production-path import set lean.
var _ = base64.StdEncoding
