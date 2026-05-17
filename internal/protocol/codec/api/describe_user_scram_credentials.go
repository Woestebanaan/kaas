package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// DescribeUserScramCredentialsRequest (key 50, v0). KIP-554 added in
// Apache 2.7. Backs `kafka-configs.sh --describe --entity-type users`
// SCRAM-credential surface and AdminClient.describeUserScramCredentials().
//
// Apache Kafka 3.7 ships only v0 of this API (flexible from v0). Even
// though the request is conceptually "list of usernames", the field is
// declared at flex level 0 — i.e., compact arrays + tagged fields from
// the start. (DescribeClientQuotas, by contrast, became flexible only
// at v1.)
type DescribeUserScramCredentialsRequest struct {
	// Users is nullable — null means "describe all". Empty list and
	// null are distinct on the wire and skafka preserves the
	// distinction with a separate bool.
	Users    []DescribeUserScramCredentialsUser
	UsersNil bool
}

type DescribeUserScramCredentialsUser struct {
	Name string
}

type DescribeUserScramCredentialsResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	ErrorMessage   string
	Results        []DescribeUserScramCredentialsResult
}

type DescribeUserScramCredentialsResult struct {
	User         string
	ErrorCode    int16
	ErrorMessage string
	Credentials  []ScramCredentialInfo
}

// ScramCredentialInfo intentionally omits salt + stored_key + server_key.
// Apache hides those on describe — the operator stores them, but
// exposing them would let a privileged operator harvest credentials
// for offline attack. Iteration count + mechanism are safe.
type ScramCredentialInfo struct {
	Mechanism  int8 // 1=SCRAM-SHA-256, 2=SCRAM-SHA-512
	Iterations int32
}

func DecodeDescribeUserScramCredentialsRequest(r *codec.Reader, version int16) (*DescribeUserScramCredentialsRequest, error) {
	req := &DescribeUserScramCredentialsRequest{}
	// v0 is already flexible per the KIP-554 spec; treat as such.
	flexible := true

	// Nullable compact array of users.
	n, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		// Null marker — "describe all users".
		req.UsersNil = true
	} else {
		count := int(n - 1)
		for i := 0; i < count; i++ {
			name, err := readString(r, flexible)
			if err != nil {
				return nil, err
			}
			if err := r.ReadTaggedFields(); err != nil {
				return nil, err
			}
			req.Users = append(req.Users, DescribeUserScramCredentialsUser{Name: name})
		}
	}
	if err := r.ReadTaggedFields(); err != nil {
		return nil, err
	}
	return req, nil
}

func EncodeDescribeUserScramCredentialsResponse(w *codec.Writer, resp *DescribeUserScramCredentialsResponse, version int16) {
	flexible := true
	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteInt16(resp.ErrorCode)
	writeNullableString(w, resp.ErrorMessage, flexible)

	writeResults := func() {
		for _, rr := range resp.Results {
			writeString(w, rr.User, flexible)
			w.WriteInt16(rr.ErrorCode)
			writeNullableString(w, rr.ErrorMessage, flexible)
			writeCred := func() {
				for _, c := range rr.Credentials {
					w.WriteInt8(c.Mechanism)
					w.WriteInt32(c.Iterations)
					w.WriteEmptyTaggedFields()
				}
			}
			w.WriteCompactArray(len(rr.Credentials), writeCred)
			w.WriteEmptyTaggedFields()
		}
	}
	w.WriteCompactArray(len(resp.Results), writeResults)
	w.WriteEmptyTaggedFields()
}
