package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// AlterUserScramCredentialsRequest (key 51, v0). KIP-554 runtime
// credential rotation. Backs `kafka-configs.sh --alter --entity-type
// users --entity-name X --add-config 'SCRAM-SHA-512=[iterations=N,
// password=PW]'` and AdminClient.alterUserScramCredentials().
//
// The request is a single batch combining two operations:
//   - Deletions: drop a specific (user, mechanism) credential.
//   - Upsertions: install / overwrite a credential with the client-
//     supplied SaltedPassword. The broker computes storedKey + serverKey
//     from SaltedPassword and persists them; the SaltedPassword itself
//     is never stored. Iterations + salt round-trip verbatim so the
//     SCRAM exchange can regenerate the SaltedPassword from the
//     plaintext password on client login.
//
// v0 is flexible per the KIP-554 spec — compact arrays + tagged fields
// from the start.
type AlterUserScramCredentialsRequest struct {
	Deletions  []ScramCredentialDeletion
	Upsertions []ScramCredentialUpsertion
}

type ScramCredentialDeletion struct {
	Name      string
	Mechanism int8
}

type ScramCredentialUpsertion struct {
	Name           string
	Mechanism      int8 // 1=SCRAM-SHA-256, 2=SCRAM-SHA-512
	Iterations     int32
	Salt           []byte
	SaltedPassword []byte // = PBKDF2(password, salt, iterations); broker
	// computes storedKey + serverKey from this and discards it.
}

type AlterUserScramCredentialsResponse struct {
	ThrottleTimeMs int32
	Results        []AlterUserScramCredentialsResult
}

type AlterUserScramCredentialsResult struct {
	User         string
	ErrorCode    int16
	ErrorMessage string
}

func DecodeAlterUserScramCredentialsRequest(r *codec.Reader, version int16) (*AlterUserScramCredentialsRequest, error) {
	req := &AlterUserScramCredentialsRequest{}
	flexible := true

	readDeletion := func() error {
		var d ScramCredentialDeletion
		n, err := readString(r, flexible)
		if err != nil {
			return err
		}
		d.Name = n
		m, err := r.ReadInt8()
		if err != nil {
			return err
		}
		d.Mechanism = m
		if err := r.ReadTaggedFields(); err != nil {
			return err
		}
		req.Deletions = append(req.Deletions, d)
		return nil
	}
	if err := r.ReadCompactArray(readDeletion); err != nil {
		return nil, err
	}

	readUpsertion := func() error {
		var u ScramCredentialUpsertion
		n, err := readString(r, flexible)
		if err != nil {
			return err
		}
		u.Name = n
		m, err := r.ReadInt8()
		if err != nil {
			return err
		}
		u.Mechanism = m
		it, err := r.ReadInt32()
		if err != nil {
			return err
		}
		u.Iterations = it
		salt, err := r.ReadCompactBytes()
		if err != nil {
			return err
		}
		u.Salt = salt
		sp, err := r.ReadCompactBytes()
		if err != nil {
			return err
		}
		u.SaltedPassword = sp
		if err := r.ReadTaggedFields(); err != nil {
			return err
		}
		req.Upsertions = append(req.Upsertions, u)
		return nil
	}
	if err := r.ReadCompactArray(readUpsertion); err != nil {
		return nil, err
	}
	if err := r.ReadTaggedFields(); err != nil {
		return nil, err
	}
	return req, nil
}

func EncodeAlterUserScramCredentialsResponse(w *codec.Writer, resp *AlterUserScramCredentialsResponse, version int16) {
	flexible := true
	w.WriteInt32(resp.ThrottleTimeMs)
	writeResult := func() {
		for _, rr := range resp.Results {
			writeString(w, rr.User, flexible)
			w.WriteInt16(rr.ErrorCode)
			writeNullableString(w, rr.ErrorMessage, flexible)
			w.WriteEmptyTaggedFields()
		}
	}
	w.WriteCompactArray(len(resp.Results), writeResult)
	w.WriteEmptyTaggedFields()
}
