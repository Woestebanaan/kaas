package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// CredentialStore looks up SCRAM credentials by username.
type CredentialStore interface {
	LookupSCRAM(username string) (storedKey, serverKey, salt []byte, iterations int, ok bool)
	LookupTLS(cn string) (username string, ok bool)
	LookupSA(namespace, name string) bool
	LookupQuotas(username string) *Quotas
}

// ScramExchange implements the server side of SCRAM-SHA-512 (RFC 5802).
type ScramExchange struct {
	store           CredentialStore
	state           int // 0 = expect client-first, 1 = expect client-final
	username        string
	clientFirstBare string
	serverFirst     string
	storedKey       []byte
	serverKey       []byte
	fullNonce       string
	principal       Principal
}

func NewScramExchange(store CredentialStore) *ScramExchange {
	return &ScramExchange{store: store}
}

func (e *ScramExchange) Step(clientMsg []byte) ([]byte, bool, error) {
	switch e.state {
	case 0:
		return e.handleClientFirst(clientMsg)
	case 1:
		return e.handleClientFinal(clientMsg)
	}
	return nil, false, errors.New("scram: unexpected additional message")
}

func (e *ScramExchange) Principal() Principal { return e.principal }

// handleClientFirst processes "n,,n=user,r=clientNonce" and returns the server-first message.
func (e *ScramExchange) handleClientFirst(msg []byte) ([]byte, bool, error) {
	s := string(msg)

	// Strip GS2 header ("n,," or "y,,").
	idx := strings.Index(s, ",,")
	if idx < 0 {
		return nil, false, errors.New("scram: missing GS2 header")
	}
	e.clientFirstBare = s[idx+2:]

	attrs := parseAttrs(e.clientFirstBare)
	username, ok := attrs["n"]
	if !ok {
		return nil, false, errors.New("scram: missing username")
	}
	clientNonce, ok := attrs["r"]
	if !ok {
		return nil, false, errors.New("scram: missing client nonce")
	}
	e.username = username

	storedKey, serverKey, salt, iterations, ok := e.store.LookupSCRAM(username)
	if !ok {
		return nil, false, fmt.Errorf("scram: unknown user %q", username)
	}
	e.storedKey = storedKey
	e.serverKey = serverKey

	// Generate server nonce.
	nonceSuffix := make([]byte, 18)
	if _, err := rand.Read(nonceSuffix); err != nil {
		return nil, false, err
	}
	e.fullNonce = clientNonce + base64.RawStdEncoding.EncodeToString(nonceSuffix)

	e.serverFirst = fmt.Sprintf("r=%s,s=%s,i=%d",
		e.fullNonce,
		base64.StdEncoding.EncodeToString(salt),
		iterations,
	)
	e.state = 1
	return []byte(e.serverFirst), false, nil
}

// handleClientFinal processes "c=biws,r=fullNonce,p=clientProof" and returns "v=serverSig".
func (e *ScramExchange) handleClientFinal(msg []byte) ([]byte, bool, error) {
	s := string(msg)

	// Split off the proof: everything before ",p=" is clientFinalWithoutProof.
	proofIdx := strings.LastIndex(s, ",p=")
	if proofIdx < 0 {
		return nil, false, errors.New("scram: missing client proof")
	}
	clientFinalWithoutProof := s[:proofIdx]
	proofB64 := s[proofIdx+3:]

	attrs := parseAttrs(clientFinalWithoutProof)
	nonce, ok := attrs["r"]
	if !ok || nonce != e.fullNonce {
		return nil, false, errors.New("scram: nonce mismatch")
	}

	clientProof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return nil, false, fmt.Errorf("scram: decode client proof: %w", err)
	}

	authMessage := e.clientFirstBare + "," + e.serverFirst + "," + clientFinalWithoutProof

	// Verify: ClientSignature = HMAC(StoredKey, AuthMessage)
	//         RecoveredKey    = ClientProof XOR ClientSignature
	//         Valid if H(RecoveredKey) == StoredKey
	clientSig := hmacSHA512(e.storedKey, []byte(authMessage))
	if len(clientProof) != len(clientSig) {
		return nil, false, errors.New("scram: invalid proof length")
	}
	recoveredKey := make([]byte, len(clientProof))
	for i := range clientProof {
		recoveredKey[i] = clientProof[i] ^ clientSig[i]
	}
	h := sha512.Sum512(recoveredKey)
	if subtle.ConstantTimeCompare(h[:], e.storedKey) != 1 {
		return nil, false, errors.New("scram: authentication failed")
	}

	// Compute server signature.
	serverSig := hmacSHA512(e.serverKey, []byte(authMessage))
	serverFinal := "v=" + base64.StdEncoding.EncodeToString(serverSig)

	e.principal = Principal{Name: e.username, Kind: "User"}
	return []byte(serverFinal), true, nil
}

// parseAttrs parses a comma-separated "key=value" SCRAM attribute string.
func parseAttrs(s string) map[string]string {
	m := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		if len(part) < 2 || part[1] != '=' {
			continue
		}
		m[string(part[0])] = part[2:]
	}
	return m
}

func hmacSHA512(key, msg []byte) []byte {
	mac := hmac.New(sha512.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}

// iterationsFromString is used in tests; exported for package-internal use only.
func iterationsFromString(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
