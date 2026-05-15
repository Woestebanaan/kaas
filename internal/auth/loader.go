package auth

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"sync"
)

// credFile mirrors the JSON structure written by the operator.
type credFile struct {
	Version int       `json:"version"`
	Users   []credUser `json:"users"`
}

type credUser struct {
	Username string      `json:"username"`
	AuthType string      `json:"authType"`
	Scram    *scramJSON  `json:"scram,omitempty"`
	TLSCN    string      `json:"tlsCN,omitempty"`
	SA       *saJSON     `json:"serviceAccount,omitempty"`
	Quotas   *quotasJSON `json:"quotas,omitempty"`
}

type scramJSON struct {
	Salt       string `json:"salt"`
	StoredKey  string `json:"storedKey"`
	ServerKey  string `json:"serverKey"`
	Iterations int    `json:"iterations"`
}

type saJSON struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type quotasJSON struct {
	ProducerMaxByteRatePerBroker *int64 `json:"producerMaxByteRatePerBroker,omitempty"`
	ConsumerMaxByteRatePerBroker *int64 `json:"consumerMaxByteRatePerBroker,omitempty"`
	RequestPercentage            *int32 `json:"requestPercentage,omitempty"`
}

type loadedCred struct {
	authType    string
	storedKey   []byte
	serverKey   []byte
	salt        []byte
	iterations  int
	tlsCN       string // reverse-lookup key: CN → username
	saNamespace string
	saName      string
	quotas      *Quotas
}

// CredentialLoader reads credentials.json and exposes it as a CredentialStore.
// Reload atomically replaces the in-memory data.
type CredentialLoader struct {
	path string
	mu   sync.RWMutex
	byUsername map[string]*loadedCred
	byCN       map[string]string // CN → username
	bySA       map[string]bool   // "namespace/name" → true
}

func NewCredentialLoader(path string) *CredentialLoader {
	return &CredentialLoader{
		path:       path,
		byUsername: make(map[string]*loadedCred),
		byCN:       make(map[string]string),
		bySA:       make(map[string]bool),
	}
}

// Reload reads the credentials file and atomically replaces in-memory state.
// Returns nil (not an error) when the file does not exist yet.
func (l *CredentialLoader) Reload() error {
	data, err := os.ReadFile(l.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var cf credFile
	if err := json.Unmarshal(data, &cf); err != nil {
		return err
	}

	byUsername := make(map[string]*loadedCred, len(cf.Users))
	byCN := make(map[string]string)
	bySA := make(map[string]bool)

	for _, u := range cf.Users {
		c := &loadedCred{authType: u.AuthType}
		if u.Quotas != nil {
			c.quotas = &Quotas{
				ProducerMaxByteRatePerBroker: u.Quotas.ProducerMaxByteRatePerBroker,
				ConsumerMaxByteRatePerBroker: u.Quotas.ConsumerMaxByteRatePerBroker,
				RequestPercentage:            u.Quotas.RequestPercentage,
			}
		}
		switch u.AuthType {
		case "scram-sha-512":
			if u.Scram != nil {
				c.storedKey, _ = base64.StdEncoding.DecodeString(u.Scram.StoredKey)
				c.serverKey, _ = base64.StdEncoding.DecodeString(u.Scram.ServerKey)
				c.salt, _ = base64.StdEncoding.DecodeString(u.Scram.Salt)
				c.iterations = u.Scram.Iterations
			}
		case "tls":
			c.tlsCN = u.TLSCN
			if c.tlsCN != "" {
				byCN[c.tlsCN] = u.Username
			}
		case "kubernetes-serviceaccount":
			if u.SA != nil {
				c.saNamespace = u.SA.Namespace
				c.saName = u.SA.Name
				bySA[u.SA.Namespace+"/"+u.SA.Name] = true
			}
		}
		byUsername[u.Username] = c
	}

	l.mu.Lock()
	l.byUsername = byUsername
	l.byCN = byCN
	l.bySA = bySA
	l.mu.Unlock()
	return nil
}

// LookupSCRAM returns SCRAM credentials for the given username.
func (l *CredentialLoader) LookupSCRAM(username string) (storedKey, serverKey, salt []byte, iterations int, ok bool) {
	l.mu.RLock()
	c := l.byUsername[username]
	l.mu.RUnlock()
	if c == nil || c.authType != "scram-sha-512" {
		return nil, nil, nil, 0, false
	}
	return c.storedKey, c.serverKey, c.salt, c.iterations, true
}

// LookupTLS returns the username for a given TLS certificate CN.
func (l *CredentialLoader) LookupTLS(cn string) (username string, ok bool) {
	l.mu.RLock()
	u, ok := l.byCN[cn]
	l.mu.RUnlock()
	return u, ok
}

// LookupSA reports whether the given ServiceAccount is registered.
func (l *CredentialLoader) LookupSA(namespace, name string) bool {
	l.mu.RLock()
	ok := l.bySA[namespace+"/"+name]
	l.mu.RUnlock()
	return ok
}

// LookupQuotas returns quota limits for a username (nil if no quotas set).
func (l *CredentialLoader) LookupQuotas(username string) *Quotas {
	l.mu.RLock()
	c := l.byUsername[username]
	l.mu.RUnlock()
	if c == nil {
		return nil
	}
	return c.quotas
}
