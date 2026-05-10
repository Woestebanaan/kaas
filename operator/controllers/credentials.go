package controllers

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"

	"golang.org/x/crypto/pbkdf2"
)

const scramIterations = 8192

// CredentialsFile is the in-memory representation of __cluster/credentials.json.
type CredentialsFile struct {
	Version int              `json:"version"`
	Users   []UserCredential `json:"users"`
}

// UserCredential holds authentication and quota data for one user.
type UserCredential struct {
	Username string          `json:"username"`
	AuthType string          `json:"authType"` // scram-sha-512 | tls | kubernetes-serviceaccount
	Scram    *ScramCredential `json:"scram,omitempty"`
	TLSCN    string          `json:"tlsCN,omitempty"`
	SA       *SACredential   `json:"serviceAccount,omitempty"`
	Quotas   *CredQuotas     `json:"quotas,omitempty"`
}

// ScramCredential holds the SCRAM-SHA-512 derived keys (no plaintext password stored).
type ScramCredential struct {
	Salt       string `json:"salt"`       // base64 random 16 bytes
	StoredKey  string `json:"storedKey"`  // base64 H(HMAC(SaltedPw, "Client Key"))
	ServerKey  string `json:"serverKey"`  // base64 HMAC(SaltedPw, "Server Key")
	Iterations int    `json:"iterations"`
}

// SACredential identifies a Kubernetes ServiceAccount used for JWT auth.
type SACredential struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// CredQuotas holds per-user throughput limits.
type CredQuotas struct {
	ProducerByteRate  *int64 `json:"producerByteRate,omitempty"`
	ConsumerByteRate  *int64 `json:"consumerByteRate,omitempty"`
	RequestPercentage *int32 `json:"requestPercentage,omitempty"`
}

func credentialsPath(dataDir string) string {
	return filepath.Join(dataDir, "__cluster", "credentials.json")
}

// readCredentials reads the credentials file, returning an empty struct when absent.
func readCredentials(dataDir string) (*CredentialsFile, error) {
	data, err := os.ReadFile(credentialsPath(dataDir))
	if os.IsNotExist(err) {
		return &CredentialsFile{Version: 1}, nil
	}
	if err != nil {
		return nil, err
	}
	var cf CredentialsFile
	if err := json.Unmarshal(data, &cf); err != nil {
		return nil, err
	}
	return &cf, nil
}

// writeCredentials atomically writes the credentials file (tmp + rename).
func writeCredentials(dataDir string, cf *CredentialsFile) error {
	if err := os.MkdirAll(filepath.Join(dataDir, "__cluster"), 0o775); err != nil {
		return err
	}
	cf.Version = 1
	return writeAtomic(credentialsPath(dataDir), cf)
}

// upsertUser replaces or appends the given credential in the file.
func (cf *CredentialsFile) upsertUser(cred UserCredential) {
	for i, u := range cf.Users {
		if u.Username == cred.Username {
			cf.Users[i] = cred
			return
		}
	}
	cf.Users = append(cf.Users, cred)
}

// removeUser deletes the named user from the file.
func (cf *CredentialsFile) removeUser(username string) {
	out := cf.Users[:0]
	for _, u := range cf.Users {
		if u.Username != username {
			out = append(out, u)
		}
	}
	cf.Users = out
}

// computeScram derives SCRAM-SHA-512 credentials from a plaintext password.
// The password is never stored; only the derived keys are returned.
func computeScram(password string) (*ScramCredential, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}

	saltedPw := pbkdf2.Key([]byte(password), salt, scramIterations, 64, sha512.New)

	clientKey := hmacSHA512(saltedPw, []byte("Client Key"))
	storedKey := sha512sum(clientKey)
	serverKey := hmacSHA512(saltedPw, []byte("Server Key"))

	return &ScramCredential{
		Salt:       base64.StdEncoding.EncodeToString(salt),
		StoredKey:  base64.StdEncoding.EncodeToString(storedKey),
		ServerKey:  base64.StdEncoding.EncodeToString(serverKey),
		Iterations: scramIterations,
	}, nil
}

func hmacSHA512(key, msg []byte) []byte {
	mac := hmac.New(sha512.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}

func sha512sum(data []byte) []byte {
	h := sha512.Sum512(data)
	return h[:]
}
