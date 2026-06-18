package auth

import "fmt"

// AuthenticateTLS authenticates a client by their TLS certificate's Common Name.
// Called by the server after TLS handshake when a peer certificate is present.
func (e *RealAuthEngine) AuthenticateTLS(cn string) (Principal, error) {
	username, ok := e.creds.LookupTLS(cn)
	if !ok {
		return Principal{}, fmt.Errorf("tls: unknown certificate CN %q", cn)
	}
	return Principal{Name: username, Kind: "User"}, nil
}
