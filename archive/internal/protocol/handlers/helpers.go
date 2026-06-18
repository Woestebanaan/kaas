package handlers

import (
	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
)

// principalFrom returns the authenticated principal for the connection,
// or an anonymous principal if SASL has not completed.
func principalFrom(conn *connstate.ConnState) auth.Principal {
	if conn != nil && conn.Principal != nil {
		return *conn.Principal
	}
	return auth.Principal{Name: "ANONYMOUS", Kind: "User"}
}
