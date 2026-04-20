package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// SAExchange implements SASL/PLAIN for Kubernetes ServiceAccount JWT tokens.
// The PLAIN message carries the JWT in the password field:
//
//	NUL + authzid(empty) + NUL + authcid(empty) + NUL + jwt
//
// The JWT is validated via the Kubernetes TokenReview API.
type SAExchange struct {
	k8s       kubernetes.Interface
	store     CredentialStore
	principal Principal
}

func NewSAExchange(k8s kubernetes.Interface, store CredentialStore) *SAExchange {
	return &SAExchange{k8s: k8s, store: store}
}

func (e *SAExchange) Step(clientMsg []byte) ([]byte, bool, error) {
	// SASL PLAIN: NUL + authzid + NUL + authcid + NUL + password (JWT).
	parts := bytes.SplitN(clientMsg, []byte{0}, 3)
	if len(parts) != 3 {
		return nil, false, errors.New("sasl plain: malformed message")
	}
	jwt := string(parts[2])
	if jwt == "" {
		return nil, false, errors.New("sasl plain: empty token")
	}

	tr := &authv1.TokenReview{
		Spec: authv1.TokenReviewSpec{Token: jwt},
	}
	result, err := e.k8s.AuthenticationV1().TokenReviews().Create(
		context.Background(), tr, metav1.CreateOptions{},
	)
	if err != nil {
		return nil, false, fmt.Errorf("sasl plain: token review: %w", err)
	}
	if !result.Status.Authenticated {
		return nil, false, errors.New("sasl plain: token not authenticated")
	}

	// Username format: "system:serviceaccount:{namespace}:{name}"
	saName, saNamespace, err := parseSAUsername(result.Status.User.Username)
	if err != nil {
		return nil, false, err
	}

	if !e.store.LookupSA(saNamespace, saName) {
		return nil, false, fmt.Errorf("sasl plain: ServiceAccount %s/%s not registered", saNamespace, saName)
	}

	e.principal = Principal{
		Name: fmt.Sprintf("ServiceAccount:%s/%s", saNamespace, saName),
		Kind: "ServiceAccount",
	}
	return nil, true, nil
}

func (e *SAExchange) Principal() Principal { return e.principal }

func parseSAUsername(username string) (name, namespace string, err error) {
	// "system:serviceaccount:{namespace}:{name}"
	parts := strings.Split(username, ":")
	if len(parts) != 4 || parts[0] != "system" || parts[1] != "serviceaccount" {
		return "", "", fmt.Errorf("sasl plain: unexpected TokenReview username format %q", username)
	}
	return parts[3], parts[2], nil
}
