package k8s

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// BrokerIdentity holds the identity of this broker pod derived from the Kubernetes
// downward API. The pod name IS the broker identity — no registration protocol needed.
type BrokerIdentity struct {
	PodName    string // e.g. "skafka-broker-2"
	Ordinal    int32  // e.g. 2 (StatefulSet ordinal suffix)
	Namespace  string
	Host       string // FQDN: "{podName}.{headlessSvc}.{namespace}.svc.cluster.local"
	Port       int32
}

// NewBrokerIdentity reads MY_POD_NAME from the environment and derives the broker
// identity. namespace and headlessSvc are read from SKAFKA_NAMESPACE and
// SKAFKA_HEADLESS_SVC env vars if not provided explicitly (empty string).
func NewBrokerIdentity(namespace, headlessSvc string, port int32) (*BrokerIdentity, error) {
	podName := os.Getenv("MY_POD_NAME")
	if podName == "" {
		return nil, errors.New("k8s: MY_POD_NAME env var not set")
	}
	if namespace == "" {
		namespace = envOr("SKAFKA_NAMESPACE", "default")
	}
	if headlessSvc == "" {
		headlessSvc = envOr("SKAFKA_HEADLESS_SVC", "skafka-headless")
	}

	ordinal, err := parseOrdinal(podName)
	if err != nil {
		return nil, fmt.Errorf("k8s: parse ordinal from %q: %w", podName, err)
	}

	host := fmt.Sprintf("%s.%s.%s.svc.cluster.local", podName, headlessSvc, namespace)
	return &BrokerIdentity{
		PodName:   podName,
		Ordinal:   ordinal,
		Namespace: namespace,
		Host:      host,
		Port:      port,
	}, nil
}

// ParseOrdinalFromIdentity extracts the StatefulSet ordinal from an identity string
// of the form "pod-name-N" or "pod-name-N-suffix".
func ParseOrdinalFromIdentity(identity string) int32 {
	n, err := parseOrdinal(identity)
	if err != nil {
		return -1
	}
	return n
}

// parseOrdinal returns the integer suffix of a hyphen-separated name.
func parseOrdinal(name string) (int32, error) {
	parts := strings.Split(name, "-")
	if len(parts) == 0 {
		return 0, fmt.Errorf("empty name")
	}
	n, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return 0, err
	}
	return int32(n), nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
