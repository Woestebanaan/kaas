package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=".spec.replicas"
// +kubebuilder:printcolumn:name="External",type=boolean,JSONPath=".spec.listeners.external.enabled"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"

// KafkaCluster is the top-level skafka cluster configuration. One KafkaCluster per
// installation; the operator reconciles the broker StatefulSet and, if enabled,
// the external listener resources (cert-manager Certificate, per-broker Services,
// Gateway API TLSRoutes).
type KafkaCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaClusterSpec   `json:"spec,omitempty"`
	Status KafkaClusterStatus `json:"status,omitempty"`
}

type KafkaClusterSpec struct {
	// +kubebuilder:validation:Minimum=1
	Replicas  int32                 `json:"replicas"`
	Storage   KafkaClusterStorage   `json:"storage,omitempty"`
	Listeners KafkaClusterListeners `json:"listeners,omitempty"`
}

type KafkaClusterStorage struct {
	ClassName string `json:"className,omitempty"`
	Size      string `json:"size,omitempty"`
}

type KafkaClusterListeners struct {
	Internal KafkaClusterInternalListener `json:"internal,omitempty"`
	External KafkaClusterExternalListener `json:"external,omitempty"`
}

type KafkaClusterInternalListener struct {
	// +kubebuilder:default=9092
	Port int32 `json:"port,omitempty"`
}

type KafkaClusterExternalListener struct {
	Enabled bool `json:"enabled"`
	// +kubebuilder:default=9093
	Port int32 `json:"port,omitempty"`
	// HostnamePattern uses Go fmt-style %d for the broker ordinal.
	// Example: "broker-%d.kafka.example.com"
	HostnamePattern string `json:"hostnamePattern,omitempty"`
	// BootstrapHostname is an optional convenience hostname included in the
	// certificate SANs. Not required for operation.
	BootstrapHostname string                    `json:"bootstrapHostname,omitempty"`
	TLS               KafkaClusterTLSConfig     `json:"tls,omitempty"`
	Gateway           KafkaClusterGatewayConfig `json:"gateway,omitempty"`
	Service           KafkaClusterServiceConfig `json:"service,omitempty"`
}

type KafkaClusterTLSConfig struct {
	CertManager KafkaClusterCertManagerConfig `json:"certManager,omitempty"`
}

type KafkaClusterCertManagerConfig struct {
	Enabled   bool                     `json:"enabled"`
	IssuerRef KafkaClusterIssuerRef    `json:"issuerRef,omitempty"`
}

type KafkaClusterIssuerRef struct {
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=ClusterIssuer;Issuer
	Kind string `json:"kind"`
}

type KafkaClusterGatewayConfig struct {
	Enabled    bool                  `json:"enabled"`
	GatewayRef KafkaClusterGatewayRef `json:"gatewayRef,omitempty"`
}

type KafkaClusterGatewayRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

type KafkaClusterServiceConfig struct {
	Annotations map[string]string `json:"annotations,omitempty"`
}

type KafkaClusterStatus struct {
	BootstrapServers []string           `json:"bootstrapServers,omitempty"`
	Conditions       []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true

type KafkaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaCluster{}, &KafkaClusterList{})
}
