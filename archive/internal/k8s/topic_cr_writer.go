package k8s

import (
	"context"
	"crypto/sha1"
	"fmt"
	"regexp"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// TopicCRWriter implements handlers.TopicCRWriter against the
// KafkaTopic CR (gh #51). The broker's CreateTopics / DeleteTopics
// handlers call into this so admin-protocol topic ops persist as the
// same source of truth that GitOps / `kubectl apply -f` writes — the
// operator reconciles the CR into partition directories on the shared
// PVC, and every broker's TopicWatcher fires Added/Deleted, so a
// Metadata refresh from any peer sees the change immediately.
//
// Without this writer wired into the handlers (kafka-compat tests, dev
// mode without an apiserver), CreateTopics is best-effort local —
// writes the in-memory TopicRegistry on the broker that received the
// request and nothing else, which is fine for single-broker tests but
// invisible to peers in multi-broker production.
//
// Naming bridge (gh #86): Kafka topic names allow uppercase, dots,
// underscores and up to 249 characters; Kubernetes resource names
// must follow RFC 1123. When the literal Kafka name is RFC-1123
// valid, the CR uses it as both metadata.name and (implicitly via
// EffectiveTopicName) the Kafka name — leaves spec.topicName unset
// for human readability. When it isn't, the CR's metadata.name is
// synthesised from a sha1 prefix and spec.topicName carries the
// literal Kafka name. Operator + TopicWatcher resolve via
// KafkaTopic.EffectiveTopicName().
type TopicCRWriter struct {
	client    client.Client
	namespace string
	argocd    ArgoCDConfig
}

// NewTopicCRWriter builds a writer bound to the given controller-
// runtime client and namespace. The Scheme on the client must have
// v1alpha1 registered. argo controls the optional ArgoCD integration
// (gh #84 + gh #106) and is shared with every other admin-protocol
// CR writer skafka grows in the future (KafkaACL on CreateAcls /
// DeleteAcls, KafkaUser on AlterClientQuotas / AlterUserScramCredentials);
// pass the zero value (ArgoCDConfig{}) on non-ArgoCD installs to
// produce plain CRs with no argocd.argoproj.io/* annotations.
func NewTopicCRWriter(c client.Client, namespace string, argo ArgoCDConfig) *TopicCRWriter {
	return &TopicCRWriter{client: c, namespace: namespace, argocd: argo}
}

// CreateTopic creates a new KafkaTopic CR. Wraps apierrors.IsAlreadyExists
// in handlers.ErrTopicAlreadyExists so the handler can surface
// TOPIC_ALREADY_EXISTS to the client.
//
// configs (gh #33) maps Kafka-wire config keys → values from the
// CreateTopics request — typically what a Kafka Streams client
// sends for changelog/repartition topics: cleanup.policy=compact,
// segment.bytes=1048576, retention.ms=-1, etc. The translation is
// best-effort: known keys land in the typed KafkaTopic Config,
// unknown keys are silently dropped (rejecting on unknown would
// break Streams' setUp because it sends configs skafka doesn't
// honour at runtime yet, e.g. compression.type, message.format.version).
func (w *TopicCRWriter) CreateTopic(ctx context.Context, name string, partitions int32, configs map[string]string) error {
	metaName, topicName := nameForCR(name)
	t := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{
			Name:      metaName,
			Namespace: w.namespace,
			// Annotations come from the shared ArgoCD helper so any
			// future CR writer (KafkaACL, KafkaUser) lands the same
			// shape. Empty config → no annotations.
			Annotations: w.argocd.Annotations("skafka.io", "KafkaTopic", w.namespace, metaName),
		},
		Spec: v1alpha1.KafkaTopicSpec{
			TopicName:  topicName, // empty when metaName == name (clean common case)
			Partitions: partitions,
			Config:     translateConfigs(configs),
		},
	}
	if err := observability.RecordK8sCall(ctx, "Create", "KafkaTopic", func() error {
		return w.client.Create(ctx, t)
	}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("%w: %s", handlers.ErrTopicAlreadyExists, name)
		}
		return fmt.Errorf("create KafkaTopic %s: %w", name, err)
	}
	return nil
}

// translateConfigs maps Kafka-wire topic config keys onto the typed
// KafkaTopic CR Config schema. Keys outside the known set are
// silently ignored (gh #33's contract — be liberal in what we
// accept). Parse errors on int values fall through to "unset"
// rather than rejecting — a malformed numeric is no worse than
// the key not being present.
func translateConfigs(configs map[string]string) v1alpha1.KafkaTopicConfig {
	out := v1alpha1.KafkaTopicConfig{}
	if v, ok := configs["cleanup.policy"]; ok {
		// CRD validates enum: delete | compact | "compact,delete".
		// Pass through verbatim; operator + DescribeConfigs handler
		// surface the value back to clients.
		out.CleanupPolicy = v
	}
	if v, ok := configs["retention.ms"]; ok {
		if n, err := parseInt64(v); err == nil {
			out.RetentionMs = &n
		}
	}
	if v, ok := configs["retention.bytes"]; ok {
		if n, err := parseInt64(v); err == nil {
			out.RetentionBytes = &n
		}
	}
	if v, ok := configs["segment.bytes"]; ok {
		if n, err := parseInt64(v); err == nil {
			out.SegmentBytes = &n
		}
	}
	if v, ok := configs["min.compaction.lag.ms"]; ok {
		if n, err := parseInt64(v); err == nil {
			out.MinCompactionLagMs = &n
		}
	}
	if v, ok := configs["delete.retention.ms"]; ok {
		if n, err := parseInt64(v); err == nil {
			out.DeleteRetentionMs = &n
		}
	}
	return out
}

// parseInt64 wraps strconv.ParseInt with sane defaults. Used by
// translateConfigs; returning err lets the caller decide whether
// to skip the field (we always skip — see the function comment).
func parseInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

// DeleteTopic removes a KafkaTopic CR. Resolves the Kafka name to the
// matching CR via the same nameForCR mapping CreateTopic used. Wraps
// apierrors.IsNotFound in handlers.ErrTopicNotFound so the handler
// can surface UNKNOWN_TOPIC_OR_PARTITION.
func (w *TopicCRWriter) DeleteTopic(ctx context.Context, name string) error {
	metaName, _ := nameForCR(name)
	t := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{
			Name:      metaName,
			Namespace: w.namespace,
		},
	}
	if err := w.client.Delete(ctx, t); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: %s", handlers.ErrTopicNotFound, name)
		}
		return fmt.Errorf("delete KafkaTopic %s: %w", name, err)
	}
	return nil
}

// UpdateTopicConfig patches the KafkaTopic CR's spec.config block
// (gh #9, KIP-339 IncrementalAlterConfigs). Each mutation is one
// (key, op, value) tuple. Op values: 0=SET, 1=DELETE, 2=APPEND,
// 3=SUBTRACT. Today skafka only honours SET and DELETE for the
// scalar-typed config keys; APPEND/SUBTRACT are accepted but
// translated to SET for cleanup.policy ("delete" → "compact,delete"
// when appended). Unknown keys are silently ignored to match
// Apache's "best-effort" semantic for forward-compat.
func (w *TopicCRWriter) UpdateTopicConfig(ctx context.Context, name string, mutations []handlers.TopicConfigMutation) error {
	metaName, _ := nameForCR(name)
	var t v1alpha1.KafkaTopic
	if err := w.client.Get(ctx, types.NamespacedName{Namespace: w.namespace, Name: metaName}, &t); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: %s", handlers.ErrTopicNotFound, name)
		}
		return fmt.Errorf("get KafkaTopic %s: %w", name, err)
	}
	for _, m := range mutations {
		applyTopicConfigMutation(&t.Spec.Config, m)
	}
	if err := w.client.Update(ctx, &t); err != nil {
		return fmt.Errorf("update KafkaTopic %s: %w", name, err)
	}
	return nil
}

// applyTopicConfigMutation maps one Kafka-wire config key+op onto
// the typed KafkaTopic.spec.config schema. SET / DELETE are
// straightforward; APPEND / SUBTRACT only apply to cleanup.policy
// (the one config skafka treats as a comma-list).
func applyTopicConfigMutation(cfg *v1alpha1.KafkaTopicConfig, m handlers.TopicConfigMutation) {
	switch m.Key {
	case "cleanup.policy":
		switch m.Op {
		case 0: // SET
			cfg.CleanupPolicy = m.Value
		case 1: // DELETE
			cfg.CleanupPolicy = ""
		case 2: // APPEND — merge into the existing comma list
			cfg.CleanupPolicy = appendCommaPolicy(cfg.CleanupPolicy, m.Value)
		case 3: // SUBTRACT
			cfg.CleanupPolicy = subtractCommaPolicy(cfg.CleanupPolicy, m.Value)
		}
	case "retention.ms":
		applyInt64Config(&cfg.RetentionMs, m)
	case "retention.bytes":
		applyInt64Config(&cfg.RetentionBytes, m)
	case "segment.bytes":
		applyInt64Config(&cfg.SegmentBytes, m)
	case "min.compaction.lag.ms":
		applyInt64Config(&cfg.MinCompactionLagMs, m)
	case "delete.retention.ms":
		applyInt64Config(&cfg.DeleteRetentionMs, m)
	default:
		// Unknown key — silent drop. Apache returns success here as long
		// as the request shape was valid; rejecting on unknown keys
		// would break clients that send a full config set on every
		// alter (Streams config dump pattern).
	}
}

func applyInt64Config(field **int64, m handlers.TopicConfigMutation) {
	switch m.Op {
	case 0: // SET
		if n, err := parseInt64(m.Value); err == nil {
			*field = &n
		}
	case 1: // DELETE
		*field = nil
	case 2, 3:
		// APPEND/SUBTRACT only make sense for list-valued configs;
		// silently ignore for scalar int64 fields.
	}
}

// appendCommaPolicy adds a value to a comma-separated policy list if
// not already present. Used for cleanup.policy APPEND ops.
func appendCommaPolicy(current, add string) string {
	if current == "" {
		return add
	}
	for _, v := range splitCSV(current) {
		if v == add {
			return current
		}
	}
	return current + "," + add
}

// subtractCommaPolicy removes a value from a comma-separated policy
// list. Returns empty when the result is empty.
func subtractCommaPolicy(current, remove string) string {
	if current == "" {
		return ""
	}
	parts := splitCSV(current)
	out := parts[:0]
	for _, v := range parts {
		if v != remove {
			out = append(out, v)
		}
	}
	return joinCSV(out)
}

// splitCSV / joinCSV are tiny strings.Split/strings.Join wrappers kept
// inline so the writer file doesn't need an extra import.
func splitCSV(s string) []string {
	out := []string{}
	for len(s) > 0 {
		i := indexByte(s, ',')
		if i < 0 {
			out = append(out, s)
			break
		}
		out = append(out, s[:i])
		s = s[i+1:]
	}
	return out
}
func joinCSV(parts []string) string {
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += "," + p
	}
	return out
}
func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// ExpandTopic grows a KafkaTopic CR's partition count (gh #52,
// KIP-195). Apache's contract: count can only grow, never shrink.
// Existing partitions keep their records; new partitions start
// empty. Wraps apierrors.IsNotFound as handlers.ErrTopicNotFound
// and rejects shrink/equal requests with ErrInvalidPartitionCount
// (mapped to INVALID_PARTITIONS=37 on the wire).
func (w *TopicCRWriter) ExpandTopic(ctx context.Context, name string, newCount int32) error {
	metaName, _ := nameForCR(name)
	var t v1alpha1.KafkaTopic
	if err := w.client.Get(ctx, types.NamespacedName{Namespace: w.namespace, Name: metaName}, &t); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: %s", handlers.ErrTopicNotFound, name)
		}
		return fmt.Errorf("get KafkaTopic %s: %w", name, err)
	}
	if newCount <= t.Spec.Partitions {
		return fmt.Errorf("%w: existing=%d requested=%d", handlers.ErrInvalidPartitionCount, t.Spec.Partitions, newCount)
	}
	t.Spec.Partitions = newCount
	if err := w.client.Update(ctx, &t); err != nil {
		return fmt.Errorf("update KafkaTopic %s: %w", name, err)
	}
	return nil
}

// rfc1123 matches the K8s resource-name validation: lowercase
// alphanumerics with single dots/hyphens between, start+end
// alphanumeric, max 253 chars. Kept lenient on length here (caller
// truncates / hashes); this is just the character-class check.
var rfc1123 = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// nameForCR returns the (metadata.name, spec.topicName) pair for a
// given Kafka topic name. When the Kafka name is a valid RFC 1123
// resource name (≤ 253 chars), use it as metadata.name and leave
// spec.topicName empty (Strimzi's recommendation: "It is recommended
// to not set this unless the topic name is not a valid Kubernetes
// resource name."). Otherwise synthesise a deterministic synthetic
// metadata.name and stash the literal Kafka name in spec.topicName.
//
// Determinism is required so re-creating a topic with the same name
// hits the same CR (TOPIC_ALREADY_EXISTS via apierrors.IsAlreadyExists)
// rather than spawning a new CR each time.
func nameForCR(kafkaName string) (metaName, topicName string) {
	if len(kafkaName) <= 253 && rfc1123.MatchString(kafkaName) {
		return kafkaName, ""
	}
	h := sha1.Sum([]byte(kafkaName))
	// 16 hex chars of sha1 is enough for collision resistance at the
	// scale of "topics on one cluster" and keeps the CR name short.
	return fmt.Sprintf("skafka-topic-%x", h[:8]), kafkaName
}
