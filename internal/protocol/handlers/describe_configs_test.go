package handlers

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// configurableTopics is a TopicSource that ALSO implements
// TopicConfigSource — exactly the contract a production
// broker.TopicRegistry satisfies (gh #93). Used to drive the merge
// path in topicConfigsFor.
type configurableTopics struct {
	known   map[string]int32
	configs map[string]TopicConfig
}

func (c *configurableTopics) Get(name string) (int32, bool) {
	p, ok := c.known[name]
	return p, ok
}

func (c *configurableTopics) All() []TopicEntry {
	out := make([]TopicEntry, 0, len(c.known))
	for name, p := range c.known {
		out = append(out, TopicEntry{Name: name, Partitions: p})
	}
	return out
}

func (c *configurableTopics) TopicConfig(name string) (TopicConfig, bool) {
	cfg, ok := c.configs[name]
	return cfg, ok
}

// plainTopics implements only TopicSource — no TopicConfigSource.
// Mirrors test stubs that haven't been updated for gh #93; the
// handler must fall through to broker defaults for these.
type plainTopics struct {
	known map[string]int32
}

func (p *plainTopics) Get(name string) (int32, bool) {
	pp, ok := p.known[name]
	return pp, ok
}
func (p *plainTopics) All() []TopicEntry { return nil }

// TestTopicConfigsFor_CROverridesReplaceDefaults is the gh #93 happy
// path: a topic whose CR sets cleanup.policy=compact +
// retention.ms=-1 + segment.bytes=524288 must surface those values
// (not broker defaults), with each overridden entry marked
// IsDefault=false / ConfigSource=DYNAMIC_TOPIC_CONFIG so admin
// tools (kafka-configs.sh, Kafbat-UI) render them as user-set.
//
// Without this test, a regression in topicConfigsFor (e.g. a
// dropped if-branch on one of the *int64 fields) would silently
// fall back to broker defaults — exactly the gh #93 bug.
func TestTopicConfigsFor_CROverridesReplaceDefaults(t *testing.T) {
	retention := int64(-1)
	segment := int64(524288)
	src := &configurableTopics{
		known: map[string]int32{"changelog": 1},
		configs: map[string]TopicConfig{
			"changelog": {
				CleanupPolicy: "compact",
				RetentionMs:   &retention,
				SegmentBytes:  &segment,
			},
		},
	}

	got := indexConfigs(topicConfigsFor(src, "changelog"))
	expectOverride(t, got, "cleanup.policy", "compact")
	expectOverride(t, got, "retention.ms", "-1")
	expectOverride(t, got, "segment.bytes", "524288")
	// Untouched keys must still be present and marked as defaults.
	expectDefault(t, got, "compression.type", "producer")
	expectDefault(t, got, "min.insync.replicas", "1")
}

// TestTopicConfigsFor_AllOverrideKeysWired pins each translateConfigs-
// recognised CR field to its DescribeConfigs key. Catches a
// refactor that drops one of the override branches in
// topicConfigsFor (e.g. forgetting min.compaction.lag.ms when
// adding a new key).
func TestTopicConfigsFor_AllOverrideKeysWired(t *testing.T) {
	retentionMs := int64(86400000)
	retentionBytes := int64(1073741824)
	segmentBytes := int64(2097152)
	minLag := int64(5000)
	deleteRetention := int64(43200000)
	src := &configurableTopics{
		known: map[string]int32{"all": 1},
		configs: map[string]TopicConfig{
			"all": {
				CleanupPolicy:      "compact,delete",
				RetentionMs:        &retentionMs,
				RetentionBytes:     &retentionBytes,
				SegmentBytes:       &segmentBytes,
				MinCompactionLagMs: &minLag,
				DeleteRetentionMs:  &deleteRetention,
			},
		},
	}
	got := indexConfigs(topicConfigsFor(src, "all"))
	expectOverride(t, got, "cleanup.policy", "compact,delete")
	expectOverride(t, got, "retention.ms", "86400000")
	expectOverride(t, got, "retention.bytes", "1073741824")
	expectOverride(t, got, "segment.bytes", "2097152")
	expectOverride(t, got, "min.compaction.lag.ms", "5000")
	expectOverride(t, got, "delete.retention.ms", "43200000")
}

// TestTopicConfigsFor_NoOverridesReturnsDefaults: a topic that
// exists in the source but has no CR-set overrides (zero-value
// TopicConfig) returns the broker defaults verbatim with
// IsDefault=true. Companion to the override test — confirms the
// merge fallthrough.
func TestTopicConfigsFor_NoOverridesReturnsDefaults(t *testing.T) {
	src := &configurableTopics{
		known:   map[string]int32{"plain": 1},
		configs: map[string]TopicConfig{"plain": {}}, // explicit but empty
	}
	got := indexConfigs(topicConfigsFor(src, "plain"))
	expectDefault(t, got, "cleanup.policy", "delete")
	expectDefault(t, got, "retention.ms", "604800000")
	expectDefault(t, got, "segment.bytes", "1073741824")
}

// TestTopicConfigsFor_NonConfigSourceFallsThrough: a TopicSource
// that doesn't implement TopicConfigSource (legacy test stubs)
// gets pure broker defaults — proves the type-assert fallback in
// topicConfigsFor works.
func TestTopicConfigsFor_NonConfigSourceFallsThrough(t *testing.T) {
	src := &plainTopics{known: map[string]int32{"legacy": 1}}
	got := indexConfigs(topicConfigsFor(src, "legacy"))
	expectDefault(t, got, "cleanup.policy", "delete")
	expectDefault(t, got, "retention.ms", "604800000")
}

// TestTopicConfigsFor_UnknownTopicFallsThrough: when the topic
// isn't in the config source, topicConfigsFor returns defaults
// (the upstream handler is responsible for surfacing the
// UNKNOWN_TOPIC_OR_PARTITION error from h.topics.Get(); this is
// just the "no overrides" path).
func TestTopicConfigsFor_UnknownTopicFallsThrough(t *testing.T) {
	src := &configurableTopics{known: map[string]int32{}, configs: map[string]TopicConfig{}}
	got := indexConfigs(topicConfigsFor(src, "absent"))
	expectDefault(t, got, "cleanup.policy", "delete")
}

func indexConfigs(entries []api.DescribeConfigsEntry) map[string]api.DescribeConfigsEntry {
	out := make(map[string]api.DescribeConfigsEntry, len(entries))
	for _, e := range entries {
		out[e.Name] = e
	}
	return out
}

func expectOverride(t *testing.T, got map[string]api.DescribeConfigsEntry, name, value string) {
	t.Helper()
	e, ok := got[name]
	if !ok {
		t.Errorf("missing config entry %q", name)
		return
	}
	if e.Value != value {
		t.Errorf("config %q: value=%q, want %q", name, e.Value, value)
	}
	if e.IsDefault {
		t.Errorf("config %q: IsDefault=true, want false (CR override)", name)
	}
	if e.ConfigSource != api.ConfigSourceDynamicTopic {
		t.Errorf("config %q: ConfigSource=%d, want DynamicTopic(%d)",
			name, e.ConfigSource, api.ConfigSourceDynamicTopic)
	}
}

func expectDefault(t *testing.T, got map[string]api.DescribeConfigsEntry, name, value string) {
	t.Helper()
	e, ok := got[name]
	if !ok {
		t.Errorf("missing config entry %q", name)
		return
	}
	if e.Value != value {
		t.Errorf("config %q: value=%q, want %q", name, e.Value, value)
	}
	if !e.IsDefault {
		t.Errorf("config %q: IsDefault=false, want true (no override)", name)
	}
	if e.ConfigSource != api.ConfigSourceDefault {
		t.Errorf("config %q: ConfigSource=%d, want Default(%d)",
			name, e.ConfigSource, api.ConfigSourceDefault)
	}
}
