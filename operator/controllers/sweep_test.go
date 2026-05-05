package controllers

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// SweepTopics must remove dirs under dataDir that have no matching KafkaTopic
// CR, while preserving the reserved __cluster/ dir, dotfiles, and dirs that
// do correspond to a live CR. This is the durability guarantee that lets us
// drop the per-CR finalizer: deletions that happen while the operator is
// down get reconciled at startup.
func TestSweepTopics(t *testing.T) {
	dir := t.TempDir()

	// Three live CRs.
	live := []*v1alpha1.KafkaTopic{
		{ObjectMeta: metav1.ObjectMeta{Name: "alive-1", Namespace: "skafka"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "alive-2", Namespace: "skafka"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "alive-3", Namespace: "skafka"}},
	}
	// Dirs on disk: 3 live + 2 orphaned + reserved + a dotfile.
	for _, name := range []string{
		"alive-1", "alive-2", "alive-3",
		"orphan-a", "orphan-b",
		clusterFilesDir, ".hidden-state",
	} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	objs := make([]client.Object, 0, len(live))
	for _, t := range live {
		objs = append(objs, t)
	}
	c := fake.NewClientBuilder().
		WithScheme(newACLScheme()).
		WithObjects(objs...).
		Build()

	removed, err := SweepTopics(context.Background(), c, "skafka", dir)
	if err != nil {
		t.Fatalf("SweepTopics: %v", err)
	}
	sort.Strings(removed)
	want := []string{"orphan-a", "orphan-b"}
	if len(removed) != len(want) || removed[0] != want[0] || removed[1] != want[1] {
		t.Errorf("removed=%v, want %v", removed, want)
	}

	// Live + reserved + dotfile all survive.
	for _, name := range []string{"alive-1", "alive-2", "alive-3", clusterFilesDir, ".hidden-state"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("expected %s to survive sweep: %v", name, err)
		}
	}
	// Orphans are gone.
	for _, name := range []string{"orphan-a", "orphan-b"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("expected %s removed, err=%v", name, err)
		}
	}
}

// SweepCredentials must drop entries from credentials.json whose KafkaUser CR
// is gone, and leave the rest intact. SCRAM keys for surviving users must
// not be regenerated — the on-disk values are reused as-is.
func TestSweepCredentials(t *testing.T) {
	dir := t.TempDir()

	// Pre-populate credentials.json with three users, two of which will lose
	// their CR. SCRAM creds get a sentinel salt so we can verify they are
	// preserved verbatim.
	cf := &CredentialsFile{Version: 1, Users: []UserCredential{
		{Username: "alice", AuthType: "tls", TLSCN: "alice"},
		{Username: "bob", AuthType: "scram-sha-512", Scram: &ScramCredential{
			Salt: "preserved-salt", StoredKey: "k1", ServerKey: "k2", Iterations: 8192,
		}},
		{Username: "carol", AuthType: "tls", TLSCN: "carol"},
	}}
	if err := writeCredentials(dir, cf); err != nil {
		t.Fatal(err)
	}

	// Only alice + bob have surviving CRs.
	live := []client.Object{
		&v1alpha1.KafkaUser{ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "skafka"}},
		&v1alpha1.KafkaUser{ObjectMeta: metav1.ObjectMeta{Name: "bob", Namespace: "skafka"}},
	}
	c := fake.NewClientBuilder().
		WithScheme(newACLScheme()).
		WithObjects(live...).
		Build()

	removed, err := SweepCredentials(context.Background(), c, "skafka", dir)
	if err != nil {
		t.Fatalf("SweepCredentials: %v", err)
	}
	if len(removed) != 1 || removed[0] != "carol" {
		t.Errorf("removed=%v, want [carol]", removed)
	}

	got, err := readCredentials(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Users) != 2 {
		t.Fatalf("want 2 surviving users, got %d", len(got.Users))
	}
	names := []string{got.Users[0].Username, got.Users[1].Username}
	sort.Strings(names)
	if names[0] != "alice" || names[1] != "bob" {
		t.Errorf("surviving users=%v, want [alice bob]", names)
	}
	for _, u := range got.Users {
		if u.Username == "bob" {
			if u.Scram == nil || u.Scram.Salt != "preserved-salt" {
				t.Errorf("bob's SCRAM creds were regenerated, expected preserved: %+v", u.Scram)
			}
		}
	}
}
