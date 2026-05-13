package auth

import "testing"

// fakeEngine is a minimal AuthEngine for selector tests — only the
// identity (via Authorize-as-tag) matters; everything else returns
// the most permissive answer.
type fakeEngine struct {
	id        string
	preAuth   bool
}

func (f *fakeEngine) NewSASLExchange(string) (SASLExchange, error)     { return nil, nil }
func (f *fakeEngine) AuthenticateTLS(string) (Principal, error)         { return Principal{Name: f.id}, nil }
func (f *fakeEngine) Authorize(Principal, Resource, Operation) bool     { return true }
func (f *fakeEngine) CheckProduceQuota(Principal, int) int32            { return 0 }
func (f *fakeEngine) CheckFetchQuota(Principal, int) int32              { return 0 }
func (f *fakeEngine) RequiresPreAuth() bool                             { return f.preAuth }

func TestSingleAuthEngineAlwaysReturnsWrapped(t *testing.T) {
	want := &fakeEngine{id: "wrapped"}
	s := NewSingleAuthEngine(want)
	for _, listener := range []string{"", "internal", "external", "bogus"} {
		if got, _ := s.For(listener).(*fakeEngine); got != want {
			t.Errorf("For(%q): got %p, want %p", listener, got, want)
		}
	}
}

func TestPerListenerAuthEngineRoutesByName(t *testing.T) {
	anon := &fakeEngine{id: "anon", preAuth: false}
	real := &fakeEngine{id: "real", preAuth: true}
	m := PerListenerAuthEngine{
		"plain":  anon,
		"secure": real,
		"":       anon, // fallback
	}

	cases := []struct {
		listener string
		want     *fakeEngine
	}{
		{"plain", anon},
		{"secure", real},
		{"unknown-listener", anon}, // fallback hit
		{"", anon},
	}
	for _, tc := range cases {
		got, _ := m.For(tc.listener).(*fakeEngine)
		if got != tc.want {
			t.Errorf("For(%q): got %s, want %s", tc.listener, got.id, tc.want.id)
		}
	}
}

func TestPerListenerAuthEngineNoFallbackReturnsNil(t *testing.T) {
	m := PerListenerAuthEngine{"plain": &fakeEngine{id: "anon"}}
	if got := m.For("ghost"); got != nil {
		t.Errorf("For unknown listener with no fallback: got %+v, want nil", got)
	}
}

func TestRequiresPreAuthSplitsByEngine(t *testing.T) {
	anon := &fakeEngine{id: "anon", preAuth: false}
	real := &fakeEngine{id: "real", preAuth: true}
	m := PerListenerAuthEngine{"plain": anon, "secure": real}
	if m.For("plain").RequiresPreAuth() {
		t.Error("anon listener: RequiresPreAuth = true, want false")
	}
	if !m.For("secure").RequiresPreAuth() {
		t.Error("secure listener: RequiresPreAuth = false, want true")
	}
}
