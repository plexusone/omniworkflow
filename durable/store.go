// Package durable is a durable embedded WorkflowProvider. It survives process
// restarts without a cluster, an external broker, or AWS — proving
// OmniWorkflow's self-hostable, low-idle-cost thesis end-to-end.
//
// Durability here is deliberately narrow: the provider checkpoints a small
// Snapshot (the plan plus three string-sets — succeeded, failed, approved —
// and the status) after each wave, and reloads it to resume on restart. There
// is no event-sourcing or workflow-code replay to get wrong, because the
// "workflow logic" is the pure interpreter core (interp.Advance) over that
// explicit state. Steps are at-least-once: a step in flight when the process
// crashes re-runs on resume, exactly as Temporal activities and Step Functions
// tasks are — so step executors must be idempotent.
//
// The default FileStore is a zero-dependency reference. A production deployment
// can supply a Postgres- or bbolt-backed StateStore via the same interface.
package durable

import (
	"context"
	"sync"

	"github.com/plexusone/omniworkflow"
)

// Snapshot is the complete durable state of one run. It is small by design:
// the plan plus the interpreter's three outcome sets and the status.
type Snapshot struct {
	RunID     string                 `json:"runId"`
	Plan      omniworkflow.Plan      `json:"plan"`
	Succeeded map[string]bool        `json:"succeeded"`
	Failed    map[string]bool        `json:"failed"`
	Approved  map[string]bool        `json:"approved"`
	Status    omniworkflow.RunStatus `json:"status"`
}

// StateStore persists run snapshots. Implementations must be safe for
// concurrent use across distinct run IDs; the provider serializes writes per
// run (one driver goroutine owns each run's snapshot).
type StateStore interface {
	// Save writes (or overwrites) a run's snapshot durably.
	Save(ctx context.Context, snap Snapshot) error
	// Load returns a run's snapshot, or ok=false if none is stored.
	Load(ctx context.Context, runID string) (snap Snapshot, ok bool, err error)
	// List returns every stored snapshot, for resume on startup.
	List(ctx context.Context) ([]Snapshot, error)
}

// MemStore is an in-memory StateStore. It is durable only within a single
// process lifetime — useful for the store's own tests and for exercising the
// resume path without disk. For real durability use FileStore.
type MemStore struct {
	mu    sync.Mutex
	snaps map[string]Snapshot
}

// NewMemStore returns an empty in-memory StateStore.
func NewMemStore() *MemStore { return &MemStore{snaps: map[string]Snapshot{}} }

func (m *MemStore) Save(_ context.Context, snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snaps[snap.RunID] = cloneSnapshot(snap)
	return nil
}

func (m *MemStore) Load(_ context.Context, runID string) (Snapshot, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap, ok := m.snaps[runID]
	return cloneSnapshot(snap), ok, nil
}

func (m *MemStore) List(_ context.Context) ([]Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Snapshot, 0, len(m.snaps))
	for _, s := range m.snaps {
		out = append(out, cloneSnapshot(s))
	}
	return out, nil
}

func cloneSnapshot(s Snapshot) Snapshot {
	s.Succeeded = cloneBoolMap(s.Succeeded)
	s.Failed = cloneBoolMap(s.Failed)
	s.Approved = cloneBoolMap(s.Approved)
	// Plan is treated as immutable once stored; its slice is not deep-copied.
	return s
}

func cloneBoolMap(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
