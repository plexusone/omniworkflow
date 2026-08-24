package stepfunctions

import (
	"context"
	"sync"
)

// TokenStore holds the Step Functions task tokens of gate steps that are
// waiting for approval, keyed by (runID, gateID). The worker records a token
// when it picks up a gate wait-task; Signal looks it up to release the gate via
// SendTaskSuccess.
//
// The default MemoryTokenStore is process-local, which suits a single-process
// deployment where the worker, control plane, and signal handler share one
// instance (ActionForge v1). A multi-process or crash-durable deployment should
// supply a shared implementation (e.g. DynamoDB- or Postgres-backed); the
// interface is the seam for that.
type TokenStore interface {
	Put(ctx context.Context, runID, gateID, token string) error
	Get(ctx context.Context, runID, gateID string) (token string, ok bool, err error)
	Delete(ctx context.Context, runID, gateID string) error
}

// MemoryTokenStore is an in-memory, concurrency-safe TokenStore.
type MemoryTokenStore struct {
	mu     sync.Mutex
	tokens map[string]string
}

// NewMemoryTokenStore returns an empty in-memory TokenStore.
func NewMemoryTokenStore() *MemoryTokenStore {
	return &MemoryTokenStore{tokens: map[string]string{}}
}

func tokenKey(runID, gateID string) string { return runID + "\x00" + gateID }

// Put records the task token for a waiting gate.
func (m *MemoryTokenStore) Put(_ context.Context, runID, gateID, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokens[tokenKey(runID, gateID)] = token
	return nil
}

// Get returns the task token for a waiting gate, if present.
func (m *MemoryTokenStore) Get(_ context.Context, runID, gateID string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	token, ok := m.tokens[tokenKey(runID, gateID)]
	return token, ok, nil
}

// Delete removes a gate's task token once it has been released.
func (m *MemoryTokenStore) Delete(_ context.Context, runID, gateID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tokens, tokenKey(runID, gateID))
	return nil
}
