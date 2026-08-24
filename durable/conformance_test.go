package durable_test

import (
	"testing"
	"time"

	"github.com/plexusone/omniworkflow/conformance"
	"github.com/plexusone/omniworkflow/durable"
)

// TestDurableConformance runs the full shared conformance suite against the
// durable provider backed by a FileStore in a temp dir — hermetic, on every
// `go test`. It proves the durable provider is behaviorally interchangeable
// with the others; the restart-specific durability guarantee is proven
// separately in TestDurableResumeAcrossRestart.
func TestDurableConformance(t *testing.T) {
	dir := t.TempDir()
	store, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	p := durable.New(store)
	conformance.Run(t, conformance.Config{
		Provider:     p,
		NewWorker:    p.NewWorker,
		Timeout:      15 * time.Second,
		PollInterval: 5 * time.Millisecond,
	})
}
