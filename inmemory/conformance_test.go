package inmemory_test

import (
	"testing"
	"time"

	"github.com/plexusone/omniworkflow/conformance"
	"github.com/plexusone/omniworkflow/inmemory"
)

// TestInMemoryConformance runs the full shared conformance suite against the
// in-memory provider. Unlike the Step Functions suite this needs no external
// resources, so it runs on every `go test` — making it the hermetic guard that
// the conformance harness itself is correct and that the interpreter semantics
// hold end-to-end.
func TestInMemoryConformance(t *testing.T) {
	p := inmemory.New()
	conformance.Run(t, conformance.Config{
		Provider:  p,
		NewWorker: p.NewWorker,
		// In-memory execution is instant; tighten the waits so the suite is fast
		// while leaving generous headroom over the cancel scenario's 5s delay.
		Timeout:      15 * time.Second,
		PollInterval: 5 * time.Millisecond,
	})
}
