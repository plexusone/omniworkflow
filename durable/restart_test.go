package durable_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/plexusone/omniworkflow"
	"github.com/plexusone/omniworkflow/durable"
)

// TestDurableResumeAcrossRestart is the headline durability proof: a run that is
// waiting for a human approval survives the loss of the process that started it,
// resumes on a fresh provider instance backed by the same on-disk store, is
// approved there, and completes — without re-running the work that had already
// succeeded before the "crash".
func TestDurableResumeAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "deploy", DependsOn: []string{"build"}, Gate: true},
	}}
	const runID = "r1"
	ctx := context.Background()

	// --- Instance A: start the run, let build finish, stop before approving. ---
	storeA, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	pA := durable.New(storeA)
	recA := &recorder{}
	ctxA, cancelA := context.WithCancel(ctx)
	doneA := runWorker(pA.NewWorker(recA), ctxA)

	if _, err := pA.Start(ctx, omniworkflow.StartRequest{ID: runID, Plan: plan}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait until build's success is durably checkpointed on disk.
	waitUntil(t, 10*time.Second, func() bool {
		snap, ok, err := storeA.Load(ctx, runID)
		return err == nil && ok && snap.Succeeded["build"]
	}, "build to be checkpointed")

	if recA.contains("deploy") {
		t.Fatal("deploy ran before approval on instance A")
	}

	// "Crash" A: cancelling the worker drops all in-memory state, including the
	// gate's approval channel.
	cancelA()
	<-doneA

	// --- Instance B: fresh provider over the same store; must resume r1. ---
	storeB, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	pB := durable.New(storeB)
	recB := &recorder{}
	ctxB, cancelB := context.WithCancel(ctx)
	doneB := runWorker(pB.NewWorker(recB), ctxB)
	// Cancel first, then wait for the worker to stop (LIFO defers would
	// otherwise wait before cancelling and deadlock).
	defer func() { cancelB(); <-doneB }()

	// The resumed run is waiting at the gate again; approve it via B.
	approveWithRetry(t, pB, runID, "deploy", 10*time.Second)

	// It should now complete.
	waitUntil(t, 10*time.Second, func() bool {
		run, err := pB.Get(ctx, runID)
		return err == nil && run.Status == omniworkflow.StatusSucceeded
	}, "run to succeed after resume")

	// deploy ran on B after resume; build did NOT re-run (it was checkpointed).
	if !recB.contains("deploy") {
		t.Fatal("deploy did not run on instance B after resume")
	}
	if recB.contains("build") {
		t.Fatal("build re-ran on instance B; checkpoint was not honored")
	}
}

// --- test helpers ---

type recorder struct {
	mu   sync.Mutex
	seen []string
}

func (r *recorder) ExecuteStep(_ context.Context, _ string, step omniworkflow.PlanStep) (any, error) {
	r.mu.Lock()
	r.seen = append(r.seen, step.ID)
	r.mu.Unlock()
	return map[string]any{"step": step.ID}, nil
}

func (r *recorder) contains(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.seen {
		if s == id {
			return true
		}
	}
	return false
}

func runWorker(w omniworkflow.Worker, ctx context.Context) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	return done
}

func approveWithRetry(t *testing.T, p omniworkflow.WorkflowProvider, runID, gateID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		err := p.Signal(context.Background(), runID, omniworkflow.Signal{Name: omniworkflow.ApproveSignal(gateID)})
		if err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("could not approve gate %q on run %q within %s", gateID, runID, timeout)
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
