package conformance

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/plexusone/omniworkflow"
)

// router is the single StepExecutor bound to the suite's worker. It dispatches
// each ExecuteStep to the per-run recorder registered for that run ID, so one
// worker can serve every scenario with independent per-run behavior.
type router struct {
	mu    sync.Mutex
	byRun map[string]*recorder
}

func (r *router) register(runID string, rec *recorder) *recorder {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec.init()
	r.byRun[runID] = rec
	return rec
}

func (r *router) ExecuteStep(ctx context.Context, runID string, step omniworkflow.PlanStep) (any, error) {
	r.mu.Lock()
	rec := r.byRun[runID]
	r.mu.Unlock()
	if rec == nil {
		// Unknown run (e.g. a stale execution): succeed benignly.
		return map[string]any{"step": step.ID}, nil
	}
	return rec.execute(ctx, step)
}

// recorder observes and shapes step execution for one run: it records start and
// finish order, injects per-step failures, and optionally delays each step.
type recorder struct {
	fail  map[string]bool
	delay time.Duration

	mu       sync.Mutex
	started  []string
	finished []string
}

func (rec *recorder) init() {
	if rec.fail == nil {
		rec.fail = map[string]bool{}
	}
}

func (rec *recorder) execute(ctx context.Context, step omniworkflow.PlanStep) (any, error) {
	rec.mu.Lock()
	rec.started = append(rec.started, step.ID)
	rec.mu.Unlock()

	if rec.delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(rec.delay):
		}
	}

	if rec.fail[step.ID] {
		return nil, fmt.Errorf("conformance: injected failure for step %q", step.ID)
	}

	rec.mu.Lock()
	rec.finished = append(rec.finished, step.ID)
	rec.mu.Unlock()
	return map[string]any{"step": step.ID}, nil
}

func (rec *recorder) startedIDs() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	out := make([]string, len(rec.started))
	copy(out, rec.started)
	return out
}

func (rec *recorder) startedContains(id string) bool  { return rec.countIn(&rec.started, id) > 0 }
func (rec *recorder) finishedContains(id string) bool { return rec.countIn(&rec.finished, id) > 0 }
func (rec *recorder) startedCount(id string) int      { return rec.countIn(&rec.started, id) }

func (rec *recorder) countIn(list *[]string, id string) int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	n := 0
	for _, v := range *list {
		if v == id {
			n++
		}
	}
	return n
}
