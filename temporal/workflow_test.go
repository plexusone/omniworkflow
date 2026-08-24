package temporal

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/plexusone/omniworkflow"
)

// These tests exercise the interpreter workflow hermetically in the Temporal
// test environment — no server, no client — validating the Temporal-specific
// logic (determinism, activity dispatch, signal-driven gates, terminal
// mapping). The full client+worker+server path is covered by the gated
// conformance test.

type recorder struct {
	mu   sync.Mutex
	seen []string
	fail map[string]bool
}

func (r *recorder) ExecuteStep(_ context.Context, _ string, step omniworkflow.PlanStep) (any, error) {
	r.mu.Lock()
	r.seen = append(r.seen, step.ID)
	fail := r.fail[step.ID]
	r.mu.Unlock()
	if fail {
		return nil, errStepFailed
	}
	return map[string]any{"step": step.ID}, nil
}

var errStepFailed = &stepErr{}

type stepErr struct{}

func (*stepErr) Error() string { return "injected step failure" }

func (r *recorder) ran(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.seen {
		if s == id {
			return true
		}
	}
	return false
}

func newEnv(t *testing.T, rec *recorder) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(InterpreterWorkflow, workflow.RegisterOptions{Name: workflowName})
	a := &activities{executor: rec}
	env.RegisterActivityWithOptions(a.RunStep, activity.RegisterOptions{Name: activityName})
	return env
}

func TestInterpreterWorkflowDiamond(t *testing.T) {
	rec := &recorder{}
	env := newEnv(t, rec)
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "test", DependsOn: []string{"build"}},
		{ID: "scan", DependsOn: []string{"build"}},
		{ID: "package", DependsOn: []string{"test", "scan"}},
	}}

	env.ExecuteWorkflow(InterpreterWorkflow, interpreterInput{RunID: "r", Plan: plan})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	for _, id := range []string{"build", "test", "scan", "package"} {
		if !rec.ran(id) {
			t.Errorf("step %q did not run", id)
		}
	}
}

func TestInterpreterWorkflowGate(t *testing.T) {
	rec := &recorder{}
	env := newEnv(t, rec)
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "deploy", DependsOn: []string{"build"}, Gate: true},
	}}

	// Send the approval shortly after start; the gate must hold until then.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(approveSignalName, "deploy")
	}, time.Millisecond)

	env.ExecuteWorkflow(InterpreterWorkflow, interpreterInput{RunID: "r", Plan: plan})

	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow did not succeed: completed=%v err=%v", env.IsWorkflowCompleted(), env.GetWorkflowError())
	}
	if !rec.ran("deploy") {
		t.Fatal("deploy did not run after approval")
	}
}

func TestInterpreterWorkflowFailSoft(t *testing.T) {
	rec := &recorder{fail: map[string]bool{"build": true}}
	env := newEnv(t, rec)
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "test", DependsOn: []string{"build"}},
		{ID: "lint"},
	}}

	env.ExecuteWorkflow(InterpreterWorkflow, interpreterInput{RunID: "r", Plan: plan})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	// A failed step makes the run fail (workflow returns an application error).
	if env.GetWorkflowError() == nil {
		t.Fatal("expected workflow error for a failed run")
	}
	if rec.ran("test") {
		t.Fatal("dependent 'test' ran after 'build' failed")
	}
	if !rec.ran("lint") {
		t.Fatal("independent 'lint' did not run (fail-soft violated)")
	}
}
