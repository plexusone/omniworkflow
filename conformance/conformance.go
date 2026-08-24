// Package conformance is the shared behavioral test suite every OmniWorkflow
// provider must pass. It is the load-bearing proof that the Step Functions and
// Temporal providers are interchangeable: both run the identical scenarios
// through the WorkflowProvider interface and a StepExecutor, so any semantic
// drift between them surfaces as a suite failure rather than a production
// surprise.
//
// It follows the systemforge providertest pattern: a provider's _test.go
// constructs a Config and calls Run.
//
//	func TestConformance(t *testing.T) {
//	    p, _ := stepfunctions.NewFromConfig(ctx, cfg, opts)
//	    conformance.Run(t, conformance.Config{
//	        Provider:  p,
//	        NewWorker: p.NewWorker,
//	    })
//	}
//
// Run drives a live backend, so a provider gates its call behind whatever
// credentials/resources it needs (see each provider's conformance_test.go).
package conformance

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/plexusone/omniworkflow"
)

// Config configures a conformance run.
type Config struct {
	// Provider is the implementation under test (required).
	Provider omniworkflow.WorkflowProvider
	// NewWorker builds a worker bound to the suite's executor (required). For
	// most providers this is the provider's own NewWorker method value.
	NewWorker func(omniworkflow.StepExecutor) omniworkflow.Worker
	// RunIDPrefix namespaces run IDs so repeated live runs do not collide with
	// retained execution history. Defaults to "conf".
	RunIDPrefix string
	// Timeout bounds how long a scenario waits for a run to reach a terminal
	// status. Defaults to 90s.
	Timeout time.Duration
	// PollInterval is how often terminal status is polled. Defaults to 1s.
	PollInterval time.Duration
}

func (c *Config) withDefaults() {
	if c.RunIDPrefix == "" {
		c.RunIDPrefix = "conf"
	}
	if c.Timeout == 0 {
		c.Timeout = 90 * time.Second
	}
	if c.PollInterval == 0 {
		c.PollInterval = time.Second
	}
}

var runSeq atomicCounter

// Run executes the full conformance suite against cfg.Provider. It starts one
// worker (bound to a per-run-routing executor) for the duration of the suite.
func Run(t *testing.T, cfg Config) {
	cfg.withDefaults()
	if cfg.Provider == nil || cfg.NewWorker == nil {
		t.Fatal("conformance: Config.Provider and Config.NewWorker are required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := &router{byRun: map[string]*recorder{}}
	worker := cfg.NewWorker(rt)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		_ = worker.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-workerDone
	})

	t.Run("Linear", func(t *testing.T) { scenarioLinear(t, cfg, rt) })
	t.Run("DiamondOrdering", func(t *testing.T) { scenarioDiamond(t, cfg, rt) })
	t.Run("GateHoldsUntilApproved", func(t *testing.T) { scenarioGate(t, cfg, rt) })
	t.Run("Cancel", func(t *testing.T) { scenarioCancel(t, cfg, rt) })
	t.Run("FailSoft", func(t *testing.T) { scenarioFailSoft(t, cfg, rt) })
	t.Run("IdempotentStart", func(t *testing.T) { scenarioIdempotent(t, cfg, rt) })
}

// --- scenarios ---

func scenarioLinear(t *testing.T, cfg Config, rt *router) {
	runID := cfg.newRunID("linear")
	rec := rt.register(runID, &recorder{})
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "a"},
		{ID: "b", DependsOn: []string{"a"}},
		{ID: "c", DependsOn: []string{"b"}},
	}}
	start(t, cfg, runID, plan)
	if status := waitTerminal(t, cfg, runID); status != omniworkflow.StatusSucceeded {
		t.Fatalf("status = %q, want SUCCEEDED", status)
	}
	assertOrder(t, rec.startedIDs(), []string{"a", "b", "c"})
}

func scenarioDiamond(t *testing.T, cfg Config, rt *router) {
	runID := cfg.newRunID("diamond")
	rec := rt.register(runID, &recorder{})
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "test", DependsOn: []string{"build"}},
		{ID: "scan", DependsOn: []string{"build"}},
		{ID: "package", DependsOn: []string{"test", "scan"}},
	}}
	start(t, cfg, runID, plan)
	if status := waitTerminal(t, cfg, runID); status != omniworkflow.StatusSucceeded {
		t.Fatalf("status = %q, want SUCCEEDED", status)
	}
	started := rec.startedIDs()
	assertBefore(t, started, "build", "test")
	assertBefore(t, started, "build", "scan")
	assertBefore(t, started, "test", "package")
	assertBefore(t, started, "scan", "package")
}

func scenarioGate(t *testing.T, cfg Config, rt *router) {
	runID := cfg.newRunID("gate")
	rec := rt.register(runID, &recorder{})
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "deploy", DependsOn: []string{"build"}, Gate: true},
	}}
	start(t, cfg, runID, plan)

	// Wait for build to finish, then confirm the run is still holding (deploy
	// not yet run) before approving.
	waitFinished(t, rec, "build", cfg.Timeout)
	if rec.finishedContains("deploy") {
		t.Fatal("deploy ran before approval")
	}
	approve(t, cfg, runID, "deploy")

	if status := waitTerminal(t, cfg, runID); status != omniworkflow.StatusSucceeded {
		t.Fatalf("status = %q, want SUCCEEDED", status)
	}
	if !rec.finishedContains("deploy") {
		t.Fatal("deploy did not run after approval")
	}
	assertBefore(t, rec.startedIDs(), "build", "deploy")
}

func scenarioCancel(t *testing.T, cfg Config, rt *router) {
	runID := cfg.newRunID("cancel")
	rec := rt.register(runID, &recorder{delay: 5 * time.Second})
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "slow"},
		{ID: "after", DependsOn: []string{"slow"}},
	}}
	start(t, cfg, runID, plan)
	waitStarted(t, rec, "slow", cfg.Timeout)

	if err := cfg.Provider.Cancel(context.Background(), runID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if status := waitTerminal(t, cfg, runID); status != omniworkflow.StatusCancelled {
		t.Fatalf("status = %q, want CANCELLED", status)
	}
	if rec.startedContains("after") {
		t.Fatal("downstream step 'after' started despite cancellation")
	}
}

func scenarioFailSoft(t *testing.T, cfg Config, rt *router) {
	runID := cfg.newRunID("failsoft")
	rec := rt.register(runID, &recorder{fail: map[string]bool{"build": true}})
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "test", DependsOn: []string{"build"}},
		{ID: "lint"},
	}}
	start(t, cfg, runID, plan)
	if status := waitTerminal(t, cfg, runID); status != omniworkflow.StatusFailed {
		t.Fatalf("status = %q, want FAILED", status)
	}
	if rec.startedContains("test") {
		t.Fatal("dependent 'test' ran after 'build' failed")
	}
	if !rec.finishedContains("lint") {
		t.Fatal("independent 'lint' did not run (fail-soft violated)")
	}
}

func scenarioIdempotent(t *testing.T, cfg Config, rt *router) {
	runID := cfg.newRunID("idem")
	rec := rt.register(runID, &recorder{})
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{{ID: "only"}}}

	r1 := start(t, cfg, runID, plan)
	// Second Start with the same ID and plan must not create a duplicate.
	r2, err := cfg.Provider.Start(context.Background(), omniworkflow.StartRequest{ID: runID, Plan: plan})
	if err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if r1.ID != r2.ID {
		t.Fatalf("idempotent Start returned different IDs: %q vs %q", r1.ID, r2.ID)
	}
	if status := waitTerminal(t, cfg, runID); status != omniworkflow.StatusSucceeded {
		t.Fatalf("status = %q, want SUCCEEDED", status)
	}
	if n := rec.startedCount("only"); n != 1 {
		t.Fatalf("step 'only' ran %d times, want exactly 1 (duplicate execution created)", n)
	}
}

// --- helpers ---

func (c Config) newRunID(base string) string {
	return fmt.Sprintf("%s-%s-%s", c.RunIDPrefix, base, runSeq.Next())
}

func start(t *testing.T, cfg Config, runID string, plan omniworkflow.Plan) *omniworkflow.Run {
	t.Helper()
	run, err := cfg.Provider.Start(context.Background(), omniworkflow.StartRequest{ID: runID, Plan: plan})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return run
}

func waitTerminal(t *testing.T, cfg Config, runID string) omniworkflow.RunStatus {
	t.Helper()
	deadline := time.Now().Add(cfg.Timeout)
	for time.Now().Before(deadline) {
		run, err := cfg.Provider.Get(context.Background(), runID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if run.Status.IsTerminal() {
			return run.Status
		}
		time.Sleep(cfg.PollInterval)
	}
	t.Fatalf("run %q did not reach a terminal status within %s", runID, cfg.Timeout)
	return ""
}

// approve retries Signal until the gate's wait-task has registered its token
// (Start dispatches the gate slightly after the run begins), or times out.
func approve(t *testing.T, cfg Config, runID, gateID string) {
	t.Helper()
	deadline := time.Now().Add(cfg.Timeout)
	for time.Now().Before(deadline) {
		err := cfg.Provider.Signal(context.Background(), runID, omniworkflow.Signal{Name: omniworkflow.ApproveSignal(gateID)})
		if err == nil {
			return
		}
		time.Sleep(cfg.PollInterval)
	}
	t.Fatalf("could not approve gate %q on run %q within %s", gateID, runID, cfg.Timeout)
}

func waitStarted(t *testing.T, rec *recorder, id string, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, func() bool { return rec.startedContains(id) }, "step "+id+" to start")
}

func waitFinished(t *testing.T, rec *recorder, id string, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, func() bool { return rec.finishedContains(id) }, "step "+id+" to finish")
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func assertOrder(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("execution order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("execution order = %v, want %v", got, want)
		}
	}
}

func assertBefore(t *testing.T, order []string, first, second string) {
	t.Helper()
	fi, si := -1, -1
	for i, id := range order {
		if id == first {
			fi = i
		}
		if id == second {
			si = i
		}
	}
	if fi < 0 || si < 0 || fi >= si {
		t.Fatalf("expected %q before %q in %v", first, second, order)
	}
}

// atomicCounter is a tiny stdlib-only counter to keep run IDs unique across
// scenarios and repeated live runs without adding a uuid dependency.
type atomicCounter struct {
	mu sync.Mutex
	n  int64
}

func (c *atomicCounter) Next() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatInt(c.n, 36)
}
