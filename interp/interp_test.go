package interp

import (
	"reflect"
	"testing"

	"github.com/plexusone/omniworkflow"
)

func diamond() omniworkflow.Plan {
	return omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "test", DependsOn: []string{"build"}},
		{ID: "scan", DependsOn: []string{"build"}},
		{ID: "package", DependsOn: []string{"test", "scan"}},
	}}
}

func TestAdvanceDiamondLifecycle(t *testing.T) {
	plan := diamond()
	st := NewState()

	// Fresh: only build is ready.
	d := Advance(plan, st)
	assertReady(t, d, []string{"build"})
	assertNotTerminal(t, d)

	// build running: nothing new ready, not terminal (running).
	st.Running["build"] = true
	d = Advance(plan, st)
	assertReady(t, d, nil)
	assertNotTerminal(t, d)

	// build succeeded: test and scan both ready (sorted).
	delete(st.Running, "build")
	st.Succeeded["build"] = true
	d = Advance(plan, st)
	assertReady(t, d, []string{"scan", "test"})
	assertNotTerminal(t, d)

	// test running, scan succeeded: package still blocked on test.
	st.Running["test"] = true
	st.Succeeded["scan"] = true
	d = Advance(plan, st)
	assertReady(t, d, nil)
	assertNotTerminal(t, d)

	// test succeeded: package ready.
	delete(st.Running, "test")
	st.Succeeded["test"] = true
	d = Advance(plan, st)
	assertReady(t, d, []string{"package"})
	assertNotTerminal(t, d)

	// package succeeded: run SUCCEEDED.
	st.Succeeded["package"] = true
	d = Advance(plan, st)
	assertReady(t, d, nil)
	if d.Terminal != omniworkflow.StatusSucceeded {
		t.Fatalf("terminal = %q, want SUCCEEDED", d.Terminal)
	}
}

func TestAdvanceFailurePropagation(t *testing.T) {
	plan := diamond()
	st := NewState()

	// build failed immediately: dependents never ready, run FAILED.
	st.Failed["build"] = true
	d := Advance(plan, st)
	assertReady(t, d, nil)
	if len(d.AwaitingApproval) != 0 {
		t.Fatalf("awaiting = %v, want none", d.AwaitingApproval)
	}
	if d.Terminal != omniworkflow.StatusFailed {
		t.Fatalf("terminal = %q, want FAILED", d.Terminal)
	}
}

func TestAdvanceFailSoftIndependentBranch(t *testing.T) {
	// Two independent steps: a fails, b must still run before the run ends.
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "a"},
		{ID: "b"},
	}}
	st := NewState()

	// a failed, b not yet run: b is still ready, run not terminal.
	st.Failed["a"] = true
	d := Advance(plan, st)
	assertReady(t, d, []string{"b"})
	assertNotTerminal(t, d)

	// b succeeded: now terminal, and FAILED because a failed.
	st.Succeeded["b"] = true
	d = Advance(plan, st)
	if d.Terminal != omniworkflow.StatusFailed {
		t.Fatalf("terminal = %q, want FAILED", d.Terminal)
	}
}

func TestAdvanceGateHoldsThenReleases(t *testing.T) {
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "deploy", DependsOn: []string{"build"}, Gate: true},
	}}
	st := NewState()
	st.Succeeded["build"] = true

	// deploy is a gate awaiting approval, not ready, run not terminal.
	d := Advance(plan, st)
	assertReady(t, d, nil)
	if !reflect.DeepEqual(d.AwaitingApproval, []string{"deploy"}) {
		t.Fatalf("awaiting = %v, want [deploy]", d.AwaitingApproval)
	}
	assertNotTerminal(t, d)

	// approve deploy: now ready.
	st.Approved["deploy"] = true
	d = Advance(plan, st)
	assertReady(t, d, []string{"deploy"})
	if len(d.AwaitingApproval) != 0 {
		t.Fatalf("awaiting = %v, want none after approval", d.AwaitingApproval)
	}
	assertNotTerminal(t, d)
}

func TestAdvanceCancelShortCircuits(t *testing.T) {
	plan := diamond()
	st := NewState()
	st.Succeeded["build"] = true
	st.Running["test"] = true
	st.Cancelled = true

	d := Advance(plan, st)
	if d.Terminal != omniworkflow.StatusCancelled {
		t.Fatalf("terminal = %q, want CANCELLED", d.Terminal)
	}
	if len(d.Ready) != 0 || len(d.AwaitingApproval) != 0 {
		t.Fatalf("cancelled run must dispatch nothing: ready=%v awaiting=%v", d.Ready, d.AwaitingApproval)
	}
}

// TestDriveToCompletion exercises the interpreter the way a provider worker
// does: repeatedly Advance, dispatch ready steps, mark them succeeded, until
// terminal. It doubles as executable documentation of the driver contract.
func TestDriveToCompletion(t *testing.T) {
	plan := diamond()
	st := NewState()
	var order []string

	for range 100 { // bounded to catch non-termination
		d := Advance(plan, st)
		if d.Terminal != "" {
			if d.Terminal != omniworkflow.StatusSucceeded {
				t.Fatalf("terminal = %q, want SUCCEEDED", d.Terminal)
			}
			// build must precede all; package must come last.
			if order[0] != "build" || order[len(order)-1] != "package" {
				t.Fatalf("bad execution order: %v", order)
			}
			return
		}
		if len(d.Ready) == 0 {
			t.Fatalf("no progress and not terminal; state=%+v", st)
		}
		for _, id := range d.Ready {
			order = append(order, id)
			st.Succeeded[id] = true
		}
	}
	t.Fatal("did not terminate within bound")
}

func assertReady(t *testing.T, d Decision, want []string) {
	t.Helper()
	if len(d.Ready) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(d.Ready, want) {
		t.Fatalf("ready = %v, want %v", d.Ready, want)
	}
}

func assertNotTerminal(t *testing.T, d Decision) {
	t.Helper()
	if d.Terminal != "" {
		t.Fatalf("terminal = %q, want in-progress", d.Terminal)
	}
}
