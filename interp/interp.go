// Package interp is the provider-independent DAG interpreter core. It is the
// single source of truth for the runtime semantics of a Plan — which steps are
// ready, how approval gates hold, and how failure propagates — so the Step
// Functions and Temporal providers cannot drift from each other. It contains
// no orchestration-engine or I/O code: Advance is a pure function of the Plan
// and the current State.
//
// # Semantics
//
//   - A step is ready when every step in its DependsOn has succeeded, it is not
//     already running or finished, and (if it is a Gate) it has been approved.
//   - A Gate step whose dependencies have succeeded but which has not been
//     approved is "awaiting approval"; the run stays RUNNING until an approval
//     arrives (or the run is cancelled).
//   - Failure is fail-soft: a failed step blocks only its transitive
//     dependents; independent branches continue to completion. The run ends
//     FAILED if any step failed, otherwise SUCCEEDED. It is the Plan author's
//     responsibility to add a dependency edge wherever a step must not run
//     after another's failure.
//   - Cancellation is terminal immediately: Advance reports CANCELLED and no
//     further steps.
package interp

import (
	"sort"

	"github.com/plexusone/omniworkflow"
)

// State is the observed execution state of a run: which steps have reached each
// outcome, which gate steps have been approved, and whether the run was
// cancelled. The driver (a provider's worker or workflow) owns and mutates it;
// Advance only reads it. The maps must be non-nil; use NewState.
type State struct {
	Succeeded map[string]bool
	Failed    map[string]bool
	Running   map[string]bool
	Approved  map[string]bool // gate step IDs that have received approval
	Cancelled bool
}

// NewState returns a State with all maps initialized and nothing recorded.
func NewState() State {
	return State{
		Succeeded: map[string]bool{},
		Failed:    map[string]bool{},
		Running:   map[string]bool{},
		Approved:  map[string]bool{},
	}
}

// Decision is the interpreter's verdict for one Advance call.
type Decision struct {
	// Ready lists step IDs that should be dispatched to the executor now,
	// sorted by ID for determinism (required so the same decision replays
	// identically inside a Temporal workflow).
	Ready []string
	// AwaitingApproval lists Gate step IDs whose dependencies are satisfied
	// but which have not yet been approved, sorted by ID.
	AwaitingApproval []string
	// Terminal is the run's terminal status, or "" if the run is still in
	// progress.
	Terminal omniworkflow.RunStatus
}

// Advance computes, from the Plan and the current State, which steps are ready
// to dispatch, which gates are awaiting approval, and whether the run has
// reached a terminal status. It is pure: it never mutates its arguments and
// its output depends only on its inputs.
func Advance(plan omniworkflow.Plan, st State) Decision {
	if st.Cancelled {
		return Decision{Terminal: omniworkflow.StatusCancelled}
	}

	var ready, awaiting []string
	for _, s := range plan.Steps {
		if st.Succeeded[s.ID] || st.Failed[s.ID] || st.Running[s.ID] {
			continue
		}
		if !depsSatisfied(s, st.Succeeded) {
			continue
		}
		if s.Gate && !st.Approved[s.ID] {
			awaiting = append(awaiting, s.ID)
		} else {
			ready = append(ready, s.ID)
		}
	}
	sort.Strings(ready)
	sort.Strings(awaiting)

	var terminal omniworkflow.RunStatus
	if len(ready) == 0 && len(awaiting) == 0 && len(st.Running) == 0 {
		if len(st.Failed) > 0 {
			terminal = omniworkflow.StatusFailed
		} else {
			terminal = omniworkflow.StatusSucceeded
		}
	}

	return Decision{Ready: ready, AwaitingApproval: awaiting, Terminal: terminal}
}

// depsSatisfied reports whether every dependency of s has succeeded.
func depsSatisfied(s omniworkflow.PlanStep, succeeded map[string]bool) bool {
	for _, dep := range s.DependsOn {
		if !succeeded[dep] {
			return false
		}
	}
	return true
}
