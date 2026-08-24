// Package temporal is the Temporal provider for OmniWorkflow. It runs the same
// generic interpreter as the other providers, but as deterministic Temporal
// workflow code: a single interpreter workflow walks the Plan wave by wave,
// dispatching each ready step to an activity that invokes the caller's
// StepExecutor, and holding gates on a Temporal signal. Callers never write
// workflow code themselves.
package temporal

import (
	"context"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/plexusone/omniworkflow"
	"github.com/plexusone/omniworkflow/interp"
)

// Registered names for the interpreter workflow, step activity, and approval
// signal. They are stable identifiers in Temporal history, so they must not
// change casually.
const (
	workflowName      = "omniworkflow.Interpreter"
	activityName      = "omniworkflow.RunStep"
	approveSignalName = "omniworkflow.approve"
	failedErrorType   = "OmniWorkflowRunFailed"
)

// interpreterInput is the workflow argument.
type interpreterInput struct {
	RunID string            `json:"runId"`
	Plan  omniworkflow.Plan `json:"plan"`
}

// stepActivityInput is the argument to the step activity.
type stepActivityInput struct {
	RunID string                `json:"runId"`
	Step  omniworkflow.PlanStep `json:"step"`
}

// stepResult is the activity's result. A business step failure is reported as
// OK=false with a nil activity error, so it does not trigger activity retries
// or abort the workflow — fail-soft is the interpreter's job.
type stepResult struct {
	OK     bool   `json:"ok"`
	Output any    `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

// activities binds the caller's StepExecutor to the registered activity.
type activities struct {
	executor omniworkflow.StepExecutor
}

// RunStep executes one step through the caller's StepExecutor.
func (a *activities) RunStep(ctx context.Context, in stepActivityInput) (stepResult, error) {
	output, err := a.executor.ExecuteStep(ctx, in.RunID, in.Step)
	if err != nil {
		return stepResult{OK: false, Error: err.Error()}, nil
	}
	return stepResult{OK: true, Output: output}, nil
}

// InterpreterWorkflow is the generic DAG interpreter as deterministic workflow
// code. It returns the set of succeeded step IDs on success.
func InterpreterWorkflow(ctx workflow.Context, in interpreterInput) (map[string]bool, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: activityStartToCloseTimeout,
	})
	approvals := workflow.GetSignalChannel(ctx, approveSignalName)

	stepByID := make(map[string]omniworkflow.PlanStep, len(in.Plan.Steps))
	for _, s := range in.Plan.Steps {
		stepByID[s.ID] = s
	}

	state := interp.State{
		Succeeded: map[string]bool{},
		Failed:    map[string]bool{},
		Running:   map[string]bool{},
		Approved:  map[string]bool{},
	}

	for {
		if ctx.Err() != nil {
			return nil, temporal.NewCanceledError()
		}

		// Fold any buffered approvals into state before deciding.
		for {
			var gateID string
			if !approvals.ReceiveAsync(&gateID) {
				break
			}
			state.Approved[gateID] = true
		}

		d := interp.Advance(in.Plan, state)
		switch d.Terminal {
		case omniworkflow.StatusSucceeded:
			return state.Succeeded, nil
		case omniworkflow.StatusFailed:
			return nil, temporal.NewApplicationError("one or more steps failed", failedErrorType)
		case omniworkflow.StatusCancelled:
			return nil, temporal.NewCanceledError()
		}

		if len(d.Ready) > 0 {
			if err := runWave(ctx, in.RunID, d.Ready, stepByID, &state); err != nil {
				return nil, err // cancellation
			}
			continue
		}

		// Only gates remain: block for one approval (or cancellation).
		if len(d.AwaitingApproval) > 0 {
			gateID, cancelled := awaitApproval(ctx, approvals)
			if cancelled {
				return nil, temporal.NewCanceledError()
			}
			state.Approved[gateID] = true
		}
	}
}

// runWave dispatches the ready steps as parallel activities and folds their
// results into state, a barrier matching the other providers' wave semantics.
// It returns a non-nil error only on cancellation.
func runWave(ctx workflow.Context, runID string, ready []string, stepByID map[string]omniworkflow.PlanStep, state *interp.State) error {
	type pending struct {
		id     string
		future workflow.Future
	}
	futures := make([]pending, 0, len(ready))
	for _, id := range ready {
		in := stepActivityInput{RunID: runID, Step: stepByID[id]}
		futures = append(futures, pending{id: id, future: workflow.ExecuteActivity(ctx, activityName, in)})
	}

	for _, p := range futures {
		var res stepResult
		err := p.future.Get(ctx, &res)
		if err != nil {
			if temporal.IsCanceledError(err) || ctx.Err() != nil {
				return temporal.NewCanceledError()
			}
			// An activity that still errors after its retry policy is a failed
			// step (fail-soft); the interpreter blocks its dependents.
			state.Failed[p.id] = true
			continue
		}
		if res.OK {
			state.Succeeded[p.id] = true
		} else {
			state.Failed[p.id] = true
		}
	}
	return nil
}

// awaitApproval blocks until an approval signal arrives or the workflow is
// cancelled, returning the approved gate ID or cancelled=true.
func awaitApproval(ctx workflow.Context, approvals workflow.ReceiveChannel) (gateID string, cancelled bool) {
	sel := workflow.NewSelector(ctx)
	sel.AddReceive(approvals, func(c workflow.ReceiveChannel, _ bool) {
		c.Receive(ctx, &gateID)
	})
	sel.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {
		cancelled = true
	})
	sel.Select(ctx)
	if ctx.Err() != nil {
		cancelled = true
	}
	return gateID, cancelled
}
