package stepfunctions

import (
	"context"

	"github.com/plexusone/omniworkflow"
	"github.com/plexusone/omniworkflow/interp"
)

// Dispatch item kinds.
const (
	kindStep = "step"
	kindGate = "gate"
)

// stateBlob is the JSON document threaded through the interpreter state machine
// as execution state. The coordinator activity reads it, merges the previous
// wave's results, and returns the next blob; the Map dispatches decision.dispatch.
type stateBlob struct {
	RunID       string            `json:"runId"`
	Plan        omniworkflow.Plan `json:"plan"`
	Progress    progress          `json:"progress"`
	WaveResults []waveResult      `json:"waveResults,omitempty"`
	Decision    *decisionBlob     `json:"decision,omitempty"`
}

// progress is the durable execution state carried between waves.
type progress struct {
	Succeeded map[string]bool `json:"succeeded"`
	Failed    map[string]bool `json:"failed"`
	Approved  map[string]bool `json:"approved"`
}

// dispatchItem is one unit the Map iterates over in a wave: either a step to
// execute or a gate to hold for approval.
type dispatchItem struct {
	ID    string `json:"id"`
	Kind  string `json:"kind"`
	Input any    `json:"input,omitempty"`
}

// decisionBlob is the coordinator's verdict for one wave.
type decisionBlob struct {
	Dispatch []dispatchItem `json:"dispatch"`
	Terminal string         `json:"terminal"` // "" | SUCCEEDED | FAILED | CANCELLED
}

// waveResult is one Map iteration's output, produced by the step activity
// (for a step) or by an approval Signal (for a gate).
type waveResult struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	OK       bool   `json:"ok,omitempty"`       // step: did it succeed
	Output   any    `json:"output,omitempty"`   // step: executor output
	Error    string `json:"error,omitempty"`    // step: failure message
	Approved bool   `json:"approved,omitempty"` // gate: was it approved
}

// coordinate is the pure heart of the coordinator activity: given the incoming
// blob (carrying the prior wave's results), it folds those results into
// durable progress, runs the shared interpreter to decide the next wave, and
// returns the outgoing blob. It performs no I/O and does not mutate its input's
// maps beyond the copies it makes, so it is fully unit-testable without AWS.
func coordinate(in stateBlob) stateBlob {
	prog := progress{
		Succeeded: cloneBoolMap(in.Progress.Succeeded),
		Failed:    cloneBoolMap(in.Progress.Failed),
		Approved:  cloneBoolMap(in.Progress.Approved),
	}

	for _, r := range in.WaveResults {
		switch r.Kind {
		case kindStep:
			if r.OK {
				prog.Succeeded[r.ID] = true
			} else {
				prog.Failed[r.ID] = true
			}
		case kindGate:
			if r.Approved {
				prog.Approved[r.ID] = true
			}
		}
	}

	st := interp.State{
		Succeeded: prog.Succeeded,
		Failed:    prog.Failed,
		Approved:  prog.Approved,
		Running:   map[string]bool{}, // between waves nothing is in flight
	}
	d := interp.Advance(in.Plan, st)

	stepInput := make(map[string]any, len(in.Plan.Steps))
	for _, s := range in.Plan.Steps {
		stepInput[s.ID] = s.Input
	}

	dispatch := make([]dispatchItem, 0, len(d.Ready)+len(d.AwaitingApproval))
	for _, id := range d.Ready {
		dispatch = append(dispatch, dispatchItem{ID: id, Kind: kindStep, Input: stepInput[id]})
	}
	for _, id := range d.AwaitingApproval {
		dispatch = append(dispatch, dispatchItem{ID: id, Kind: kindGate})
	}

	return stateBlob{
		RunID:    in.RunID,
		Plan:     in.Plan,
		Progress: prog,
		Decision: &decisionBlob{Dispatch: dispatch, Terminal: string(d.Terminal)},
		// WaveResults intentionally cleared for the next iteration.
	}
}

// executeStepItem runs a step dispatch item through the executor and returns
// its wave result. A step failure is reported as a result with OK=false rather
// than an error, so the Map does not abort (fail-soft is the interpreter's job).
// It is unit-testable with a fake executor.
func executeStepItem(ctx context.Context, executor omniworkflow.StepExecutor, runID string, item dispatchItem) waveResult {
	step := omniworkflow.PlanStep{ID: item.ID, Input: item.Input}
	output, err := executor.ExecuteStep(ctx, runID, step)
	if err != nil {
		return waveResult{ID: item.ID, Kind: kindStep, OK: false, Error: err.Error()}
	}
	return waveResult{ID: item.ID, Kind: kindStep, OK: true, Output: output}
}

func cloneBoolMap(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
