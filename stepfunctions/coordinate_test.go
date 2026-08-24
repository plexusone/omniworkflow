package stepfunctions

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/plexusone/omniworkflow"
)

// driveSFN simulates the interpreter state machine purely in Go: it loops
// coordinate -> (Map over dispatch producing waveResults) -> coordinate, exactly
// as Step Functions would, JSON round-tripping the blob each hop to prove it
// survives serialization. Step items always succeed; gate items are approved
// iff their ID is in approvals (otherwise the test fails, since a real gate
// would block). It returns the terminal status and the order steps executed.
func driveSFN(t *testing.T, plan omniworkflow.Plan, approvals map[string]bool) (omniworkflow.RunStatus, []string) {
	t.Helper()
	blob := stateBlob{
		RunID:    "run-1",
		Plan:     plan,
		Progress: progress{Succeeded: map[string]bool{}, Failed: map[string]bool{}, Approved: map[string]bool{}},
	}
	var order []string

	for range 100 {
		blob = jsonRoundTrip(t, blob)
		out := coordinate(blob)
		out = jsonRoundTrip(t, out)

		if term := out.Decision.Terminal; term != "" {
			return omniworkflow.RunStatus(term), order
		}
		if len(out.Decision.Dispatch) == 0 {
			t.Fatalf("no progress and not terminal; progress=%+v", out.Progress)
		}

		var results []waveResult
		for _, item := range out.Decision.Dispatch {
			switch item.Kind {
			case kindStep:
				order = append(order, item.ID)
				results = append(results, waveResult{ID: item.ID, Kind: kindStep, OK: true})
			case kindGate:
				if !approvals[item.ID] {
					t.Fatalf("gate %q awaiting approval with none given (would block)", item.ID)
				}
				results = append(results, waveResult{ID: item.ID, Kind: kindGate, Approved: true})
			default:
				t.Fatalf("unknown dispatch kind %q", item.Kind)
			}
		}
		out.WaveResults = results
		blob = out
	}
	t.Fatal("did not terminate within bound")
	return "", nil
}

func jsonRoundTrip(t *testing.T, b stateBlob) stateBlob {
	t.Helper()
	data, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal blob: %v", err)
	}
	var out stateBlob
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal blob: %v", err)
	}
	return out
}

func TestCoordinateDiamondDrivesToSucceeded(t *testing.T) {
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "test", DependsOn: []string{"build"}},
		{ID: "scan", DependsOn: []string{"build"}},
		{ID: "package", DependsOn: []string{"test", "scan"}},
	}}
	status, order := driveSFN(t, plan, nil)
	if status != omniworkflow.StatusSucceeded {
		t.Fatalf("status = %q, want SUCCEEDED", status)
	}
	if order[0] != "build" || order[len(order)-1] != "package" {
		t.Fatalf("bad order: %v", order)
	}
	if len(order) != 4 {
		t.Fatalf("expected 4 steps executed, got %v", order)
	}
}

func TestCoordinateGateHoldsAndReleases(t *testing.T) {
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "deploy", DependsOn: []string{"build"}, Gate: true, Input: map[string]any{"env": "prod"}},
	}}

	// First, prove the gate is emitted as an awaiting gate before approval.
	blob := stateBlob{
		RunID:    "run-1",
		Plan:     plan,
		Progress: progress{Succeeded: map[string]bool{"build": true}, Failed: map[string]bool{}, Approved: map[string]bool{}},
	}
	out := coordinate(blob)
	if len(out.Decision.Dispatch) != 1 || out.Decision.Dispatch[0].Kind != kindGate || out.Decision.Dispatch[0].ID != "deploy" {
		t.Fatalf("expected a single awaiting gate 'deploy', got %+v", out.Decision.Dispatch)
	}

	// Now drive with approval: deploy must execute (as a step) and the run succeeds.
	status, order := driveSFN(t, plan, map[string]bool{"deploy": true})
	if status != omniworkflow.StatusSucceeded {
		t.Fatalf("status = %q, want SUCCEEDED", status)
	}
	want := []string{"build", "deploy"}
	if len(order) != 2 || order[0] != want[0] || order[1] != want[1] {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestCoordinateFailSoft(t *testing.T) {
	// build -> test; independent 'lint'. build fails: test is skipped, lint runs,
	// run ends FAILED.
	plan := omniworkflow.Plan{Steps: []omniworkflow.PlanStep{
		{ID: "build"},
		{ID: "test", DependsOn: []string{"build"}},
		{ID: "lint"},
	}}
	blob := stateBlob{
		RunID:    "run-1",
		Plan:     plan,
		Progress: progress{Succeeded: map[string]bool{}, Failed: map[string]bool{}, Approved: map[string]bool{}},
	}

	var ran []string
	var status omniworkflow.RunStatus
	for range 100 {
		blob = jsonRoundTrip(t, blob)
		out := coordinate(blob)
		if term := out.Decision.Terminal; term != "" {
			status = omniworkflow.RunStatus(term)
			break
		}
		var results []waveResult
		for _, item := range out.Decision.Dispatch {
			ran = append(ran, item.ID)
			ok := item.ID != "build" // build fails
			results = append(results, waveResult{ID: item.ID, Kind: kindStep, OK: ok})
		}
		out.WaveResults = results
		blob = out
	}

	if status != omniworkflow.StatusFailed {
		t.Fatalf("status = %q, want FAILED", status)
	}
	for _, id := range ran {
		if id == "test" {
			t.Fatalf("test should have been skipped after build failed; ran=%v", ran)
		}
	}
	if !contains(ran, "lint") {
		t.Fatalf("independent step lint should have run; ran=%v", ran)
	}
}

func TestExecuteStepItem(t *testing.T) {
	okExec := execFunc(func(_ context.Context, runID string, step omniworkflow.PlanStep) (any, error) {
		return map[string]any{"ran": step.ID}, nil
	})
	res := executeStepItem(context.Background(), okExec, "run-1", dispatchItem{ID: "build", Kind: kindStep})
	if !res.OK || res.Error != "" {
		t.Fatalf("ok exec: %+v", res)
	}

	errExec := execFunc(func(_ context.Context, _ string, _ omniworkflow.PlanStep) (any, error) {
		return nil, errors.New("boom")
	})
	res = executeStepItem(context.Background(), errExec, "run-1", dispatchItem{ID: "build", Kind: kindStep})
	if res.OK || res.Error != "boom" {
		t.Fatalf("err exec: %+v", res)
	}
}

type execFunc func(context.Context, string, omniworkflow.PlanStep) (any, error)

func (f execFunc) ExecuteStep(ctx context.Context, runID string, step omniworkflow.PlanStep) (any, error) {
	return f(ctx, runID, step)
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
