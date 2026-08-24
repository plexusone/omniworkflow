package omniworkflow

import (
	"errors"
	"testing"
)

func TestRunStatusIsTerminal(t *testing.T) {
	cases := map[RunStatus]bool{
		StatusRunning:         false,
		StatusSucceeded:       true,
		StatusFailed:          true,
		StatusCancelled:       true,
		RunStatus("nonsense"): false,
	}
	for status, want := range cases {
		if got := status.IsTerminal(); got != want {
			t.Errorf("RunStatus(%q).IsTerminal() = %v, want %v", status, got, want)
		}
	}
}

func TestApproveSignal(t *testing.T) {
	if got, want := ApproveSignal("deploy"), "approve:deploy"; got != want {
		t.Errorf("ApproveSignal(deploy) = %q, want %q", got, want)
	}
}

func TestPlanValidate(t *testing.T) {
	tests := []struct {
		name    string
		plan    Plan
		wantErr error // nil means valid
	}{
		{
			name:    "empty plan",
			plan:    Plan{},
			wantErr: ErrEmptyPlan,
		},
		{
			name:    "empty step id",
			plan:    Plan{Steps: []PlanStep{{ID: "  "}}},
			wantErr: ErrEmptyStepID,
		},
		{
			name:    "duplicate step id",
			plan:    Plan{Steps: []PlanStep{{ID: "a"}, {ID: "a"}}},
			wantErr: ErrDuplicateStepID,
		},
		{
			name:    "self dependency",
			plan:    Plan{Steps: []PlanStep{{ID: "a", DependsOn: []string{"a"}}}},
			wantErr: ErrSelfDependency,
		},
		{
			name:    "unknown dependency",
			plan:    Plan{Steps: []PlanStep{{ID: "a", DependsOn: []string{"ghost"}}}},
			wantErr: ErrUnknownDependency,
		},
		{
			name: "two-node cycle",
			plan: Plan{Steps: []PlanStep{
				{ID: "a", DependsOn: []string{"b"}},
				{ID: "b", DependsOn: []string{"a"}},
			}},
			wantErr: ErrCyclicPlan,
		},
		{
			name: "three-node cycle",
			plan: Plan{Steps: []PlanStep{
				{ID: "a", DependsOn: []string{"c"}},
				{ID: "b", DependsOn: []string{"a"}},
				{ID: "c", DependsOn: []string{"b"}},
			}},
			wantErr: ErrCyclicPlan,
		},
		{
			name: "valid single step",
			plan: Plan{Steps: []PlanStep{{ID: "a"}}},
		},
		{
			name: "valid diamond",
			plan: Plan{Steps: []PlanStep{
				{ID: "build"},
				{ID: "test", DependsOn: []string{"build"}},
				{ID: "scan", DependsOn: []string{"build"}},
				{ID: "package", DependsOn: []string{"test", "scan"}},
			}},
		},
		{
			name: "valid with gate",
			plan: Plan{Steps: []PlanStep{
				{ID: "build"},
				{ID: "deploy", DependsOn: []string{"build"}, Gate: true},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.plan.Validate()
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Validate() = %v, want errors.Is(%v)", err, tc.wantErr)
			}
		})
	}
}

// diamondPlan is a shared fixture: build fans out to test+scan, which fan in
// to package. Used by interpreter and provider tests.
func diamondPlan() Plan {
	return Plan{Steps: []PlanStep{
		{ID: "build"},
		{ID: "test", DependsOn: []string{"build"}},
		{ID: "scan", DependsOn: []string{"build"}},
		{ID: "package", DependsOn: []string{"test", "scan"}},
	}}
}

func TestDiamondPlanIsValid(t *testing.T) {
	if err := diamondPlan().Validate(); err != nil {
		t.Fatalf("diamondPlan should be valid: %v", err)
	}
}
