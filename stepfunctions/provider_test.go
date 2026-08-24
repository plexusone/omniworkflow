package stepfunctions

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/plexusone/omniworkflow"
)

func TestExecutionARN(t *testing.T) {
	p := &Provider{resources: Provisioned{
		StateMachineARN: "arn:aws:states:us-east-1:123456789012:stateMachine:omniworkflow-interpreter",
	}}
	got := p.executionARN("run-abc")
	want := "arn:aws:states:us-east-1:123456789012:execution:omniworkflow-interpreter:run-abc"
	if got != want {
		t.Fatalf("executionARN = %q, want %q", got, want)
	}
}

func TestMapStatus(t *testing.T) {
	cases := map[types.ExecutionStatus]omniworkflow.RunStatus{
		types.ExecutionStatusRunning:        omniworkflow.StatusRunning,
		types.ExecutionStatusPendingRedrive: omniworkflow.StatusRunning,
		types.ExecutionStatusSucceeded:      omniworkflow.StatusSucceeded,
		types.ExecutionStatusFailed:         omniworkflow.StatusFailed,
		types.ExecutionStatusTimedOut:       omniworkflow.StatusFailed,
		types.ExecutionStatusAborted:        omniworkflow.StatusCancelled,
	}
	for in, want := range cases {
		if got := mapStatus(in); got != want {
			t.Errorf("mapStatus(%s) = %q, want %q", in, got, want)
		}
	}
}

func TestSignalUnsupportedName(t *testing.T) {
	p := New(nil, Provisioned{}, Options{})
	err := p.Signal(context.Background(), "run-1", omniworkflow.Signal{Name: "pause"})
	if err == nil || !strings.Contains(err.Error(), "unsupported signal") {
		t.Fatalf("Signal(pause) err = %v, want unsupported-signal error", err)
	}
}

func TestSignalNoPendingApproval(t *testing.T) {
	// Empty token store, nil client: the no-pending path returns before any AWS call.
	p := New(nil, Provisioned{}, Options{TokenStore: NewMemoryTokenStore()})
	err := p.Signal(context.Background(), "run-1", omniworkflow.Signal{Name: omniworkflow.ApproveSignal("deploy")})
	if !errors.Is(err, ErrNoPendingApproval) {
		t.Fatalf("Signal err = %v, want ErrNoPendingApproval", err)
	}
}

func TestMemoryTokenStore(t *testing.T) {
	s := NewMemoryTokenStore()
	ctx := context.Background()

	if _, ok, _ := s.Get(ctx, "r", "g"); ok {
		t.Fatal("empty store returned a token")
	}
	if err := s.Put(ctx, "r", "g", "tok-123"); err != nil {
		t.Fatal(err)
	}
	token, ok, _ := s.Get(ctx, "r", "g")
	if !ok || token != "tok-123" {
		t.Fatalf("Get = %q, %v; want tok-123, true", token, ok)
	}
	// Distinct (run, gate) keys do not collide.
	if _, ok, _ := s.Get(ctx, "r", "other"); ok {
		t.Fatal("unexpected token for different gate")
	}
	if err := s.Delete(ctx, "r", "g"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get(ctx, "r", "g"); ok {
		t.Fatal("token present after delete")
	}
}
