package omniworkflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// RunStatus is the normalized, provider-independent lifecycle status of a run.
// Every provider maps its native execution-status vocabulary (Temporal's
// WorkflowExecutionStatus, Step Functions' ExecutionStatus, ...) into these
// values so callers never see backend-specific types.
type RunStatus string

const (
	StatusRunning   RunStatus = "RUNNING"
	StatusSucceeded RunStatus = "SUCCEEDED"
	StatusFailed    RunStatus = "FAILED"
	StatusCancelled RunStatus = "CANCELLED"
)

// IsTerminal reports whether the status is a final state that will not change.
func (s RunStatus) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// WorkflowProvider is the provider-neutral control-plane interface. A provider
// (Step Functions, Temporal, ...) implements it to start and manage runs of a
// Plan. Callers program against this interface alone and select a provider by
// configuration.
//
// Values carried by any-typed fields (PlanStep.Input, Run.Output,
// Signal.Payload) cross the provider boundary and must be JSON-serializable —
// Step Functions passes state as JSON, and Temporal serializes payloads.
type WorkflowProvider interface {
	// Start begins executing req.Plan. The run is identified by req.ID, which
	// is also the idempotency key: starting again with the same ID must not
	// create a duplicate execution (it returns the existing run).
	Start(ctx context.Context, req StartRequest) (*Run, error)

	// Get returns the current state of the run with the given id.
	// It returns ErrRunNotFound if no such run exists.
	Get(ctx context.Context, id string) (*Run, error)

	// Signal delivers a signal to a running run — most importantly the
	// approval of a Gate step (see ApproveSignal). It returns ErrRunNotFound
	// if no such run exists.
	Signal(ctx context.Context, id string, signal Signal) error

	// Cancel requests cancellation of the run. After cancellation no further
	// steps execute and the run's terminal status is StatusCancelled.
	Cancel(ctx context.Context, id string) error
}

// StartRequest is the input to WorkflowProvider.Start.
type StartRequest struct {
	// ID is the caller-assigned run identity and idempotency key.
	ID string
	// Plan is the DAG to execute.
	Plan Plan
}

// Run is a provider-neutral view of a workflow run.
type Run struct {
	ID       string
	Status   RunStatus
	Output   any               // populated on success; JSON-serializable
	Metadata map[string]string // provider-specific extras, opt-in (e.g. execution ARN)
}

// Signal is a message delivered to a running run.
type Signal struct {
	Name    string
	Payload any // JSON-serializable
}

// ApproveSignal returns the signal name that approves a Gate step. A Gate step
// holds until a Signal with this name is delivered, then executes.
func ApproveSignal(stepID string) string {
	return "approve:" + stepID
}

// Plan is a flat DAG of steps. It is deliberately not a general-purpose
// workflow language: loops, dynamic fan-out, and arbitrary control flow are
// out of scope. It targets pipeline-shaped work.
type Plan struct {
	Steps []PlanStep
}

// PlanStep is a single unit of work in a Plan.
type PlanStep struct {
	// ID uniquely identifies the step within its Plan.
	ID string
	// DependsOn lists step IDs that must succeed before this step is ready.
	// Empty means the step is ready immediately.
	DependsOn []string
	// Gate, when true, makes the step hold once ready until an approval
	// Signal (ApproveSignal(ID)) is delivered, then execute.
	Gate bool
	// Input is opaque data passed to the StepExecutor verbatim.
	// JSON-serializable.
	Input any
}

// StepExecutor performs the work of a single step. The caller registers one
// StepExecutor with a provider's worker; the interpreter invokes it for each
// ready step. runID is the enclosing run's ID (StartRequest.ID).
type StepExecutor interface {
	ExecuteStep(ctx context.Context, runID string, step PlanStep) (output any, err error)
}

// Worker hosts a provider's embedded execution loop, dispatching ready steps
// to the registered StepExecutor. A caller obtains one from a provider's
// NewWorker and runs it inside its own process.
type Worker interface {
	// Run starts the worker and blocks until ctx is cancelled or a fatal
	// error occurs. A clean shutdown via ctx cancellation returns nil.
	Run(ctx context.Context) error
}

// Errors returned across the package. Providers wrap ErrRunNotFound and
// ErrDuplicateRun so callers can match with errors.Is.
var (
	ErrEmptyPlan         = errors.New("omniworkflow: plan has no steps")
	ErrEmptyStepID       = errors.New("omniworkflow: step has empty id")
	ErrDuplicateStepID   = errors.New("omniworkflow: duplicate step id")
	ErrUnknownDependency = errors.New("omniworkflow: step depends on unknown step id")
	ErrSelfDependency    = errors.New("omniworkflow: step depends on itself")
	ErrCyclicPlan        = errors.New("omniworkflow: plan contains a dependency cycle")
	ErrRunNotFound       = errors.New("omniworkflow: run not found")
	ErrDuplicateRun      = errors.New("omniworkflow: run id already exists")
)

// Validate checks the Plan's structure: it must be non-empty, every step ID
// must be unique and non-empty, every DependsOn must reference an existing
// step and never the step itself, and the dependency graph must be acyclic.
// Runtime execution semantics (readiness, gates, failure propagation) are the
// interpreter's concern, not Validate's.
func (p Plan) Validate() error {
	if len(p.Steps) == 0 {
		return ErrEmptyPlan
	}

	byID := make(map[string]PlanStep, len(p.Steps))
	for _, s := range p.Steps {
		id := strings.TrimSpace(s.ID)
		if id == "" {
			return ErrEmptyStepID
		}
		if _, dup := byID[id]; dup {
			return fmt.Errorf("%w: %q", ErrDuplicateStepID, id)
		}
		byID[id] = s
	}

	for _, s := range p.Steps {
		for _, dep := range s.DependsOn {
			if dep == s.ID {
				return fmt.Errorf("%w: %q", ErrSelfDependency, s.ID)
			}
			if _, ok := byID[dep]; !ok {
				return fmt.Errorf("%w: step %q -> %q", ErrUnknownDependency, s.ID, dep)
			}
		}
	}

	return p.checkAcyclic(byID)
}

// checkAcyclic runs a depth-first search with three-color marking and returns
// ErrCyclicPlan on the first back-edge found.
func (p Plan) checkAcyclic(byID map[string]PlanStep) error {
	const (
		white = 0 // unvisited
		gray  = 1 // on the current DFS stack
		black = 2 // fully explored
	)
	color := make(map[string]int, len(byID))

	var visit func(id string) error
	visit = func(id string) error {
		color[id] = gray
		for _, dep := range byID[id].DependsOn {
			switch color[dep] {
			case gray:
				return fmt.Errorf("%w: via %q -> %q", ErrCyclicPlan, id, dep)
			case white:
				if err := visit(dep); err != nil {
					return err
				}
			}
		}
		color[id] = black
		return nil
	}

	for _, s := range p.Steps {
		if color[s.ID] == white {
			if err := visit(s.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
