package stepfunctions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/grokify/awsgo/sfnutil"
	"github.com/plexusone/omniworkflow"
)

// Provider is the AWS Step Functions implementation of
// omniworkflow.WorkflowProvider. Construct it with New (or NewFromConfig, which
// provisions first), then obtain a Worker with NewWorker and run it in-process.
type Provider struct {
	client     *sfnutil.Client
	resources  Provisioned
	tokens     TokenStore
	workerName string
}

// Options configures a Provider beyond its AWS resources.
type Options struct {
	// TokenStore holds pending gate-approval tokens shared between the
	// Provider (Signal) and its Worker. Defaults to an in-process
	// MemoryTokenStore.
	TokenStore TokenStore
	// WorkerName is an optional identifier reported when polling activities.
	WorkerName string
}

// ErrNoPendingApproval is returned by Signal when no gate is currently awaiting
// the given approval (the gate has not reached its wait-task yet, or was
// already approved).
var ErrNoPendingApproval = errors.New("stepfunctions: no gate awaiting this approval")

// New builds a Provider over already-provisioned resources (see Provision).
func New(client *sfnutil.Client, resources Provisioned, opts Options) *Provider {
	if opts.TokenStore == nil {
		opts.TokenStore = NewMemoryTokenStore()
	}
	return &Provider{
		client:     client,
		resources:  resources,
		tokens:     opts.TokenStore,
		workerName: opts.WorkerName,
	}
}

// NewFromConfig provisions the interpreter resources (idempotently) and returns
// a ready Provider.
func NewFromConfig(ctx context.Context, cfg ProvisionConfig, opts Options) (*Provider, error) {
	client, err := sfnutil.NewClientDefault(ctx, cfg.Region)
	if err != nil {
		return nil, err
	}
	resources, err := ProvisionWithClient(ctx, client, cfg)
	if err != nil {
		return nil, err
	}
	return New(client, *resources, opts), nil
}

var _ omniworkflow.WorkflowProvider = (*Provider)(nil)

// Start begins executing req.Plan as a Step Functions execution named req.ID.
// The execution name is the idempotency key: re-starting with the same ID
// returns the existing run rather than creating a duplicate.
func (p *Provider) Start(ctx context.Context, req omniworkflow.StartRequest) (*omniworkflow.Run, error) {
	if err := req.Plan.Validate(); err != nil {
		return nil, err
	}
	blob := stateBlob{
		RunID: req.ID,
		Plan:  req.Plan,
		Progress: progress{
			Succeeded: map[string]bool{},
			Failed:    map[string]bool{},
			Approved:  map[string]bool{},
		},
	}
	input, err := json.Marshal(blob)
	if err != nil {
		return nil, err
	}

	_, err = p.client.StartExecution(ctx, p.resources.StateMachineARN, req.ID, string(input))
	if err != nil {
		var already *types.ExecutionAlreadyExists
		if errors.As(err, &already) {
			// Idempotent: the run already exists; return its current state.
			return p.Get(ctx, req.ID)
		}
		return nil, err
	}
	return &omniworkflow.Run{
		ID:       req.ID,
		Status:   omniworkflow.StatusRunning,
		Metadata: map[string]string{"executionArn": p.executionARN(req.ID)},
	}, nil
}

// Get returns the current state of the run with the given id.
func (p *Provider) Get(ctx context.Context, id string) (*omniworkflow.Run, error) {
	exec, err := p.client.DescribeExecution(ctx, p.executionARN(id))
	if err != nil {
		var missing *types.ExecutionDoesNotExist
		if errors.As(err, &missing) {
			return nil, fmt.Errorf("%w: %q", omniworkflow.ErrRunNotFound, id)
		}
		return nil, err
	}

	run := &omniworkflow.Run{
		ID:       id,
		Status:   mapStatus(exec.Status),
		Metadata: map[string]string{"executionArn": exec.ARN},
	}
	if run.Status == omniworkflow.StatusSucceeded && exec.Output != "" {
		var out stateBlob
		if err := json.Unmarshal([]byte(exec.Output), &out); err == nil {
			run.Output = out.Progress.Succeeded
		}
	}
	return run, nil
}

// Signal delivers an approval to a waiting gate. The signal name must be
// ApproveSignal(gateID); the gate's task token is looked up and released via
// SendTaskSuccess.
func (p *Provider) Signal(ctx context.Context, id string, signal omniworkflow.Signal) error {
	gateID, ok := strings.CutPrefix(signal.Name, "approve:")
	if !ok {
		return fmt.Errorf("stepfunctions: unsupported signal %q (only approve:<stepID> is supported)", signal.Name)
	}
	token, found, err := p.tokens.Get(ctx, id, gateID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: run %q gate %q", ErrNoPendingApproval, id, gateID)
	}

	result, err := json.Marshal(waveResult{ID: gateID, Kind: kindGate, Approved: true})
	if err != nil {
		return err
	}
	if err := p.client.SendTaskSuccess(ctx, token, string(result)); err != nil {
		return err
	}
	return p.tokens.Delete(ctx, id, gateID)
}

// Cancel aborts the run's execution. Get will then report StatusCancelled.
func (p *Provider) Cancel(ctx context.Context, id string) error {
	return p.client.StopExecution(ctx, p.executionARN(id), "OmniWorkflowCancelled", "cancelled by caller")
}

// executionARN reconstructs an execution ARN from the state machine ARN and the
// run ID (which is the execution name), avoiding any need to persist a
// runID->ARN mapping. State machine ARNs and execution ARNs differ only in the
// ":stateMachine:" vs ":execution:" segment plus the trailing execution name.
func (p *Provider) executionARN(runID string) string {
	base := strings.Replace(p.resources.StateMachineARN, ":stateMachine:", ":execution:", 1)
	return base + ":" + runID
}

// mapStatus normalizes a Step Functions execution status into a RunStatus.
func mapStatus(s types.ExecutionStatus) omniworkflow.RunStatus {
	switch s {
	case types.ExecutionStatusSucceeded:
		return omniworkflow.StatusSucceeded
	case types.ExecutionStatusFailed, types.ExecutionStatusTimedOut:
		return omniworkflow.StatusFailed
	case types.ExecutionStatusAborted:
		return omniworkflow.StatusCancelled
	default:
		// Running, PendingRedrive, and any future status are in-progress.
		return omniworkflow.StatusRunning
	}
}
