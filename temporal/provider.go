package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/plexusone/omniworkflow"
)

// Defaults.
const (
	DefaultTaskQueue = "omniworkflow"

	// activityStartToCloseTimeout bounds a single step execution. It is
	// generous because steps (e.g. builds) can be long; long human waits happen
	// at gates, which are signals, not activities.
	activityStartToCloseTimeout = 24 * time.Hour
)

// Provider is the Temporal implementation of omniworkflow.WorkflowProvider.
type Provider struct {
	client    client.Client
	taskQueue string
}

// Config configures a dialed Provider.
type Config struct {
	// HostPort is the Temporal frontend address; empty uses the SDK default
	// (127.0.0.1:7233).
	HostPort string
	// Namespace is the Temporal namespace; empty uses "default".
	Namespace string
	// TaskQueue is the queue the interpreter workflow and step activity run on;
	// empty uses DefaultTaskQueue.
	TaskQueue string
}

// New builds a Provider over an existing Temporal client. taskQueue empty uses
// DefaultTaskQueue.
func New(c client.Client, taskQueue string) *Provider {
	if taskQueue == "" {
		taskQueue = DefaultTaskQueue
	}
	return &Provider{client: c, taskQueue: taskQueue}
}

// NewFromConfig dials a Temporal client from cfg and returns a Provider. The
// caller owns the client's lifecycle via Close.
func NewFromConfig(cfg Config) (*Provider, error) {
	c, err := client.Dial(client.Options{
		HostPort:  cfg.HostPort,
		Namespace: cfg.Namespace,
	})
	if err != nil {
		return nil, err
	}
	return New(c, cfg.TaskQueue), nil
}

// Close closes the underlying Temporal client.
func (p *Provider) Close() { p.client.Close() }

var _ omniworkflow.WorkflowProvider = (*Provider)(nil)

// Start begins the interpreter workflow for req.Plan with workflow ID req.ID.
// The workflow ID is the idempotency key: starting again while a run with the
// same ID is open returns the existing run instead of creating a duplicate.
func (p *Provider) Start(ctx context.Context, req omniworkflow.StartRequest) (*omniworkflow.Run, error) {
	if err := req.Plan.Validate(); err != nil {
		return nil, err
	}
	opts := client.StartWorkflowOptions{
		ID:        req.ID,
		TaskQueue: p.taskQueue,
	}
	run, err := p.client.ExecuteWorkflow(ctx, opts, workflowName, interpreterInput{RunID: req.ID, Plan: req.Plan})
	if err != nil {
		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &already) {
			return p.Get(ctx, req.ID) // idempotent
		}
		return nil, err
	}
	return &omniworkflow.Run{
		ID:       req.ID,
		Status:   omniworkflow.StatusRunning,
		Metadata: map[string]string{"workflowId": req.ID, "runId": run.GetRunID()},
	}, nil
}

// Get returns the current state of the run.
func (p *Provider) Get(ctx context.Context, id string) (*omniworkflow.Run, error) {
	desc, err := p.client.DescribeWorkflowExecution(ctx, id, "")
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, fmt.Errorf("%w: %q", omniworkflow.ErrRunNotFound, id)
		}
		return nil, err
	}
	status := mapStatus(desc.GetWorkflowExecutionInfo().GetStatus())
	run := &omniworkflow.Run{ID: id, Status: status, Metadata: map[string]string{"workflowId": id}}

	if status == omniworkflow.StatusSucceeded {
		var out map[string]bool
		if err := p.client.GetWorkflow(ctx, id, "").Get(ctx, &out); err == nil {
			run.Output = out
		}
	}
	return run, nil
}

// Signal delivers a gate approval. The signal name must be ApproveSignal(gateID).
func (p *Provider) Signal(ctx context.Context, id string, signal omniworkflow.Signal) error {
	gateID, ok := strings.CutPrefix(signal.Name, "approve:")
	if !ok {
		return fmt.Errorf("temporal: unsupported signal %q (only approve:<stepID> is supported)", signal.Name)
	}
	if err := p.client.SignalWorkflow(ctx, id, "", approveSignalName, gateID); err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return fmt.Errorf("%w: %q", omniworkflow.ErrRunNotFound, id)
		}
		return err
	}
	return nil
}

// Cancel requests cancellation of the run. Get then reports StatusCancelled.
func (p *Provider) Cancel(ctx context.Context, id string) error {
	if err := p.client.CancelWorkflow(ctx, id, ""); err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return fmt.Errorf("%w: %q", omniworkflow.ErrRunNotFound, id)
		}
		return err
	}
	return nil
}

// mapStatus normalizes a Temporal workflow execution status into a RunStatus.
func mapStatus(s enumspb.WorkflowExecutionStatus) omniworkflow.RunStatus {
	switch s {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return omniworkflow.StatusSucceeded
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return omniworkflow.StatusFailed
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return omniworkflow.StatusCancelled
	default:
		// Running, ContinuedAsNew, and any future status are in-progress.
		return omniworkflow.StatusRunning
	}
}
