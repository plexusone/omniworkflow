// Package inmemory is a process-local reference implementation of
// omniworkflow.WorkflowProvider. It has no durability — state lives in memory
// and is lost on restart — which is exactly right for its two jobs: running the
// conformance suite hermetically (no AWS, no Temporal), and giving consumers a
// fast local provider for their own tests.
//
// It reuses the shared interpreter core and executes wave-by-wave, matching the
// Step Functions provider's observable semantics so all providers stay
// interchangeable. It is a reference, not a durable engine; for durable
// non-AWS/non-Temporal orchestration, wrap an embeddable engine (e.g.
// durabletask-go) as a separate provider.
package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/plexusone/omniworkflow"
	"github.com/plexusone/omniworkflow/interp"
)

// Provider is the in-memory WorkflowProvider.
type Provider struct {
	mu       sync.Mutex
	executor omniworkflow.StepExecutor
	runs     map[string]*run
}

// New returns an empty in-memory Provider. Register an executor and start the
// lifecycle by obtaining a Worker with NewWorker and running it.
func New() *Provider {
	return &Provider{runs: map[string]*run{}}
}

var _ omniworkflow.WorkflowProvider = (*Provider)(nil)

// ErrNoExecutor is returned by Start when no worker/executor has been
// registered yet (call NewWorker first).
var ErrNoExecutor = errors.New("omniworkflow/inmemory: no executor registered; call NewWorker first")

// ErrNoPendingApproval is returned by Signal when no gate is currently awaiting
// the given approval.
var ErrNoPendingApproval = errors.New("omniworkflow/inmemory: no gate awaiting this approval")

// run is the in-memory state of a single execution. Only its driver goroutine
// mutates state and status (guarded by mu for concurrent Get/Signal/Cancel).
type run struct {
	plan   omniworkflow.Plan
	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	state     interp.State
	status    omniworkflow.RunStatus
	approvals map[string]chan struct{} // gateID -> channel closed on approval
}

// Start begins executing req.Plan. The ID is the idempotency key: starting
// again with the same ID returns the existing run without relaunching it.
func (p *Provider) Start(_ context.Context, req omniworkflow.StartRequest) (*omniworkflow.Run, error) {
	if err := req.Plan.Validate(); err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.executor == nil {
		p.mu.Unlock()
		return nil, ErrNoExecutor
	}
	if existing, ok := p.runs[req.ID]; ok {
		p.mu.Unlock()
		return existing.snapshot(req.ID), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &run{
		plan:   req.Plan,
		ctx:    ctx,
		cancel: cancel,
		state: interp.State{
			Succeeded: map[string]bool{},
			Failed:    map[string]bool{},
			Running:   map[string]bool{},
			Approved:  map[string]bool{},
		},
		status:    omniworkflow.StatusRunning,
		approvals: map[string]chan struct{}{},
	}
	p.runs[req.ID] = r
	executor := p.executor
	p.mu.Unlock()

	go p.drive(req.ID, r, executor)
	return r.snapshot(req.ID), nil
}

// Get returns the current state of the run.
func (p *Provider) Get(_ context.Context, id string) (*omniworkflow.Run, error) {
	p.mu.Lock()
	r, ok := p.runs[id]
	p.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", omniworkflow.ErrRunNotFound, id)
	}
	return r.snapshot(id), nil
}

// Signal approves a waiting gate. The signal name must be ApproveSignal(gateID).
func (p *Provider) Signal(_ context.Context, id string, signal omniworkflow.Signal) error {
	gateID, ok := cutApprove(signal.Name)
	if !ok {
		return fmt.Errorf("omniworkflow/inmemory: unsupported signal %q (only approve:<stepID> is supported)", signal.Name)
	}
	p.mu.Lock()
	r, ok := p.runs[id]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %q", omniworkflow.ErrRunNotFound, id)
	}
	return r.approve(gateID)
}

// Cancel aborts the run. Get then reports StatusCancelled.
func (p *Provider) Cancel(_ context.Context, id string) error {
	p.mu.Lock()
	r, ok := p.runs[id]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %q", omniworkflow.ErrRunNotFound, id)
	}
	r.cancel()
	return nil
}

// drive runs the wave-based interpreter loop until the run reaches a terminal
// status, mirroring the Step Functions coordinator/Map cycle: advance, dispatch
// the ready steps and awaiting gates as one barrier wave, fold the results back
// in, repeat.
func (p *Provider) drive(runID string, r *run, executor omniworkflow.StepExecutor) {
	for {
		if r.ctx.Err() != nil {
			r.setStatus(omniworkflow.StatusCancelled)
			return
		}

		d := interp.Advance(r.plan, r.readState())
		if d.Terminal != "" {
			r.setStatus(d.Terminal)
			return
		}

		type outcome struct {
			id       string
			isGate   bool
			ok       bool
			approved bool
		}
		results := make(chan outcome, len(d.Ready)+len(d.AwaitingApproval))
		var wg sync.WaitGroup

		for _, id := range d.Ready {
			step := r.step(id)
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := executor.ExecuteStep(r.ctx, runID, step)
				results <- outcome{id: id, ok: err == nil}
			}()
		}
		for _, id := range d.AwaitingApproval {
			ch := r.approvalChan(id)
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-ch:
					results <- outcome{id: id, isGate: true, approved: true}
				case <-r.ctx.Done():
				}
			}()
		}
		wg.Wait()
		close(results)

		if r.ctx.Err() != nil {
			r.setStatus(omniworkflow.StatusCancelled)
			return
		}

		r.mu.Lock()
		for o := range results {
			switch {
			case o.isGate:
				if o.approved {
					r.state.Approved[o.id] = true
				}
			case o.ok:
				r.state.Succeeded[o.id] = true
			default:
				r.state.Failed[o.id] = true
			}
		}
		r.mu.Unlock()
	}
}

func (r *run) readState() interp.State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return interp.State{
		Succeeded: cloneBoolMap(r.state.Succeeded),
		Failed:    cloneBoolMap(r.state.Failed),
		Running:   map[string]bool{},
		Approved:  cloneBoolMap(r.state.Approved),
	}
}

func (r *run) setStatus(s omniworkflow.RunStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = s
}

func (r *run) snapshot(id string) *omniworkflow.Run {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := &omniworkflow.Run{ID: id, Status: r.status}
	if r.status == omniworkflow.StatusSucceeded {
		out.Output = cloneBoolMap(r.state.Succeeded)
	}
	return out
}

func (r *run) step(id string) omniworkflow.PlanStep {
	for _, s := range r.plan.Steps {
		if s.ID == id {
			return s
		}
	}
	return omniworkflow.PlanStep{ID: id}
}

// approvalChan returns the approval channel for a gate, creating it on first
// use so Signal can find and close it.
func (r *run) approvalChan(gateID string) chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ch, ok := r.approvals[gateID]; ok {
		return ch
	}
	ch := make(chan struct{})
	r.approvals[gateID] = ch
	return ch
}

// approve closes a gate's approval channel. It returns ErrNoPendingApproval if
// the gate is not currently awaiting (its wait-task has not registered yet or
// it was already approved).
func (r *run) approve(gateID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch, ok := r.approvals[gateID]
	if !ok {
		return fmt.Errorf("%w: gate %q", ErrNoPendingApproval, gateID)
	}
	close(ch)
	delete(r.approvals, gateID) // prevent a double close on a second Signal
	return nil
}

func cloneBoolMap(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cutApprove(name string) (string, bool) {
	const prefix = "approve:"
	if len(name) > len(prefix) && name[:len(prefix)] == prefix {
		return name[len(prefix):], true
	}
	return "", false
}
