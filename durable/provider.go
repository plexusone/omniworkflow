package durable

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/plexusone/omniworkflow"
	"github.com/plexusone/omniworkflow/interp"
)

// Provider is the durable embedded WorkflowProvider. It checkpoints run state to
// a StateStore after every wave and resumes non-terminal runs on startup.
type Provider struct {
	store StateStore

	mu       sync.Mutex
	executor omniworkflow.StepExecutor
	runs     map[string]*run
	drivers  sync.WaitGroup // tracks live drive goroutines for clean shutdown
}

// New returns a durable Provider backed by store. Register an executor and
// start the lifecycle (including resuming persisted runs) by obtaining a Worker
// with NewWorker and running it.
func New(store StateStore) *Provider {
	return &Provider{store: store, runs: map[string]*run{}}
}

var _ omniworkflow.WorkflowProvider = (*Provider)(nil)

// ErrNoExecutor is returned by Start when no worker/executor has been
// registered yet (call NewWorker first).
var ErrNoExecutor = errors.New("omniworkflow/durable: no executor registered; call NewWorker first")

// ErrNoPendingApproval is returned by Signal when no gate is currently awaiting
// the given approval.
var ErrNoPendingApproval = errors.New("omniworkflow/durable: no gate awaiting this approval")

type run struct {
	plan   omniworkflow.Plan
	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	state      interp.State
	status     omniworkflow.RunStatus
	approvals  map[string]chan struct{}
	userCancel bool // true only when Cancel was called (vs. worker shutdown)
}

// Start begins executing req.Plan, persisting an initial snapshot before the
// driver runs. The ID is the idempotency key: an existing run (in memory or in
// the store) is returned without relaunching.
func (p *Provider) Start(ctx context.Context, req omniworkflow.StartRequest) (*omniworkflow.Run, error) {
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
	p.mu.Unlock()

	// Guard against a run that exists in the store but is not yet loaded.
	if snap, ok, err := p.store.Load(ctx, req.ID); err != nil {
		return nil, err
	} else if ok {
		return &omniworkflow.Run{ID: req.ID, Status: snap.Status, Output: successOutput(snap)}, nil
	}

	r := newRun(req.Plan, interp.State{
		Succeeded: map[string]bool{},
		Failed:    map[string]bool{},
		Running:   map[string]bool{},
		Approved:  map[string]bool{},
	}, omniworkflow.StatusRunning)

	if err := p.save(ctx, req.ID, r); err != nil {
		r.cancel()
		return nil, err
	}

	p.mu.Lock()
	p.runs[req.ID] = r
	executor := p.executor
	p.mu.Unlock()

	// drive runs on the run's own lifecycle context (r.ctx), not the Start
	// caller's request context: a durable run must outlive the call that
	// started it, which is the point of durable orchestration.
	p.drivers.Add(1)
	go p.drive(req.ID, r, executor) //nolint:gosec // G118: intentional — see comment above
	return r.snapshot(req.ID), nil
}

// Get returns the current state of the run, consulting the store if it is not
// in memory (e.g. a terminal run from a previous process).
func (p *Provider) Get(ctx context.Context, id string) (*omniworkflow.Run, error) {
	p.mu.Lock()
	r, ok := p.runs[id]
	p.mu.Unlock()
	if ok {
		return r.snapshot(id), nil
	}
	if snap, ok, err := p.store.Load(ctx, id); err != nil {
		return nil, err
	} else if ok {
		return &omniworkflow.Run{ID: id, Status: snap.Status, Output: successOutput(snap)}, nil
	}
	return nil, fmt.Errorf("%w: %q", omniworkflow.ErrRunNotFound, id)
}

// Signal approves a waiting gate. The signal name must be ApproveSignal(gateID).
func (p *Provider) Signal(_ context.Context, id string, signal omniworkflow.Signal) error {
	gateID, ok := cutApprove(signal.Name)
	if !ok {
		return fmt.Errorf("omniworkflow/durable: unsupported signal %q (only approve:<stepID> is supported)", signal.Name)
	}
	p.mu.Lock()
	r, ok := p.runs[id]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %q", omniworkflow.ErrRunNotFound, id)
	}
	return r.approve(gateID)
}

// Cancel aborts the run.
func (p *Provider) Cancel(_ context.Context, id string) error {
	p.mu.Lock()
	r, ok := p.runs[id]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %q", omniworkflow.ErrRunNotFound, id)
	}
	r.mu.Lock()
	r.userCancel = true
	r.mu.Unlock()
	r.cancel()
	return nil
}

// resume reloads non-terminal runs from the store and relaunches their drivers.
// Terminal runs are left in the store but not driven. Called by the worker once
// an executor is registered.
func (p *Provider) resume(ctx context.Context, executor omniworkflow.StepExecutor) error {
	snaps, err := p.store.List(ctx)
	if err != nil {
		return err
	}
	for _, snap := range snaps {
		if snap.Status.IsTerminal() {
			continue
		}
		p.mu.Lock()
		_, already := p.runs[snap.RunID]
		if already {
			p.mu.Unlock()
			continue
		}
		r := newRun(snap.Plan, interp.State{
			Succeeded: nonNil(snap.Succeeded),
			Failed:    nonNil(snap.Failed),
			Running:   map[string]bool{},
			Approved:  nonNil(snap.Approved),
		}, omniworkflow.StatusRunning)
		p.runs[snap.RunID] = r
		p.mu.Unlock()
		// Resumed runs drive on their own r.ctx, decoupled from the worker's
		// startup context (which only bounds the resume scan itself).
		p.drivers.Add(1)
		go p.drive(snap.RunID, r, executor) //nolint:gosec // G118: intentional — see comment above
	}
	return nil
}

// drive runs the wave-based interpreter loop, checkpointing after each wave.
func (p *Provider) drive(runID string, r *run, executor omniworkflow.StepExecutor) {
	defer p.drivers.Done()
	for {
		if r.ctx.Err() != nil {
			p.stop(runID, r)
			return
		}

		d := interp.Advance(r.plan, r.readState())
		if d.Terminal != "" {
			r.setStatus(d.Terminal)
			_ = p.save(context.Background(), runID, r)
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
			p.stop(runID, r)
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

		// Checkpoint the wave's outcome durably before the next wave.
		if err := p.save(context.Background(), runID, r); err != nil {
			// A store failure must not silently lose durability; stop driving so
			// the run can be resumed once the store recovers.
			return
		}
	}
}

func (p *Provider) save(ctx context.Context, runID string, r *run) error {
	return p.store.Save(ctx, r.toSnapshot(runID))
}

// stop ends a driver because its context was cancelled. An explicit Cancel
// makes the run terminal (CANCELLED, checkpointed); a worker shutdown instead
// leaves the run RUNNING in the store so a later process resumes it.
func (p *Provider) stop(runID string, r *run) {
	r.mu.Lock()
	userCancel := r.userCancel
	r.mu.Unlock()
	if userCancel {
		r.setStatus(omniworkflow.StatusCancelled)
		_ = p.save(context.Background(), runID, r)
	}
}

func newRun(plan omniworkflow.Plan, state interp.State, status omniworkflow.RunStatus) *run {
	ctx, cancel := context.WithCancel(context.Background())
	return &run{
		plan:      plan,
		ctx:       ctx,
		cancel:    cancel,
		state:     state,
		status:    status,
		approvals: map[string]chan struct{}{},
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

func (r *run) toSnapshot(id string) Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Snapshot{
		RunID:     id,
		Plan:      r.plan,
		Succeeded: cloneBoolMap(r.state.Succeeded),
		Failed:    cloneBoolMap(r.state.Failed),
		Approved:  cloneBoolMap(r.state.Approved),
		Status:    r.status,
	}
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

func (r *run) approve(gateID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch, ok := r.approvals[gateID]
	if !ok {
		return fmt.Errorf("%w: gate %q", ErrNoPendingApproval, gateID)
	}
	close(ch)
	delete(r.approvals, gateID)
	return nil
}

func successOutput(snap Snapshot) any {
	if snap.Status == omniworkflow.StatusSucceeded {
		return cloneBoolMap(snap.Succeeded)
	}
	return nil
}

func nonNil(m map[string]bool) map[string]bool {
	if m == nil {
		return map[string]bool{}
	}
	return m
}

func cutApprove(name string) (string, bool) {
	const prefix = "approve:"
	if len(name) > len(prefix) && name[:len(prefix)] == prefix {
		return name[len(prefix):], true
	}
	return "", false
}
