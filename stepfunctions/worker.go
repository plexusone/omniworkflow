package stepfunctions

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/plexusone/omniworkflow"
)

// pollErrorBackoff is the pause after a failed activity poll, to avoid a hot
// loop against a persistently failing endpoint.
const pollErrorBackoff = 2 * time.Second

// NewWorker returns a Worker that drives this provider's interpreter: it polls
// the coordinator activity (advancing the DAG) and the step activity (executing
// steps and holding gate tokens), dispatching step work to executor. Run it in
// the caller's process; it shares the provider's TokenStore so Signal can
// release gates the worker is holding.
func (p *Provider) NewWorker(executor omniworkflow.StepExecutor) omniworkflow.Worker {
	return &worker{provider: p, executor: executor}
}

type worker struct {
	provider *Provider
	executor omniworkflow.StepExecutor
}

// Run polls the coordinator and step activities until ctx is cancelled. A clean
// shutdown via ctx cancellation returns nil.
func (w *worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		w.pollLoop(ctx, w.provider.resources.CoordinatorActivityARN, w.handleCoordinatorTask)
	}()
	go func() {
		defer wg.Done()
		w.pollLoop(ctx, w.provider.resources.StepActivityARN, w.handleStepTask)
	}()
	wg.Wait()
	if ctx.Err() != nil {
		return nil // clean shutdown
	}
	return nil
}

// pollLoop long-polls one activity ARN, invoking handle for each task with a
// non-empty token, until ctx is cancelled.
func (w *worker) pollLoop(ctx context.Context, activityARN string, handle func(context.Context, string, string)) {
	for {
		if ctx.Err() != nil {
			return
		}
		task, err := w.provider.client.PollActivityTask(ctx, activityARN, w.provider.workerName)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !sleepCtx(ctx, pollErrorBackoff) {
				return
			}
			continue
		}
		if task == nil || task.TaskToken == "" {
			continue // long poll timed out with no work
		}
		handle(ctx, task.TaskToken, task.Input)
	}
}

// handleCoordinatorTask advances the interpreter one wave and returns the next
// state blob.
func (w *worker) handleCoordinatorTask(ctx context.Context, token, input string) {
	var in stateBlob
	if err := json.Unmarshal([]byte(input), &in); err != nil {
		_ = w.provider.client.SendTaskFailure(ctx, token, "BadCoordinatorInput", err.Error())
		return
	}
	out := coordinate(in)
	payload, err := json.Marshal(out)
	if err != nil {
		_ = w.provider.client.SendTaskFailure(ctx, token, "BadCoordinatorOutput", err.Error())
		return
	}
	_ = w.provider.client.SendTaskSuccess(ctx, token, string(payload))
}

// handleStepTask executes a step, or records a gate's token to await approval.
func (w *worker) handleStepTask(ctx context.Context, token, input string) {
	var in struct {
		RunID string       `json:"runId"`
		Item  dispatchItem `json:"item"`
	}
	if err := json.Unmarshal([]byte(input), &in); err != nil {
		_ = w.provider.client.SendTaskFailure(ctx, token, "BadStepInput", err.Error())
		return
	}

	switch in.Item.Kind {
	case kindGate:
		// Hold: record the token so Signal can release it. The Map iteration
		// stays pending until approval completes this token.
		_ = w.provider.tokens.Put(ctx, in.RunID, in.Item.ID, token)
	case kindStep:
		result := executeStepItem(ctx, w.executor, in.RunID, in.Item)
		payload, err := json.Marshal(result)
		if err != nil {
			_ = w.provider.client.SendTaskFailure(ctx, token, "BadStepResult", err.Error())
			return
		}
		_ = w.provider.client.SendTaskSuccess(ctx, token, string(payload))
	default:
		_ = w.provider.client.SendTaskFailure(ctx, token, "UnknownItemKind", in.Item.Kind)
	}
}

// sleepCtx sleeps for d or until ctx is cancelled. It returns false if ctx was
// cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
