package temporal

import (
	"context"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/plexusone/omniworkflow"
)

// NewWorker returns a Worker that registers the interpreter workflow and the
// step activity (bound to executor) and polls the provider's task queue.
func (p *Provider) NewWorker(executor omniworkflow.StepExecutor) omniworkflow.Worker {
	w := worker.New(p.client, p.taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(InterpreterWorkflow, workflow.RegisterOptions{Name: workflowName})
	a := &activities{executor: executor}
	w.RegisterActivityWithOptions(a.RunStep, activity.RegisterOptions{Name: activityName})
	return &temporalWorker{w: w}
}

type temporalWorker struct {
	w worker.Worker
}

// Run starts the Temporal worker and blocks until ctx is cancelled, then stops
// it. A clean shutdown returns nil.
func (tw *temporalWorker) Run(ctx context.Context) error {
	if err := tw.w.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	tw.w.Stop()
	return nil
}
