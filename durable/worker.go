package durable

import (
	"context"

	"github.com/plexusone/omniworkflow"
)

// NewWorker registers executor as the provider's step executor and returns a
// Worker. Running the worker resumes any non-terminal runs found in the store.
func (p *Provider) NewWorker(executor omniworkflow.StepExecutor) omniworkflow.Worker {
	p.mu.Lock()
	p.executor = executor
	p.mu.Unlock()
	return &worker{provider: p, executor: executor}
}

type worker struct {
	provider *Provider
	executor omniworkflow.StepExecutor
}

// Run resumes persisted non-terminal runs, then blocks until ctx is cancelled,
// at which point it stops all in-flight runs. Resume failures are returned;
// a clean shutdown returns nil.
func (w *worker) Run(ctx context.Context) error {
	if err := w.provider.resume(ctx, w.executor); err != nil {
		return err
	}
	<-ctx.Done()
	w.provider.cancelAll()
	return nil
}

func (p *Provider) cancelAll() {
	p.mu.Lock()
	runs := make([]*run, 0, len(p.runs))
	for _, r := range p.runs {
		runs = append(runs, r)
	}
	p.mu.Unlock()
	for _, r := range runs {
		r.cancel()
	}
	// Wait for every driver to observe cancellation and finish its final
	// checkpoint, so Run returns only after all persistence has settled.
	p.drivers.Wait()
}
