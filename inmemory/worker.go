package inmemory

import (
	"context"

	"github.com/plexusone/omniworkflow"
)

// NewWorker registers executor as the provider's step executor and returns a
// Worker. Unlike the remote providers there is no polling: the provider drives
// runs directly in goroutines. The Worker exists to satisfy the interface and
// to bound run lifecycle — when its Run context is cancelled, all in-flight runs
// are cancelled.
func (p *Provider) NewWorker(executor omniworkflow.StepExecutor) omniworkflow.Worker {
	p.mu.Lock()
	p.executor = executor
	p.mu.Unlock()
	return &worker{provider: p}
}

type worker struct {
	provider *Provider
}

// Run blocks until ctx is cancelled, then cancels every active run. It always
// returns nil (a clean shutdown).
func (w *worker) Run(ctx context.Context) error {
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
}
