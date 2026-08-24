# TRD — OmniWorkflow — Portable Durable-Orchestration Provider Abstraction

**Initiative:** `INIT-OMNIWORKFLOW-001`
**Status:** Draft

## Current State

- `github.com/plexusone/omniworkflow` — new repo, git remote configured, no
  code yet.
- First (and currently only) consumer is ActionForge (`INIT-ACTIONFORGE-001`,
  which records a `requires` dependency on this initiative). ActionForge's own
  TRD originally sketched this interface in-repo as `orchestrator` +
  `orchestrator/temporal` + `orchestrator/stepfunctions`; that design moves
  here unchanged in shape, only the module boundary changes.
- Matches the established `plexusone/omni-*` family pattern (`omniagent`,
  `omnillm`, `omni-aws`, `omni-openai`, etc.) — provider-neutral capability
  libraries that Forge products depend on.

## Design

### Interface

```go
package omniworkflow

type WorkflowProvider interface {
    Start(ctx context.Context, req StartRequest) (*Run, error)
    Get(ctx context.Context, id string) (*Run, error)
    Signal(ctx context.Context, id string, signal Signal) error
    Cancel(ctx context.Context, id string) error
}

type StartRequest struct {
    WorkflowType string
    ID           string // caller-assigned idempotency key
    Input        any    // small structured payload, not a blob
}

type Run struct {
    ID       string
    Status   RunStatus // RUNNING | SUCCEEDED | FAILED | CANCELLED
    Output   any
    Metadata map[string]string // provider-specific extras, opt-in
}

type Signal struct {
    Name    string
    Payload any
}
```

`RunStatus` is the normalized vocabulary every provider must map into —
callers never see Temporal's `WorkflowExecutionStatus` or Step Functions'
`ExecutionStatus` directly.

### Definition plane: Plan + StepExecutor + embedded worker

The control-plane interface alone starts nothing runnable — Temporal needs
deterministic workflow code polled by a worker; Step Functions needs a state
machine and something to execute its tasks. OmniWorkflow owns this too
(decided at spec review): each provider ships a **generic DAG-interpreter
workflow body**, so callers never author Temporal workflow code or ASL.

```go
type Plan struct {
    Steps []PlanStep
}

type PlanStep struct {
    ID        string
    DependsOn []string // step IDs; empty = ready immediately
    Gate      bool     // true = hold until Signal("approve:"+ID) before executing
    Input     any      // opaque, passed to the executor verbatim
}

type StepExecutor interface {
    ExecuteStep(ctx context.Context, runID string, step PlanStep) (output any, err error)
}
```

`StartRequest.Input` carries a `Plan`. The caller registers one
`StepExecutor` (ActionForge's invokes Dagger) and embeds the provider's
**worker** in its own process — `worker := provider.NewWorker(executor);
worker.Start(ctx)`. Both backends map onto the same polling shape:

- **Temporal**: the worker hosts one generic interpreter workflow plus one
  activity that delegates to the `StepExecutor`. Interpreter semantics
  (readiness, gates, failure propagation) run as deterministic workflow code;
  the executor runs inside the activity with heartbeats for long steps.
- **Step Functions**: a generic state machine template (provisioned once per
  deployment by a provider helper) drives **SFN Activities** — the worker
  polls `GetActivityTask` for step tasks and completes them with
  `SendTaskSuccess`/`SendTaskFailure`. SFN Activities are the same
  outbound-only polling model as Temporal task queues, so a self-hosted
  process behind NAT works identically on both providers.

The interpreter core (which steps are ready, gate handling, what happens on
step failure) is a shared provider-independent package so the two providers
cannot drift semantically — Temporal runs it inside workflow code; the SFN
state machine encodes the same transitions.

### Providers (control plane mapping)

- **`omniworkflow/temporal`**: `Start` maps to `StartWorkflow` (generic
  interpreter workflow type); `Signal` maps to Temporal signals (gates wait
  on them); `Cancel` maps to `CancelWorkflow`; long waits are native
  (Temporal timers). Requires a configured Temporal client/cluster.
- **`omniworkflow/stepfunctions`**: `Start` maps to `StartExecution` on the
  provisioned interpreter state machine; `Signal` maps to task-token
  resumption (`SendTaskSuccess` on the gate's waiting task — Step Functions
  has no native mid-execution signal primitive; bridging it is the
  provider's job, not the caller's); `Cancel` maps to `StopExecution`.
  Requires AWS credentials and the one-time state-machine provisioning step.

### Conformance suite

A single behavioral test suite (`omniworkflow/conformance`) that both
provider packages import and run against a live or test-harness instance of
their backend:

1. Start a `Plan` → Get returns `RUNNING` then a terminal status.
2. Steps execute through the registered `StepExecutor` in dependency order;
   independent steps may run concurrently, dependents never before their
   prerequisites.
3. A `Gate` step holds until its approval `Signal` arrives, then executes;
   the signal is delivered mid-execution, not queued for a restart.
4. Cancel transitions to `CANCELLED` and no further steps execute.
5. A step returning an error surfaces the run as `FAILED` with a retrievable
   error (not a provider-specific panic/exception shape), and downstream
   dependents do not execute.
6. Idempotent `Start` with the same ID does not create a duplicate execution.

This suite is the actual product: proving the abstraction holds, not just
declaring it in an interface.

## Risks

- **Step Functions has no native signal primitive.** Approvals/mid-execution
  input require modeling a wait state with a task token and bridging
  `Signal()` to `SendTaskSuccess`/`SendTaskFailure`. This is real
  provider-side complexity, not a thin adapter — budget for it accordingly
  and cover it explicitly in the conformance suite (item 3 above).
- **Interpreter semantics can drift between providers.** Temporal expresses
  the DAG walk as workflow code; Step Functions encodes it in a state
  machine. Mitigated by the shared provider-independent interpreter-core
  package (single source of truth for readiness/gate/failure rules) and by
  conformance items 2–5 exercising exactly those rules on both backends.
- **Expressiveness ceiling of the flat Plan.** Consumers will eventually ask
  for loops, dynamic fan-out, or per-step retry policies. Accepted for v1:
  the Plan targets pipeline-shaped work (PRD Non-Goals); extending it is a
  deliberate follow-on, not scope creep inside this initiative.
- **Temporal and Step Functions diverge in retry/timeout semantics.**
  Mitigated by keeping the interface's retry/timeout surface minimal (provider
  defaults apply unless a caller opts into provider-specific configuration via
  `Metadata`), rather than trying to build a fully generic retry-policy DSL
  that neither backend maps to cleanly.
- **Single early consumer (ActionForge).** Risk of over-fitting the interface
  to ActionForge's pipeline shape. Mitigated by keeping `Input`/`Output` as
  opaque `any` payloads (caller-defined schema) rather than pipeline-specific
  types, so a second consumer doesn't require an interface change.

## Testing Strategy

- Unit tests per provider package (request/response mapping, status
  normalization) with mocked SDK clients.
- Conformance suite run against: a Temporal test server (`testsuite` package)
  and a local/mocked Step Functions harness (AWS SDK's local testing support
  or a recorded-fixture approach if no local Step Functions emulator is
  viable).
- Consumer-side proof: ActionForge's Phase 2 end-to-end toy pipeline test
  (owned by `INIT-ACTIONFORGE-001`) is the real acceptance test — it must pass
  unmodified against both providers.
