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

### Providers

- **`omniworkflow/temporal`**: `Start` maps to `StartWorkflow`; `Signal` maps
  to Temporal signals; `Cancel` maps to `CancelWorkflow`; long waits are
  native (Temporal timers). Requires a configured Temporal client/cluster.
- **`omniworkflow/stepfunctions`**: `Start` maps to `StartExecution`;
  `Signal` maps to `SendTaskSuccess`/callback-token resumption for a waiting
  state (Step Functions has no native mid-execution signal primitive — this
  is the provider's job to bridge, not the caller's); `Cancel` maps to
  `StopExecution`. Requires AWS credentials and a pre-provisioned state
  machine (or a state-machine-per-workflow-type template the provider
  manages).

### Conformance suite

A single behavioral test suite (`omniworkflow/conformance`) that both
provider packages import and run against a live or test-harness instance of
their backend:

1. Start → Get returns `RUNNING` then a terminal status.
2. Signal delivered while running is observed by the workflow logic.
3. Cancel transitions to `CANCELLED` and stops further execution.
4. A failing workflow surfaces as `FAILED` with a retrievable error, not a
   provider-specific panic/exception shape.
5. Idempotent `Start` with the same ID does not create a duplicate execution.

This suite is the actual product: proving the abstraction holds, not just
declaring it in an interface.

## Risks

- **Step Functions has no native signal primitive.** Approvals/mid-execution
  input require modeling a wait state with a task token and bridging
  `Signal()` to `SendTaskSuccess`/`SendTaskFailure`. This is real
  provider-side complexity, not a thin adapter — budget for it accordingly
  and cover it explicitly in the conformance suite (item 2 above).
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
