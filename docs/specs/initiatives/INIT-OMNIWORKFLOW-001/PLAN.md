# PLAN — OmniWorkflow — Portable Durable-Orchestration Provider Abstraction

**Initiative:** `INIT-OMNIWORKFLOW-001`
**Status:** Draft

## Entry Gate

1. Repo registered in VisionStudio (done: `github.com/plexusone/omniworkflow`).
2. `INIT-ACTIONFORGE-001 --requires--> INIT-OMNIWORKFLOW-001` recorded (done)
   — ActionForge's Phase 2 (`RMI-ACTIONFORGE-007`, integrating the
   orchestration engine) is blocked on this initiative's provider work, not
   the reverse; this initiative has no dependency on ActionForge and can
   start immediately.
3. No other blocking prerequisites — this is a standalone library.

## Build Order

### Phase 1 — Interface, Interpreter, Providers & Conformance Suite

Single phase: the interface, the shared interpreter core, both providers,
and the suite that proves they agree are one unit of work — a provider
without conformance proof isn't done, and the interface without a second
provider risks over-fitting to Temporal alone. The conformance suite's
skeleton should be written alongside the interface (item 2) and run
continuously as the providers develop — its position at the end of the
build order is a completion gate, not a sequencing instruction.

1. `RMI-OMNIWORKFLOW-001` Library scaffolding: `go.mod`, package layout, CI
   and lint config per org conventions.
2. `RMI-OMNIWORKFLOW-002` `WorkflowProvider` interface and shared types
   (`StartRequest`, `Run`, `Signal`, normalized `RunStatus`, `Plan`,
   `PlanStep`, `StepExecutor`).
3. `RMI-OMNIWORKFLOW-006` Shared DAG-interpreter core: provider-independent
   readiness/gate/failure-propagation semantics as a reusable package.
4. `RMI-OMNIWORKFLOW-003` Temporal provider: control-plane mapping plus
   embedded worker hosting the interpreter workflow and executor activity.
5. `RMI-OMNIWORKFLOW-007` Step Functions state-machine template and one-time
   provisioning helper (the ASL that drives SFN Activity tasks).
6. `RMI-OMNIWORKFLOW-004` Step Functions provider: control-plane mapping,
   Activity-polling worker, and task-token signal bridging for gates.
7. `RMI-OMNIWORKFLOW-005` Conformance test suite (steps, ordering, gates,
   cancel, failure, idempotent start), run against both providers.

**Exit criteria:** both providers pass the identical conformance suite; a
caller can start a `Plan`, watch its steps execute through a registered
`StepExecutor`, approve a gate, and cancel — through the interface alone,
with zero provider-specific types (and zero Temporal workflow code or ASL)
in its own code.

## Risks and Mitigations

See TRD Risks: Step Functions' missing native signal primitive (task-token
bridging is real provider-side work, covered explicitly by the conformance
suite), Temporal/Step Functions retry-semantics divergence (keep the core
interface's retry surface minimal; provider-specific tuning via `Metadata`),
single early consumer risk of over-fitting (keep `Input`/`Output` opaque).

## Follow-On

- Additional providers (e.g. Azure Durable Functions) once a concrete
  consumer needs one, following the same conformance-suite discipline
  established here.
- Extraction of a shared retry/timeout policy vocabulary if a second consumer
  surfaces needs the current minimal `Metadata` escape hatch doesn't cover
  cleanly.
- `Plan` expressiveness extensions (per-step retry policies, dynamic
  fan-out, loops) — driven by concrete consumer demand, per the TRD's
  expressiveness-ceiling risk; never added speculatively.
