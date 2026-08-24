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

### Phase 1 — Interface, Providers & Conformance Suite

Single phase: the interface, both providers, and the suite that proves they
agree are one unit of work — a provider without conformance proof isn't
done, and the interface without a second provider risks over-fitting to
Temporal alone.

1. `RMI-OMNIWORKFLOW-001` Library scaffolding: `go.mod`, package layout, CI
   and lint config per org conventions.
2. `RMI-OMNIWORKFLOW-002` `WorkflowProvider` interface and shared types
   (`StartRequest`, `Run`, `Signal`, normalized `RunStatus`).
3. `RMI-OMNIWORKFLOW-003` Temporal provider implementation.
4. `RMI-OMNIWORKFLOW-004` AWS Step Functions provider implementation,
   including task-token signal bridging for mid-execution approvals.
5. `RMI-OMNIWORKFLOW-005` Conformance test suite, run against both providers.

**Exit criteria:** both providers pass the identical conformance suite; a
caller can start/get/signal/cancel a workflow through the interface alone,
with zero provider-specific types in its own code.

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
