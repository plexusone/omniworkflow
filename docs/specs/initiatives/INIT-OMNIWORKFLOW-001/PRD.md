# PRD — OmniWorkflow — Portable Durable-Orchestration Provider Abstraction

**Initiative:** `INIT-OMNIWORKFLOW-001`
**Status:** Draft
**Home repo:** `github.com/plexusone/omniworkflow`

## Problem

ActionForge (`INIT-ACTIONFORGE-001`) needs durable pipeline orchestration
(stage handoffs, approvals, long waits, retries) but must not hard-code a
single backend: a self-hosted Product Builder deployment wants Temporal
(first-class long waits, no per-transition metering), while an AWS-hosted
low-traffic deployment wants Step Functions (metered, near-zero idle cost, no
cluster to operate). Building this abstraction *inside* ActionForge would tie
a reusable capability to one product, and violates the ecosystem's own
naming/layering convention: `OmniX` names are provider-neutral abstraction
libraries; `XForge` names are the products that consume them (see
`SystemForge` underneath `AgentForge`/`ActionForge`/`DashForge` for the
established pattern).

## Goals

- **One Go interface, two conformance-tested providers, from v1.** A
  `WorkflowProvider` interface (`Start`/`Get`/`Signal`/`Cancel`) with a
  Temporal provider and an AWS Step Functions provider, both required to pass
  the same behavioral contract test suite — not one provider now with the
  other deferred.
- **Own the definition plane, not just the control plane.** Each provider
  ships a generic DAG-interpreter workflow body: callers hand `Start` a
  `Plan` (a flat step DAG with `needs` edges and approval gates) and register
  a `StepExecutor` callback; the provider's embedded worker walks the DAG and
  invokes the executor per step. Callers never write Temporal workflow code
  or Step Functions ASL — without this, "provider-neutral" would be false
  advertising, since every consumer would still author per-provider workflow
  bodies.
- **Normalize the common 80–90%, not force artificial parity.** Long waits,
  signals/approvals, retries, and cancellation must behave identically from
  the caller's perspective. Provider-specific capabilities that don't
  generalize are exposed as typed extensions, not smuggled into the core
  interface (same philosophy as an OmniLLM-style abstraction: normalize the
  common surface, keep differentiated functionality reachable).
- **No payloads in workflow state.** The interface carries references
  (workflow/execution IDs, small structured inputs/outputs), never large
  blobs — callers own their own artifact storage (ActionForge's Postgres +
  S3, for example).
- **Ship as an importable library**, not a service — `go get
  github.com/plexusone/omniworkflow`, no separate OmniWorkflow deployment to
  operate.

## Non-Goals (this initiative)

- A general-purpose workflow programming model. The `Plan` is deliberately a
  flat step DAG with dependency edges and approval gates — enough for
  pipeline-shaped work. Loops, dynamic fan-out, and arbitrary control flow
  are not in scope; a consumer needing those writes provider-native workflow
  code outside this library.
- Additional providers beyond Temporal and Step Functions (Azure Durable
  Functions, etc.) — added later if a concrete consumer needs one, following
  the same conformance-suite discipline.
- A hosted/managed OmniWorkflow control plane — this is a library consumed
  in-process by callers (ActionForge today), not a standalone service.
- Building ActionForge-specific orchestration logic (pipeline stage
  semantics, approval UI, artifact handling) — that stays in ActionForge and
  consumes this library.

## Users and Experience

- **ActionForge (first consumer)**: depends on `omniworkflow`, picks a
  provider via configuration, and gets identical pipeline-orchestration
  behavior regardless of which one is configured.
- **Future Forge/PlexusOne consumers**: any durable-workflow need elsewhere
  in the ecosystem (per the original ideation chat's broader "durable
  Product Workflow Engine" framing) can depend on the same library instead of
  re-deriving a Temporal-or-Step-Functions choice from scratch.
- **Self-hosted operators**: configure the Temporal provider, point at a
  Temporal cluster they run.
- **AWS-hosted operators**: configure the Step Functions provider, no
  additional infrastructure to operate beyond what AWS manages.

## Success Criteria

- A single caller-facing test suite (the conformance suite) passes
  identically against both providers: start a `Plan`, observe step execution
  through a registered `StepExecutor` in dependency order, hold at an
  approval gate until signaled, cancel mid-run, and observe failure
  propagation — all with the same caller code.
- ActionForge's Phase 2 integration test (toy pipeline, build → test →
  package) runs unmodified against both providers by swapping only
  configuration.
- The public interface has zero Temporal-specific or Step-Functions-specific
  types leaking through it — a caller written against the interface cannot
  tell which provider is running underneath without querying provider
  metadata explicitly.
