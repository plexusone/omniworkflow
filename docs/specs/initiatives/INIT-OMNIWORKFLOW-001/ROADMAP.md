# ROADMAP — OmniWorkflow — Portable Durable-Orchestration Provider Abstraction

**Initiative:** `INIT-OMNIWORKFLOW-001`
**Repository:** `github.com/plexusone/omniworkflow`

`INIT-ACTIONFORGE-001` requires this initiative (`RMI-ACTIONFORGE-007`
integrates the provider produced here); this initiative has no dependency on
ActionForge and can proceed independently.

## Phase 1 — Interface, Interpreter, Providers & Conformance Suite

**Theme:** One interface, a shared DAG interpreter, two conformance-tested providers — a provider without proof isn't done

- [ ] `RMI-OMNIWORKFLOW-001` Library scaffolding: go.mod, package layout, CI and lint config per org conventions
- [ ] `RMI-OMNIWORKFLOW-002` WorkflowProvider interface and shared types (StartRequest, Run, Signal, RunStatus, Plan, PlanStep, StepExecutor)
  - Depends on: `RMI-OMNIWORKFLOW-001`
- [ ] `RMI-OMNIWORKFLOW-006` Shared DAG-interpreter core: provider-independent readiness, gate, and failure-propagation semantics
  - Depends on: `RMI-OMNIWORKFLOW-002`
- [ ] `RMI-OMNIWORKFLOW-003` Temporal provider: control-plane mapping plus embedded worker hosting the interpreter workflow and executor activity
  - Depends on: `RMI-OMNIWORKFLOW-006`
- [ ] `RMI-OMNIWORKFLOW-007` Step Functions state-machine template and one-time provisioning helper
  - Depends on: `RMI-OMNIWORKFLOW-002`
- [ ] `RMI-OMNIWORKFLOW-004` Step Functions provider: control-plane mapping, Activity-polling worker, task-token signal bridging for gates
  - Depends on: `RMI-OMNIWORKFLOW-006`
  - Depends on: `RMI-OMNIWORKFLOW-007`
- [ ] `RMI-OMNIWORKFLOW-005` Conformance test suite (steps, ordering, gates, cancel, failure, idempotent start), run against both providers
  - Depends on: `RMI-OMNIWORKFLOW-003`
  - Depends on: `RMI-OMNIWORKFLOW-004`
