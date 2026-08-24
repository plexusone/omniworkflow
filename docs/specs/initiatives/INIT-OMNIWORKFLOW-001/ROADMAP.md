# ROADMAP — OmniWorkflow — Portable Durable-Orchestration Provider Abstraction

**Initiative:** `INIT-OMNIWORKFLOW-001`
**Repository:** `github.com/plexusone/omniworkflow`

`INIT-ACTIONFORGE-001` requires this initiative (`RMI-ACTIONFORGE-007`
integrates the provider produced here); this initiative has no dependency on
ActionForge and can proceed independently.

## Phase 1 — Interface, Providers & Conformance Suite

**Theme:** One interface, two conformance-tested providers — a provider without proof isn't done

- [ ] `RMI-OMNIWORKFLOW-001` Library scaffolding: go.mod, package layout, CI and lint config per org conventions
- [ ] `RMI-OMNIWORKFLOW-002` WorkflowProvider interface and shared types (StartRequest, Run, Signal, normalized RunStatus)
  - Depends on: `RMI-OMNIWORKFLOW-001`
- [ ] `RMI-OMNIWORKFLOW-003` Temporal provider implementation
  - Depends on: `RMI-OMNIWORKFLOW-002`
- [ ] `RMI-OMNIWORKFLOW-004` AWS Step Functions provider implementation, including task-token signal bridging for mid-execution approvals
  - Depends on: `RMI-OMNIWORKFLOW-002`
- [ ] `RMI-OMNIWORKFLOW-005` Conformance test suite, run against both providers
  - Depends on: `RMI-OMNIWORKFLOW-003`
  - Depends on: `RMI-OMNIWORKFLOW-004`
