# OmniWorkflow

[![Go CI][go-ci-svg]][go-ci-url]
[![Go Lint][go-lint-svg]][go-lint-url]
[![Go SAST][go-sast-svg]][go-sast-url]
[![Docs][docs-godoc-svg]][docs-godoc-url]
[![License][license-svg]][license-url]

 [go-ci-svg]: https://github.com/plexusone/omniworkflow/actions/workflows/go-ci.yaml/badge.svg?branch=main
 [go-ci-url]: https://github.com/plexusone/omniworkflow/actions/workflows/go-ci.yaml
 [go-lint-svg]: https://github.com/plexusone/omniworkflow/actions/workflows/go-lint.yaml/badge.svg?branch=main
 [go-lint-url]: https://github.com/plexusone/omniworkflow/actions/workflows/go-lint.yaml
 [go-sast-svg]: https://github.com/plexusone/omniworkflow/actions/workflows/go-sast-codeql.yaml/badge.svg?branch=main
 [go-sast-url]: https://github.com/plexusone/omniworkflow/actions/workflows/go-sast-codeql.yaml
 [docs-godoc-svg]: https://pkg.go.dev/badge/github.com/plexusone/omniworkflow
 [docs-godoc-url]: https://pkg.go.dev/github.com/plexusone/omniworkflow
 [docs-mkdoc-svg]: https://img.shields.io/badge/Go-dev%20guide-blue.svg
 [docs-mkdoc-url]: https://plexusone.dev/omniworkflow
 [repo-url]: https://github.com/plexusone/omniworkflow
 [license-svg]: https://img.shields.io/badge/license-MIT-blue.svg
 [license-url]: https://github.com/plexusone/omniworkflow/blob/main/LICENSE

A provider-neutral durable-orchestration abstraction: one Go interface with
interchangeable backends (**AWS Step Functions** and **Temporal**), so you
write pipeline-shaped orchestration once and run it on either engine by
configuration.

OmniWorkflow follows the plexusone `Omni*` convention — provider-neutral
capability libraries that `*Forge` products consume.
[ActionForge](https://github.com/plexusone/actionforge) is the first consumer.

## Installation

```bash
go get github.com/plexusone/omniworkflow
```

## Model

You hand `Start` a **Plan** — a flat DAG of steps connected by `DependsOn`
edges, with `Gate` steps that hold until an approval `Signal` — and register a
**StepExecutor** callback that performs each step's work. Each provider ships a
generic DAG interpreter plus an embedded worker, so you never author Temporal
workflow code or Step Functions state-machine definitions.

```
Plan ──▶ WorkflowProvider.Start ──▶ [interpreter walks the DAG]
                                          │
                                          ▼
                              StepExecutor.ExecuteStep (your code)
```

The `Plan` targets pipeline-shaped work; it is deliberately not a
general-purpose workflow language (no loops, dynamic fan-out, or arbitrary
control flow).

## Providers

| Provider           | Package                                             | Status  |
| ------------------ | --------------------------------------------------- | ------- |
| In-memory (reference/testing) | `github.com/plexusone/omniworkflow/inmemory`        | ready   |
| Durable embedded (no cluster) | `github.com/plexusone/omniworkflow/durable`         | ready   |
| AWS Step Functions | `github.com/plexusone/omniworkflow/stepfunctions`   | ready   |
| Temporal           | `github.com/plexusone/omniworkflow/temporal`        | ready   |

A shared conformance suite (`github.com/plexusone/omniworkflow/conformance`)
proves every provider behaves identically. The in-memory and durable providers
run it hermetically on every `go test`; the Step Functions provider runs it
against live AWS, and the Temporal provider against a dev or real server, when
configured. The Temporal provider's workflow logic is additionally unit-tested
hermetically in the Temporal test environment.

The **durable** provider survives process restarts with no cluster, broker, or
AWS — it checkpoints a small snapshot (plan + the interpreter's outcome sets)
to a pluggable `StateStore` (a zero-dependency file store by default) after each
wave, and resumes non-terminal runs on startup. Steps are at-least-once, so
step executors must be idempotent. It is the low-idle-cost self-hosted option;
a production deployment can supply a Postgres- or bbolt-backed `StateStore`.

The **in-memory** provider is a non-durable reference for tests and local use.

## License

[MIT](LICENSE)
