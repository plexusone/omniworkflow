// Package omniworkflow is a provider-neutral durable-orchestration
// abstraction: one Go interface (WorkflowProvider) with interchangeable
// backends (AWS Step Functions and Temporal), so a caller writes pipeline-
// shaped orchestration once and runs it on either engine by configuration.
//
// It follows the plexusone Omni* convention: OmniX names are provider-neutral
// capability libraries, XForge names are the products that consume them.
// ActionForge (github.com/plexusone/actionforge) is the first consumer.
//
// # Model
//
// Callers hand Start a Plan — a flat DAG of PlanStep values connected by
// DependsOn edges, with Gate steps that hold until an approval Signal — and
// register a StepExecutor callback that performs each step's work. Each
// provider ships a generic DAG interpreter plus an embedded worker
// (NewWorker), so callers never author Temporal workflow code or Step
// Functions state-machine definitions. The normalized RunStatus vocabulary
// hides each backend's native execution-status types from callers.
//
// The Plan is deliberately not a general-purpose workflow language: loops,
// dynamic fan-out, and arbitrary control flow are out of scope. It targets
// pipeline-shaped work.
package omniworkflow

// Version is the module's semantic version.
const Version = "0.1.0"
