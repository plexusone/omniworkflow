// Package stepfunctions is the AWS Step Functions provider for OmniWorkflow.
// It runs a Plan on a generic interpreter state machine: a coordinator
// activity computes readiness/gates/terminal via the shared interpreter core
// (github.com/plexusone/omniworkflow/interp), and each wave of ready steps and
// awaiting gates is dispatched to a step activity that an embedded worker
// polls. Callers never author Amazon States Language.
package stepfunctions

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	_ "embed"
)

//go:embed interpreter.asl.json
var interpreterASLTemplate string

// Placeholders substituted into the embedded Amazon States Language template.
const (
	placeholderCoordinatorARN = "${CoordinatorActivityArn}"
	placeholderStepARN        = "${StepActivityArn}"
)

// ErrEmptyActivityARN is returned when rendering with a missing activity ARN.
var ErrEmptyActivityARN = errors.New("stepfunctions: activity ARN is empty")

// RenderInterpreterASL substitutes the coordinator and step activity ARNs into
// the embedded interpreter template and returns the resulting Amazon States
// Language definition. It validates that the result is well-formed JSON so a
// malformed template can never reach CreateStateMachine.
func RenderInterpreterASL(coordinatorActivityARN, stepActivityARN string) (string, error) {
	if strings.TrimSpace(coordinatorActivityARN) == "" || strings.TrimSpace(stepActivityARN) == "" {
		return "", ErrEmptyActivityARN
	}
	asl := strings.ReplaceAll(interpreterASLTemplate, placeholderCoordinatorARN, coordinatorActivityARN)
	asl = strings.ReplaceAll(asl, placeholderStepARN, stepActivityARN)

	if !json.Valid([]byte(asl)) {
		return "", fmt.Errorf("stepfunctions: rendered ASL is not valid JSON")
	}
	return asl, nil
}
