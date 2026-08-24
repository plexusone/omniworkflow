package stepfunctions

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestRenderInterpreterASL(t *testing.T) {
	const coordARN = "arn:aws:states:us-east-1:123456789012:activity:omniworkflow-coordinator"
	const stepARN = "arn:aws:states:us-east-1:123456789012:activity:omniworkflow-step"

	asl, err := RenderInterpreterASL(coordARN, stepARN)
	if err != nil {
		t.Fatalf("RenderInterpreterASL: %v", err)
	}

	// Placeholders must be gone; both ARNs present.
	if strings.Contains(asl, "${") {
		t.Fatalf("unsubstituted placeholder remains in rendered ASL")
	}
	if !strings.Contains(asl, coordARN) || !strings.Contains(asl, stepARN) {
		t.Fatalf("rendered ASL missing an activity ARN")
	}

	// Must be valid JSON with the expected top-level shape.
	var doc struct {
		StartAt string                     `json:"StartAt"`
		States  map[string]json.RawMessage `json:"States"`
	}
	if err := json.Unmarshal([]byte(asl), &doc); err != nil {
		t.Fatalf("rendered ASL is not valid JSON: %v", err)
	}
	if doc.StartAt != "Coordinate" {
		t.Errorf("StartAt = %q, want Coordinate", doc.StartAt)
	}
	for _, want := range []string{"Coordinate", "CheckTerminal", "DispatchWave", "Succeeded", "Failed"} {
		if _, ok := doc.States[want]; !ok {
			t.Errorf("rendered ASL missing state %q", want)
		}
	}
}

func TestRenderInterpreterASLEmptyARN(t *testing.T) {
	if _, err := RenderInterpreterASL("", "step"); !errors.Is(err, ErrEmptyActivityARN) {
		t.Errorf("empty coordinator ARN: err = %v, want ErrEmptyActivityARN", err)
	}
	if _, err := RenderInterpreterASL("coord", "  "); !errors.Is(err, ErrEmptyActivityARN) {
		t.Errorf("blank step ARN: err = %v, want ErrEmptyActivityARN", err)
	}
}

func TestEmbeddedTemplateIsValidJSON(t *testing.T) {
	if !json.Valid([]byte(interpreterASLTemplate)) {
		t.Fatal("embedded interpreter.asl.json is not valid JSON")
	}
}

func TestProvisionRequiresRoleARN(t *testing.T) {
	if _, err := Provision(t.Context(), ProvisionConfig{}); !errors.Is(err, ErrNoRoleARN) {
		t.Errorf("Provision without RoleARN: err = %v, want ErrNoRoleARN", err)
	}
}

func TestProvisionConfigNames(t *testing.T) {
	def := ProvisionConfig{}
	if got := def.stateMachineName(); got != "omniworkflow-interpreter" {
		t.Errorf("default stateMachineName = %q", got)
	}
	custom := ProvisionConfig{NamePrefix: "acme"}
	if got := custom.coordinatorActivity(); got != "acme-coordinator" {
		t.Errorf("custom coordinatorActivity = %q", got)
	}
	if got := custom.stepActivity(); got != "acme-step" {
		t.Errorf("custom stepActivity = %q", got)
	}
}
