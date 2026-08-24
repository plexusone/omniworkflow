package stepfunctions_test

import (
	"context"
	"os"
	"testing"

	"github.com/plexusone/omniworkflow/conformance"
	"github.com/plexusone/omniworkflow/stepfunctions"
)

// TestStepFunctionsConformance runs the shared conformance suite against live
// AWS Step Functions. It is skipped unless OMNIWORKFLOW_SFN_ROLE_ARN is set
// (the IAM role the interpreter state machine assumes); standard AWS
// credential/region resolution supplies the rest. This keeps `go test ./...`
// hermetic while making the full suite runnable in CI or locally with creds:
//
//	OMNIWORKFLOW_SFN_ROLE_ARN=arn:aws:iam::…:role/… AWS_REGION=us-east-1 \
//	    go test ./stepfunctions/ -run Conformance -v
func TestStepFunctionsConformance(t *testing.T) {
	roleARN := os.Getenv("OMNIWORKFLOW_SFN_ROLE_ARN")
	if roleARN == "" {
		t.Skip("set OMNIWORKFLOW_SFN_ROLE_ARN (and AWS creds/region) to run the live Step Functions conformance suite")
	}

	ctx := context.Background()
	provider, err := stepfunctions.NewFromConfig(ctx, stepfunctions.ProvisionConfig{
		Region:     os.Getenv("AWS_REGION"),
		RoleARN:    roleARN,
		NamePrefix: "omniworkflow-conftest",
	}, stepfunctions.Options{WorkerName: "conformance"})
	if err != nil {
		t.Fatalf("provision Step Functions provider: %v", err)
	}

	conformance.Run(t, conformance.Config{
		Provider:  provider,
		NewWorker: provider.NewWorker,
	})
}
