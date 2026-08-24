package stepfunctions

import (
	"context"
	"errors"
	"strings"

	"github.com/grokify/awsgo/sfnutil"
)

// DefaultNamePrefix is used when ProvisionConfig.NamePrefix is empty. Set a
// distinct prefix per deployment or tenant so their interpreter state machines
// and activities do not collide.
const DefaultNamePrefix = "omniworkflow"

// ProvisionConfig configures the one-time creation of the interpreter state
// machine and its activities.
type ProvisionConfig struct {
	// Region is the AWS region; empty uses the SDK's default resolution.
	Region string
	// RoleARN is the IAM role the state machine assumes to run its Map/Task
	// states. Required.
	RoleARN string
	// NamePrefix prefixes the created resource names. Empty uses
	// DefaultNamePrefix.
	NamePrefix string
}

// Names returns the resource names derived from the prefix.
func (c ProvisionConfig) prefix() string {
	if p := strings.TrimSpace(c.NamePrefix); p != "" {
		return p
	}
	return DefaultNamePrefix
}

func (c ProvisionConfig) stateMachineName() string    { return c.prefix() + "-interpreter" }
func (c ProvisionConfig) coordinatorActivity() string { return c.prefix() + "-coordinator" }
func (c ProvisionConfig) stepActivity() string        { return c.prefix() + "-step" }

// Provisioned holds the ARNs of the resources ensured by Provision.
type Provisioned struct {
	StateMachineARN        string
	CoordinatorActivityARN string
	StepActivityARN        string
}

// ErrNoRoleARN is returned when ProvisionConfig.RoleARN is empty.
var ErrNoRoleARN = errors.New("stepfunctions: ProvisionConfig.RoleARN is required")

// Provision ensures the interpreter state machine and its two activities exist,
// creating them if absent and updating the state machine definition if present.
// It is idempotent: activities are create-idempotent in SFN, and the state
// machine is matched by name and updated in place. It returns the resource ARNs
// the provider's worker and control-plane methods need.
func Provision(ctx context.Context, cfg ProvisionConfig) (*Provisioned, error) {
	if strings.TrimSpace(cfg.RoleARN) == "" {
		return nil, ErrNoRoleARN
	}
	client, err := sfnutil.NewClientDefault(ctx, cfg.Region)
	if err != nil {
		return nil, err
	}
	return ProvisionWithClient(ctx, client, cfg)
}

// ProvisionWithClient is Provision with a caller-supplied sfnutil client, for
// reuse or testing against a local Step Functions endpoint.
func ProvisionWithClient(ctx context.Context, client *sfnutil.Client, cfg ProvisionConfig) (*Provisioned, error) {
	if strings.TrimSpace(cfg.RoleARN) == "" {
		return nil, ErrNoRoleARN
	}

	coordinatorARN, err := client.CreateActivity(ctx, cfg.coordinatorActivity())
	if err != nil {
		return nil, err
	}
	stepARN, err := client.CreateActivity(ctx, cfg.stepActivity())
	if err != nil {
		return nil, err
	}

	asl, err := RenderInterpreterASL(coordinatorARN, stepARN)
	if err != nil {
		return nil, err
	}

	smName := cfg.stateMachineName()
	smARN, found, err := client.FindStateMachineARN(ctx, smName)
	if err != nil {
		return nil, err
	}
	if found {
		if err := client.UpdateStateMachineDefinition(ctx, smARN, asl, cfg.RoleARN); err != nil {
			return nil, err
		}
	} else {
		smARN, err = client.CreateStandardStateMachine(ctx, smName, asl, cfg.RoleARN)
		if err != nil {
			return nil, err
		}
	}

	return &Provisioned{
		StateMachineARN:        smARN,
		CoordinatorActivityARN: coordinatorARN,
		StepActivityARN:        stepARN,
	}, nil
}
