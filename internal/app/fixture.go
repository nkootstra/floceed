package app

import (
	"context"
	"os"
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/policy"
)

// VerifyFixture is an AWS-free application boundary for local fixture
// verification. It deliberately does not use Application.Factory.
func (a *Application) VerifyFixture(ctx context.Context, input string) (model.VerificationResult, error) {
	if err := ctx.Err(); err != nil {
		return model.VerificationResult{}, err
	}
	return bundle.VerifyFixture(input)
}

func (a *Application) AdmitFixture(ctx context.Context, input, policyPath string) (policy.Decision, error) {
	result, err := a.VerifyFixture(ctx, input)
	if err != nil {
		return policy.Decision{}, err
	}
	policyBytes, err := os.ReadFile(policyPath)
	if err != nil {
		return policy.Decision{}, err
	}
	admission, err := policy.Load(policyBytes)
	if err != nil {
		return policy.Decision{}, err
	}
	generated, err := bundle.LoadGenerated(ctx, input)
	if err != nil {
		return policy.Decision{}, err
	}
	now := time.Now()
	if a.Now != nil {
		now = a.Now()
	}
	return admission.Evaluate(policy.Facts{Identity: result.Identity, Manifest: generated.Manifest, CapturedAt: generated.Manifest.Capture.CapturedAt, Provenance: result.Provenance, TrustedProducer: policy.TrustedProducerFromEnvironment()}, now), nil
}
