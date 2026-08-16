package policy

import (
	"testing"
	"time"

	"github.com/nkootstra/floceed/internal/model"
)

func TestLoadRejectsUnknownFieldsAndCanonicalizes(t *testing.T) {
	one, err := Load([]byte("schema_version: 1\nallowed_accounts: [123456789012]\nmax_age: 24h\n"))
	if err != nil {
		t.Fatal(err)
	}
	two, err := Load([]byte("allowed_accounts: [123456789012]\nmax_age: 24h\nschema_version: 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := one.CanonicalDigest()
	b, _ := two.CanonicalDigest()
	if a != b {
		t.Fatalf("canonical digests differ: %s != %s", a, b)
	}
	if _, err := Load([]byte("schema_version: 1\nunknown: true\n")); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestEvaluateDeniesExpiredAndDisallowedFinding(t *testing.T) {
	policy, err := Load([]byte("schema_version: 1\nallowed_accounts: [123456789012]\nmax_age: 1h\n"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := model.Manifest{Source: model.SourceMetadata{AccountID: "123456789012"}, Findings: []model.Finding{{Code: "DATA_CAPTURE_PARTIAL", Severity: model.SeverityWarning}}}
	decision := policy.Evaluate(Facts{Identity: "sha256:fixture", Manifest: manifest, CapturedAt: time.Unix(0, 0).UTC()}, time.Unix(7200, 0).UTC())
	if decision.Allowed {
		t.Fatal("disallowed fixture admitted")
	}
	if len(decision.Reasons) != 2 || decision.Reasons[0] != "finding_DATA_CAPTURE_PARTIAL" || decision.Reasons[1] != "fixture_expired" {
		t.Fatalf("reasons = %#v", decision.Reasons)
	}
}

func TestEvaluateDoesNotTreatSelfAssertedProvenanceAsProducerBinding(t *testing.T) {
	p, err := Load([]byte("schema_version: 1\nallowed_accounts: [123456789012]\nproducer:\n  repository: nkootstra/floceed\n  workflow: fixture-producer\n"))
	if err != nil {
		t.Fatal(err)
	}
	decision := p.Evaluate(Facts{
		Identity:   "sha256:fixture",
		Manifest:   model.Manifest{Source: model.SourceMetadata{AccountID: "123456789012"}},
		Provenance: &model.Provenance{SchemaVersion: 1, AccountID: "123456789012"},
	}, time.Unix(0, 0).UTC())
	if decision.Allowed || len(decision.Reasons) != 1 || decision.Reasons[0] != "producer_binding_unverified" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestEvaluateRequiresExactTrustedProducerBinding(t *testing.T) {
	p, err := Load([]byte("schema_version: 1\nallowed_accounts: [123456789012]\nproducer:\n  repository: nkootstra/floceed\n  workflow: fixture-producer\n"))
	if err != nil {
		t.Fatal(err)
	}
	facts := Facts{Identity: "sha256:fixture", Manifest: model.Manifest{Source: model.SourceMetadata{AccountID: "123456789012"}}}
	if d := p.Evaluate(facts, time.Unix(0, 0).UTC()); d.Allowed {
		t.Fatal("missing trusted producer accepted")
	}
	facts.TrustedProducer = &ProducerBinding{Repository: "other/repo", Workflow: "fixture-producer"}
	if d := p.Evaluate(facts, time.Unix(0, 0).UTC()); d.Allowed {
		t.Fatal("mismatched trusted producer accepted")
	}
	facts.TrustedProducer = &ProducerBinding{Repository: "nkootstra/floceed", Workflow: "fixture-producer"}
	if d := p.Evaluate(facts, time.Unix(0, 0).UTC()); !d.Allowed {
		t.Fatalf("matching trusted producer rejected: %#v", d)
	}
}

func TestTrustedProducerFromEnvironment(t *testing.T) {
	t.Setenv("FLOCEED_TRUSTED_PRODUCER_REPOSITORY", "nkootstra/floceed")
	t.Setenv("FLOCEED_TRUSTED_PRODUCER_WORKFLOW", "CI")
	got := TrustedProducerFromEnvironment()
	if got == nil || got.Repository != "nkootstra/floceed" || got.Workflow != "CI" {
		t.Fatalf("trusted producer = %#v", got)
	}
	t.Setenv("FLOCEED_TRUSTED_PRODUCER_WORKFLOW", "")
	if TrustedProducerFromEnvironment() != nil {
		t.Fatal("partial trusted producer accepted")
	}
}
