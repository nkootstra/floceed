package governance

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestNewEffectivePolicyCanonicalizesRuleOrder(t *testing.T) {
	first, err := NewEffectivePolicy("safe", []Rule{
		{ID: "z-rule", Service: ServiceS3, Resource: "assets", Target: Target{Kind: TargetS3Metadata, Path: "X-Team"}, Action: ActionOmit},
		{ID: "a-rule", Service: ServiceDynamoDB, Resource: "orders", Target: Target{Kind: TargetDynamoDBAttribute, Path: "customer.email"}, Action: ActionReplace, Replacement: "hidden"},
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewEffectivePolicy("safe", []Rule{
		{ID: "a-rule", Service: ServiceDynamoDB, Resource: "orders", Target: Target{Kind: TargetDynamoDBAttribute, Path: "customer.email"}, Action: ActionReplace, Replacement: "hidden"},
		{ID: "z-rule", Service: ServiceS3, Resource: "assets", Target: Target{Kind: TargetS3Metadata, Path: "x-team"}, Action: ActionOmit},
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if first.Identity == "" || first.Identity != second.Identity {
		t.Fatalf("identities = %q and %q, want equal non-empty identities", first.Identity, second.Identity)
	}
	if got := first.Rules[0].ID; got != "a-rule" {
		t.Fatalf("first canonical rule = %q, want a-rule", got)
	}
}

func TestNewEffectivePolicySecretRotationChangesIdentityWithoutSerializingSecret(t *testing.T) {
	rules := []Rule{{ID: "rule-001", Service: ServiceDynamoDB, Resource: "orders", Target: Target{Kind: TargetDynamoDBAttribute, Path: "email"}, Action: ActionPseudonymize, KeyID: "key-1"}}
	first, err := NewEffectivePolicy("safe", rules, nil, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewEffectivePolicy("safe", rules, nil, []byte("abcdefghijklmnopqrstuvwxyzABCDEF"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Identity == second.Identity || first.secretVerifier == second.secretVerifier {
		t.Fatal("secret rotation must change both policy identity and verifier")
	}
	payload, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, first.Secret()) || bytes.Contains(payload, []byte(first.secretVerifier)) {
		t.Fatalf("serialized policy leaks secret material: %s", payload)
	}
}

func TestNewEffectivePolicyRejectsAlgorithmsOutsideActionContract(t *testing.T) {
	base := Rule{ID: "rule-001", Service: ServiceDynamoDB, Resource: "orders", Target: Target{Kind: TargetDynamoDBAttribute, Path: "email"}}
	for _, rule := range []Rule{
		func() Rule { r := base; r.Action = ActionOmit; r.Algorithm = HashAlgorithm; return r }(),
		func() Rule { r := base; r.Action = ActionHash; r.Algorithm = PseudonymAlgorithm; return r }(),
		func() Rule { r := base; r.Action = ActionPseudonymize; r.Algorithm = HashAlgorithm; return r }(),
	} {
		if _, err := NewEffectivePolicy("safe", []Rule{rule}, nil, bytes.Repeat([]byte{1}, 32)); err == nil {
			t.Fatalf("accepted invalid rule %#v", rule)
		}
	}
	if _, err := NewEffectivePolicy("safe", nil, []Cohort{{Resource: "orders", KeyID: "key-1", Algorithm: "rank/v2", Limit: 1}}, bytes.Repeat([]byte{1}, 32)); err == nil {
		t.Fatal("accepted invalid cohort algorithm")
	}
}
