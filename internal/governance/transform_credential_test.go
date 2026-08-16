package governance_test

import (
	"testing"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/governance"
)

func TestReplacementRemainsSubjectToCredentialDetection(t *testing.T) {
	rule := governance.Rule{ID: "rule-001", Service: governance.ServiceDynamoDB, Resource: "orders", Target: governance.Target{Kind: governance.TargetDynamoDBAttribute, Path: "email"}, Action: governance.ActionReplace, Replacement: "AKIA1234567890123456"}
	result, err := governance.NewEngine("safe", nil).Apply(rule, []byte("source value"))
	if err != nil {
		t.Fatal(err)
	}

	detector := bundle.NewCredentialDetector()
	if _, err := detector.Write(result.Value); err != nil {
		t.Fatal(err)
	}
	if detector.Err() == nil {
		t.Fatal("credential-bearing replacement passed credential detection")
	}
}
