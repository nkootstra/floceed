package testfixture

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"
)

func TestRepresentativeAppIAMPolicyIsDeterministic(t *testing.T) {
	first := RepresentativeAppIAMPolicy("floceed-integration-bucket")
	second := RepresentativeAppIAMPolicy("floceed-integration-bucket")
	if !bytes.Equal(first, second) {
		t.Fatalf("policy generation is not deterministic:\n%s\n%s", first, second)
	}
}

func TestRepresentativeServiceDiscoveryIAMActionsReturnsCopy(t *testing.T) {
	actions := RepresentativeServiceDiscoveryIAMActions()
	actions[0] = "mutated:Action"
	if slices.Contains(RepresentativeServiceDiscoveryIAMActions(), "mutated:Action") {
		t.Fatal("caller mutation changed representative service discovery actions")
	}
}

func TestRepresentativeAppIAMPolicyGrantsReadsOnly(t *testing.T) {
	data := RepresentativeAppIAMPolicy("floceed-integration-bucket")
	var document iamPolicyDocument
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("policy is not valid JSON: %v", err)
	}
	if document.Version != "2012-10-17" {
		t.Fatalf("policy version = %q, want 2012-10-17", document.Version)
	}
	var actions []string
	for _, statement := range document.Statement {
		if statement.Effect != "Allow" {
			t.Fatalf("statement %s effect = %q, want Allow", statement.Sid, statement.Effect)
		}
		actions = append(actions, statement.Action...)
	}
	want := []string{
		"apigateway:GET",
		"dynamodb:DescribeTable",
		"dynamodb:GetItem",
		"dynamodb:ListTables",
		"events:ListEventBuses",
		"kinesis:ListStreams",
		"lambda:ListFunctions",
		"logs:DescribeLogGroups",
		"s3:GetObject",
		"s3:ListAllMyBuckets",
		"secretsmanager:ListSecrets",
		"sns:ListTopics",
		"sqs:ListQueues",
		"ssm:DescribeParameters",
		"states:ListStateMachines",
	}
	for _, action := range want {
		if !slices.Contains(actions, action) {
			t.Fatalf("policy is missing allowed action %q: %v", action, actions)
		}
	}
	for _, denied := range []string{"dynamodb:Scan", "dynamodb:PutItem", "s3:PutObject", "s3:DeleteObject"} {
		if slices.Contains(actions, denied) {
			t.Fatalf("policy unexpectedly grants %q: %v", denied, actions)
		}
	}
	tableARN := "arn:aws:dynamodb:eu-west-1:123456789012:table/*"
	if document.Statement[0].Resource != tableARN {
		t.Fatalf("table resource = %q, want %q", document.Statement[0].Resource, tableARN)
	}
	bucketARN := "arn:aws:s3:::floceed-integration-bucket/*"
	if document.Statement[1].Resource != bucketARN {
		t.Fatalf("bucket resource = %q, want %q", document.Statement[1].Resource, bucketARN)
	}
}
