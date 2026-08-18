package eventbridge

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsEvents "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type fakeClient struct{}

func (fakeClient) ListRules(context.Context, *awsEvents.ListRulesInput, ...func(*awsEvents.Options)) (*awsEvents.ListRulesOutput, error) {
	return &awsEvents.ListRulesOutput{Rules: []types.Rule{{
		Name: aws.String("orders"), Arn: aws.String("arn:aws:events:eu-west-1:123456789012:rule/orders"),
		State: types.RuleStateEnabled, EventPattern: aws.String(`{"source":["app"]}`),
	}}}, nil
}
func (fakeClient) ListTargetsByRule(context.Context, *awsEvents.ListTargetsByRuleInput, ...func(*awsEvents.Options)) (*awsEvents.ListTargetsByRuleOutput, error) {
	return &awsEvents.ListTargetsByRuleOutput{Targets: []types.Target{{Id: aws.String("target-1"), Arn: aws.String("arn:aws:sqs:eu-west-1:123456789012:orders")}}}, nil
}

func TestPlanAndCaptureBusTopology(t *testing.T) {
	arn := "arn:aws:events:eu-west-1:123456789012:event-bus/orders"
	project := config.Project{Resources: config.Resources{EventBridge: []config.EventBridgeResource{{Name: "orders", ARN: arn}}}}
	adapter := New(fakeClient{})
	plan := adapter.Plan(project, false)
	if len(plan.Selections) != 1 || plan.Selections[0].Resource.Service != "events" {
		t.Fatalf("plan = %#v", plan)
	}
	snapshot, err := adapter.Capture(context.Background(), model.SourceScope{}, plan.Selections[0].Resource, plan.Selections[0].Options)
	if err != nil {
		t.Fatal(err)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
}
