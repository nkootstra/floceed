package sns

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSNS "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func TestPlanCaptureAndDiscoverAreMetadataOnly(t *testing.T) {
	adapter := New()
	project := config.Project{Resources: config.Resources{SNS: []config.SNSResource{{Name: "events", ARN: "arn:aws:sns:eu-west-1:123456789012:events"}}}}
	contribution := adapter.Plan(project, true)
	if len(contribution.Selections) != 1 || len(contribution.RequiredIAMActions) != 0 {
		t.Fatalf("contribution = %#v", contribution)
	}
	selection := contribution.Selections[0]
	snapshot, err := adapter.Capture(context.Background(), model.SourceScope{}, selection.Resource, selection.Options)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Data != nil || snapshot.Dataset != nil {
		t.Fatalf("metadata snapshot contains data: %#v", snapshot)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatalf("snapshot should validate: %v", err)
	}
	discovery, err := adapter.Discover(context.Background(), model.SourceScope{})
	if err != nil || len(discovery.Resources) != 0 || len(adapter.Dependencies(snapshot)) != 0 {
		t.Fatalf("discovery/dependencies = %#v, %v", discovery, err)
	}
}

func TestCaptureRejectsData(t *testing.T) {
	_, err := New().Capture(context.Background(), model.SourceScope{}, model.ResourceRef{Service: "sns", Type: "topic", ID: "events", ARN: "arn:aws:sns:eu-west-1:123456789012:events"}, model.CaptureOptions{IncludeData: true})
	if err == nil {
		t.Fatal("expected data capture to be rejected")
	}
}

type subscriptionClient struct{}

func (subscriptionClient) ListSubscriptionsByTopic(context.Context, *awsSNS.ListSubscriptionsByTopicInput, ...func(*awsSNS.Options)) (*awsSNS.ListSubscriptionsByTopicOutput, error) {
	return &awsSNS.ListSubscriptionsByTopicOutput{Subscriptions: []types.Subscription{{Protocol: aws.String("sqs"), Endpoint: aws.String("arn:aws:sqs:eu-west-1:123456789012:events")}}}, nil
}

func TestCapturePreservesSubscriptions(t *testing.T) {
	ref := model.ResourceRef{Service: "sns", Type: "topic", ID: "events", ARN: "arn:aws:sns:eu-west-1:123456789012:events"}
	snapshot, err := New(subscriptionClient{}).Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var structure struct {
		Subscriptions []map[string]string `json:"subscriptions"`
	}
	if err := json.Unmarshal(snapshot.Structure, &structure); err != nil {
		t.Fatal(err)
	}
	if len(structure.Subscriptions) != 1 || structure.Subscriptions[0]["protocol"] != "sqs" {
		t.Fatalf("subscriptions = %#v", structure.Subscriptions)
	}
}
