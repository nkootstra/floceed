package sns

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSNS "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func TestPlanCaptureAndDiscoverAreMetadataOnly(t *testing.T) {
	adapter := New(nil)
	project := config.Project{Resources: config.Resources{SNS: []config.SNSResource{{Name: "events", ARN: "arn:aws:sns:eu-west-1:123456789012:events"}}}}
	contribution := adapter.Plan(project, true)
	if len(contribution.Selections) != 1 || !slices.Contains(contribution.RequiredIAMActions, "sns:ListSubscriptionsByTopic") {
		t.Fatalf("contribution = %#v", contribution)
	}
	selection := contribution.Selections[0]
	if _, err := adapter.Capture(context.Background(), model.SourceScope{}, selection.Resource, selection.Options); err == nil {
		t.Fatal("expected clientless capture to fail")
	}
	discovery, err := adapter.Discover(context.Background(), model.SourceScope{})
	if err != nil || len(discovery.Resources) != 0 || len(adapter.Dependencies(nil)) != 0 {
		t.Fatalf("discovery/dependencies = %#v, %v", discovery, err)
	}
}

func TestCaptureRejectsData(t *testing.T) {
	_, err := New(nil).Capture(context.Background(), model.SourceScope{}, model.ResourceRef{Service: "sns", Type: "topic", ID: "events", ARN: "arn:aws:sns:eu-west-1:123456789012:events"}, model.CaptureOptions{IncludeData: true})
	if err == nil {
		t.Fatal("expected data capture to be rejected")
	}
}

type subscriptionClient struct{}

type emptySubscriptionClient struct{}

func (subscriptionClient) ListSubscriptionsByTopic(context.Context, *awsSNS.ListSubscriptionsByTopicInput, ...func(*awsSNS.Options)) (*awsSNS.ListSubscriptionsByTopicOutput, error) {
	return &awsSNS.ListSubscriptionsByTopicOutput{Subscriptions: []types.Subscription{{Protocol: aws.String("sqs"), Endpoint: aws.String("arn:aws:sqs:eu-west-1:123456789012:events")}}}, nil
}

func (emptySubscriptionClient) ListSubscriptionsByTopic(context.Context, *awsSNS.ListSubscriptionsByTopicInput, ...func(*awsSNS.Options)) (*awsSNS.ListSubscriptionsByTopicOutput, error) {
	return &awsSNS.ListSubscriptionsByTopicOutput{}, nil
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

func TestCaptureEmptySubscriptionsUsesArray(t *testing.T) {
	ref := model.ResourceRef{Service: "sns", Type: "topic", ID: "events", ARN: "arn:aws:sns:eu-west-1:123456789012:events"}
	snapshot, err := New(emptySubscriptionClient{}).Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var structure struct {
		Subscriptions json.RawMessage `json:"subscriptions"`
	}
	if err := json.Unmarshal(snapshot.Structure, &structure); err != nil {
		t.Fatal(err)
	}
	if string(structure.Subscriptions) != "[]" {
		t.Fatalf("empty subscriptions = %s, want []", structure.Subscriptions)
	}
}
