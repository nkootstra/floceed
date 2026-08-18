package sqs

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func TestPlanCaptureAndDiscoverAreMetadataOnly(t *testing.T) {
	adapter := New()
	project := config.Project{Resources: config.Resources{SQS: []config.SQSResource{{Name: "jobs.fifo", ARN: "arn:aws:sqs:eu-west-1:123456789012:jobs.fifo"}}}}
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

func TestPlanIncludesDataIAMActionsWhenMessageCaptureEnabled(t *testing.T) {
	adapter := New()
	project := config.Project{Resources: config.Resources{SQS: []config.SQSResource{{Name: "jobs", ARN: "arn:aws:sqs:eu-west-1:123456789012:jobs", Data: &config.SQSDataPolicy{Enabled: true, Mode: config.DataModeBounded}}}}}
	contribution := adapter.Plan(project, false)
	if !slices.Contains(contribution.RequiredIAMActions, "sqs:GetQueueUrl") || !slices.Contains(contribution.RequiredIAMActions, "sqs:ReceiveMessage") {
		t.Fatalf("data-enabled plan must require message actions regardless of run flag: %#v", contribution.RequiredIAMActions)
	}
	if !contribution.Selections[0].Options.IncludeData {
		t.Fatal("data-enabled selection must carry IncludeData")
	}
}

type messageClient struct{}

func (messageClient) GetQueueUrl(context.Context, *awsSQS.GetQueueUrlInput, ...func(*awsSQS.Options)) (*awsSQS.GetQueueUrlOutput, error) {
	return &awsSQS.GetQueueUrlOutput{QueueUrl: aws.String("http://localhost/queue")}, nil
}
func (messageClient) ReceiveMessage(context.Context, *awsSQS.ReceiveMessageInput, ...func(*awsSQS.Options)) (*awsSQS.ReceiveMessageOutput, error) {
	return &awsSQS.ReceiveMessageOutput{Messages: []types.Message{{Body: aws.String("hello")}}}, nil
}

func TestCapturesBoundedMessages(t *testing.T) {
	root := t.TempDir()
	ref := model.ResourceRef{Service: "sqs", Type: "queue", ID: "jobs", ARN: "arn:aws:sqs:eu-west-1:123456789012:jobs"}
	snapshot, err := New(messageClient{}).Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{IncludeData: true, ArtifactDirectory: root, Limits: model.DataLimits{MaxItems: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Dataset == nil || snapshot.Dataset.Records != 1 || snapshot.Dataset.Format != "sqs-messages-ndjson-v1" {
		t.Fatalf("dataset = %#v", snapshot.Dataset)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(snapshot.Dataset.Chunks[0].Data.Path))); err != nil {
		t.Fatal(err)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
}
