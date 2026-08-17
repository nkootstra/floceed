package sqs

import (
	"context"
	"testing"

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

func TestCaptureRejectsData(t *testing.T) {
	_, err := New().Capture(context.Background(), model.SourceScope{}, model.ResourceRef{Service: "sqs", Type: "queue", ID: "jobs", ARN: "arn:aws:sqs:eu-west-1:123456789012:jobs"}, model.CaptureOptions{IncludeData: true})
	if err == nil {
		t.Fatal("expected data capture to be rejected")
	}
}
