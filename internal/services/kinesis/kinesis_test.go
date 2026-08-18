package kinesis

import (
	"context"
	"testing"

	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func TestMetadataOnlyStreamCapture(t *testing.T) {
	adapter := New()
	project := config.Project{Resources: config.Resources{Kinesis: []config.KinesisResource{{Name: "events", ARN: "arn:aws:kinesis:eu-west-1:123456789012:stream/events"}}}}
	selection := adapter.Plan(project, true).Selections[0]
	snapshot, err := adapter.Capture(context.Background(), model.SourceScope{}, selection.Resource, selection.Options)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Data != nil || snapshot.Dataset != nil {
		t.Fatalf("metadata snapshot contains data: %#v", snapshot)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureRejectsData(t *testing.T) {
	_, err := New().Capture(context.Background(), model.SourceScope{}, model.ResourceRef{Service: "kinesis", Type: "stream", ID: "events", ARN: "arn:aws:kinesis:eu-west-1:123456789012:stream/events"}, model.CaptureOptions{IncludeData: true})
	if err == nil {
		t.Fatal("expected data capture to be rejected")
	}
}
