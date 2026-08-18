package cloudwatchlogs

import (
	"context"
	"testing"

	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func TestPlanAndCaptureAreStructureOnly(t *testing.T) {
	project := config.Project{Resources: config.Resources{LogGroups: []config.LogGroupResource{{Name: "/app/orders", ARN: "arn:aws:logs:eu-west-1:123456789012:log-group:/app/orders:*"}}}}
	selection := New().Plan(project, true).Selections
	if len(selection) != 1 || selection[0].Resource.Service != "logs" || selection[0].Resource.Type != "log_group" {
		t.Fatalf("selection = %#v", selection)
	}
	snapshot, err := New().Capture(context.Background(), model.SourceScope{}, selection[0].Resource, selection[0].Options)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Data != nil || snapshot.Dataset != nil {
		t.Fatalf("capture contains data: %#v", snapshot)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
}
