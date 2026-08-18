package apigateway

import (
	"context"
	"testing"

	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func TestPlanAndCaptureAreStructureOnly(t *testing.T) {
	project := config.Project{Resources: config.Resources{APIs: []config.APIResource{{
		Name: "api-1", ARN: "arn:aws:apigateway:eu-west-1::/apis/api-1",
	}}}}
	selection := New().Plan(project, true).Selections
	if len(selection) != 1 || selection[0].Resource.Service != "apigateway" || selection[0].Resource.Type != "api" {
		t.Fatalf("selection = %#v", selection)
	}
	if got := New().Plan(project, true).RequiredIAMActions; len(got) != 1 || got[0] != "apigateway:GET" {
		t.Fatalf("IAM actions = %#v", got)
	}
	snapshot, err := New().Capture(context.Background(), model.SourceScope{}, selection[0].Resource, selection[0].Options)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Data != nil || snapshot.Dataset != nil {
		t.Fatalf("API Gateway capture contains data: %#v", snapshot)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureRejectsDataMode(t *testing.T) {
	ref := model.ResourceRef{Service: "apigateway", Type: "api", ID: "api-1", ARN: "arn:aws:apigateway:eu-west-1::/apis/api-1"}
	if _, err := New().Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{IncludeData: true}); err == nil {
		t.Fatal("expected structure-only capture to reject data mode")
	}
}
