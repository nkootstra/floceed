package stepfunctions

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type fakeClient struct {
	describe func(context.Context, *sfn.DescribeStateMachineInput, ...func(*sfn.Options)) (*sfn.DescribeStateMachineOutput, error)
}

func (f fakeClient) DescribeStateMachine(ctx context.Context, in *sfn.DescribeStateMachineInput, opts ...func(*sfn.Options)) (*sfn.DescribeStateMachineOutput, error) {
	return f.describe(ctx, in, opts...)
}

func (f fakeClient) ListTagsForResource(context.Context, *sfn.ListTagsForResourceInput, ...func(*sfn.Options)) (*sfn.ListTagsForResourceOutput, error) {
	return &sfn.ListTagsForResourceOutput{}, nil
}

func TestPlanAndCaptureAreStructureOnly(t *testing.T) {
	project := config.Project{Resources: config.Resources{StateMachines: []config.StateMachineResource{{Name: "orders", ARN: "arn:aws:states:eu-west-1:123456789012:stateMachine:orders"}}}}
	selection := New().Plan(project, true).Selections
	if len(selection) != 1 || selection[0].Resource.Service != "stepfunctions" || selection[0].Resource.Type != "state_machine" {
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

// TestCaptureWithoutLoggingConfiguration guards against a nil-pointer panic:
// LoggingConfiguration is a pointer and a state machine without logging config
// must capture its topology without dereferencing it.
func TestCaptureWithoutLoggingConfiguration(t *testing.T) {
	ref := model.ResourceRef{Service: "stepfunctions", Type: "state_machine", ID: "orders", ARN: "arn:aws:states:eu-west-1:123456789012:stateMachine:orders"}
	adapter := New(fakeClient{describe: func(context.Context, *sfn.DescribeStateMachineInput, ...func(*sfn.Options)) (*sfn.DescribeStateMachineOutput, error) {
		return &sfn.DescribeStateMachineOutput{
			Type: types.StateMachineTypeStandard,
			// LoggingConfiguration intentionally left nil.
		}, nil
	}})
	snapshot, err := adapter.Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{})
	if err != nil {
		t.Fatal(err)
	}
	structure, err := model.DecodeStructure[map[string]any](snapshot)
	if err != nil {
		t.Fatal(err)
	}
	// logging_level must be present and empty (not absent) so every snapshot of
	// this resource type has the same structure shape.
	if got := structure["logging_level"]; got != "" {
		t.Fatalf("logging_level = %#v, want empty string when the state machine has no logging configuration", got)
	}
}
