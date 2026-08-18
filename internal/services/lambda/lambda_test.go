package lambda

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsLambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type fakeClient struct {
	config func(context.Context, *awsLambda.GetFunctionConfigurationInput, ...func(*awsLambda.Options)) (*awsLambda.GetFunctionConfigurationOutput, error)
}

func (f fakeClient) GetFunctionConfiguration(ctx context.Context, in *awsLambda.GetFunctionConfigurationInput, opts ...func(*awsLambda.Options)) (*awsLambda.GetFunctionConfigurationOutput, error) {
	return f.config(ctx, in, opts...)
}

func (f fakeClient) ListAliases(context.Context, *awsLambda.ListAliasesInput, ...func(*awsLambda.Options)) (*awsLambda.ListAliasesOutput, error) {
	return &awsLambda.ListAliasesOutput{Aliases: []types.AliasConfiguration{{Name: aws.String("live"), FunctionVersion: aws.String("1")}}}, nil
}

func (f fakeClient) ListEventSourceMappings(context.Context, *awsLambda.ListEventSourceMappingsInput, ...func(*awsLambda.Options)) (*awsLambda.ListEventSourceMappingsOutput, error) {
	return &awsLambda.ListEventSourceMappingsOutput{EventSourceMappings: []types.EventSourceMappingConfiguration{{UUID: aws.String("abc"), EventSourceArn: aws.String("arn:aws:sqs:eu-west-1:123456789012:jobs"), State: aws.String("Enabled"), BatchSize: aws.Int32(5)}}}, nil
}

func TestPlanSelectsFunctionsWithIAMActions(t *testing.T) {
	project := config.Project{Resources: config.Resources{Lambda: []config.LambdaResource{{Name: "worker", ARN: "arn:aws:lambda:eu-west-1:123456789012:function:worker"}}}}
	contribution := New().Plan(project, true)
	if len(contribution.Selections) != 1 || contribution.Selections[0].Resource.Type != "function" {
		t.Fatalf("selections = %#v", contribution.Selections)
	}
	want := map[string]bool{"lambda:GetFunctionConfiguration": true, "lambda:ListAliases": true, "lambda:ListEventSourceMappings": true}
	for _, action := range contribution.RequiredIAMActions {
		if want[action] {
			delete(want, action)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing IAM actions: %#v", want)
	}
}

func TestCaptureIsStructureOnlyAndBuildsSnapshot(t *testing.T) {
	ref := model.ResourceRef{Service: "lambda", Type: "function", ID: "worker", ARN: "arn:aws:lambda:eu-west-1:123456789012:function:worker"}
	adapter := New(fakeClient{config: func(context.Context, *awsLambda.GetFunctionConfigurationInput, ...func(*awsLambda.Options)) (*awsLambda.GetFunctionConfigurationOutput, error) {
		return &awsLambda.GetFunctionConfigurationOutput{Runtime: types.RuntimePython39, Handler: aws.String("index.handler"), Timeout: aws.Int32(30), MemorySize: aws.Int32(128), Architectures: []types.Architecture{types.ArchitectureArm64}}, nil
	}})
	snapshot, err := adapter.Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{})
	if err != nil {
		t.Fatal(err)
	}
	structure, err := model.DecodeStructure[map[string]any](snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if structure["handler"] != "index.handler" {
		t.Fatalf("handler = %#v", structure["handler"])
	}
	if len(structure["aliases"].([]any)) != 1 {
		t.Fatalf("aliases = %#v", structure["aliases"])
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := New().Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{IncludeData: true}); err == nil {
		t.Fatal("data capture must be rejected")
	}
}
