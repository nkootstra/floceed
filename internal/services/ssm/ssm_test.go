package ssm

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSSM "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type fakeClient struct {
	describe func(context.Context, *awsSSM.DescribeParametersInput, ...func(*awsSSM.Options)) (*awsSSM.DescribeParametersOutput, error)
}

func (f fakeClient) DescribeParameters(ctx context.Context, in *awsSSM.DescribeParametersInput, opts ...func(*awsSSM.Options)) (*awsSSM.DescribeParametersOutput, error) {
	return f.describe(ctx, in, opts...)
}

func TestPlanSelectsParametersWithIAMActions(t *testing.T) {
	project := config.Project{Resources: config.Resources{Parameters: []config.ParameterResource{{Name: "/app/key", ARN: "arn:aws:ssm:eu-west-1:123456789012:parameter/app/key"}}}}
	contribution := New().Plan(project, true)
	if len(contribution.Selections) != 1 || contribution.Selections[0].Resource.Type != "parameter" {
		t.Fatalf("selections = %#v", contribution.Selections)
	}
	if len(contribution.RequiredIAMActions) != 1 || contribution.RequiredIAMActions[0] != "ssm:DescribeParameters" {
		t.Fatalf("IAM actions = %#v", contribution.RequiredIAMActions)
	}
}

func TestCaptureIsStructureOnlyAndNeverReadsValues(t *testing.T) {
	ref := model.ResourceRef{Service: "ssm", Type: "parameter", ID: "/app/key", ARN: "arn:aws:ssm:eu-west-1:123456789012:parameter/app/key"}
	adapter := New(fakeClient{describe: func(context.Context, *awsSSM.DescribeParametersInput, ...func(*awsSSM.Options)) (*awsSSM.DescribeParametersOutput, error) {
		return &awsSSM.DescribeParametersOutput{Parameters: []types.ParameterMetadata{{Name: aws.String("/app/key"), Type: types.ParameterTypeString, DataType: aws.String("text"), Version: 3, LastModifiedDate: aws.Time(time.Unix(0, 0).UTC()), Tier: types.ParameterTierStandard}}}, nil
	}})
	snapshot, err := adapter.Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{})
	if err != nil {
		t.Fatal(err)
	}
	structure, err := model.DecodeStructure[map[string]any](snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if structure["type"] != "String" || structure["value_captured"] != false {
		t.Fatalf("structure = %#v", structure)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := New().Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{IncludeData: true}); err == nil {
		t.Fatal("data capture must be rejected")
	}
}

func TestCaptureFailsClosedWhenParameterIsMissing(t *testing.T) {
	ref := model.ResourceRef{Service: "ssm", Type: "parameter", ID: "/missing", ARN: "arn:aws:ssm:eu-west-1:123456789012:parameter/missing"}
	adapter := New(fakeClient{describe: func(context.Context, *awsSSM.DescribeParametersInput, ...func(*awsSSM.Options)) (*awsSSM.DescribeParametersOutput, error) {
		return &awsSSM.DescribeParametersOutput{}, nil
	}})
	if _, err := adapter.Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{}); err == nil {
		t.Fatal("capturing a missing parameter must fail")
	}
}
