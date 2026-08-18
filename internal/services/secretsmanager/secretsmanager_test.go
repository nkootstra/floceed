package secretsmanager

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSecrets "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type fakeClient struct {
	describe func(context.Context, *awsSecrets.DescribeSecretInput, ...func(*awsSecrets.Options)) (*awsSecrets.DescribeSecretOutput, error)
}

func (f fakeClient) DescribeSecret(ctx context.Context, in *awsSecrets.DescribeSecretInput, opts ...func(*awsSecrets.Options)) (*awsSecrets.DescribeSecretOutput, error) {
	return f.describe(ctx, in, opts...)
}

func TestPlanSelectsSecretsWithIAMActions(t *testing.T) {
	project := config.Project{Resources: config.Resources{Secrets: []config.SecretResource{{Name: "db", ARN: "arn:aws:secretsmanager:eu-west-1:123456789012:secret:db"}}}}
	contribution := New().Plan(project, true)
	if len(contribution.Selections) != 1 || contribution.Selections[0].Resource.Type != "secret" {
		t.Fatalf("selections = %#v", contribution.Selections)
	}
	if len(contribution.RequiredIAMActions) != 1 || contribution.RequiredIAMActions[0] != "secretsmanager:DescribeSecret" {
		t.Fatalf("IAM actions = %#v", contribution.RequiredIAMActions)
	}
}

func TestCaptureIsStructureOnlyAndNeverReadsValues(t *testing.T) {
	ref := model.ResourceRef{Service: "secretsmanager", Type: "secret", ID: "db", ARN: "arn:aws:secretsmanager:eu-west-1:123456789012:secret:db"}
	adapter := New(fakeClient{describe: func(context.Context, *awsSecrets.DescribeSecretInput, ...func(*awsSecrets.Options)) (*awsSecrets.DescribeSecretOutput, error) {
		return &awsSecrets.DescribeSecretOutput{Description: aws.String("app credentials"), KmsKeyId: aws.String("alias/aws/secretsmanager"), RotationEnabled: aws.Bool(false), LastChangedDate: aws.Time(time.Unix(0, 0).UTC())}, nil
	}})
	snapshot, err := adapter.Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{})
	if err != nil {
		t.Fatal(err)
	}
	structure, err := model.DecodeStructure[map[string]any](snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if structure["value_captured"] != false || structure["description"] != "app credentials" {
		t.Fatalf("structure = %#v", structure)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := New().Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{IncludeData: true}); err == nil {
		t.Fatal("data capture must be rejected")
	}
}
