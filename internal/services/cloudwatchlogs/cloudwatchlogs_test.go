package cloudwatchlogs

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsLogs "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
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

type pagedClient struct {
	pages    [][]types.LogGroup
	tags     map[string]string
	describe func(context.Context, *awsLogs.DescribeLogGroupsInput, ...func(*awsLogs.Options)) (*awsLogs.DescribeLogGroupsOutput, error)
}

func (c pagedClient) DescribeLogGroups(ctx context.Context, in *awsLogs.DescribeLogGroupsInput, opts ...func(*awsLogs.Options)) (*awsLogs.DescribeLogGroupsOutput, error) {
	return c.describe(ctx, in, opts...)
}
func (c pagedClient) ListTagsForResource(ctx context.Context, in *awsLogs.ListTagsForResourceInput, opts ...func(*awsLogs.Options)) (*awsLogs.ListTagsForResourceOutput, error) {
	return &awsLogs.ListTagsForResourceOutput{Tags: c.tags}, nil
}

func TestCaptureFindsGroupBeyondFirstPageAndUsesAPIARN(t *testing.T) {
	// The exact log group only appears on the second page of a prefix search.
	pages := [][]types.LogGroup{
		{{LogGroupName: aws.String("/app/orders-archive"), RetentionInDays: aws.Int32(7), StoredBytes: aws.Int64(100)}},
		{{LogGroupName: aws.String("/app/orders"), Arn: aws.String("arn:aws:logs:eu-west-1:123456789012:log-group:/app/orders:*"), RetentionInDays: aws.Int32(30), StoredBytes: aws.Int64(500)}},
	}
	pageIdx := 0
	adapter := New(pagedClient{
		pages: pages,
		tags:  map[string]string{"env": "prod"},
		describe: func(ctx context.Context, in *awsLogs.DescribeLogGroupsInput, opts ...func(*awsLogs.Options)) (*awsLogs.DescribeLogGroupsOutput, error) {
			out := &awsLogs.DescribeLogGroupsOutput{LogGroups: pages[pageIdx]}
			if pageIdx < len(pages)-1 {
				out.NextToken = aws.String("next")
			}
			pageIdx++
			return out, nil
		},
	})

	// Configured ARN uses the bare form; the API returns the ":*" suffixed
	// form, which must not fail manifest validation.
	ref := model.ResourceRef{Service: "logs", Type: "log_group", ID: "/app/orders", ARN: "arn:aws:logs:eu-west-1:123456789012:log-group:/app/orders"}
	snapshot, err := adapter.Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{})
	if err != nil {
		t.Fatal(err)
	}
	structure, err := model.DecodeStructure[map[string]any](snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if structure["arn"] != "arn:aws:logs:eu-west-1:123456789012:log-group:/app/orders:*" {
		t.Fatalf("structure arn = %#v, want API-returned ARN", structure["arn"])
	}
	if structure["retention_days"] != float64(30) {
		t.Fatalf("retention_days = %#v", structure["retention_days"])
	}
	if tags := structure["tags"].(map[string]any); tags["env"] != "prod" {
		t.Fatalf("tags = %#v", structure["tags"])
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatalf("manifest must validate despite bare configured ARN vs API :* ARN: %v", err)
	}
}

func TestCaptureFailsClosedWhenGroupMissing(t *testing.T) {
	adapter := New(pagedClient{
		describe: func(context.Context, *awsLogs.DescribeLogGroupsInput, ...func(*awsLogs.Options)) (*awsLogs.DescribeLogGroupsOutput, error) {
			return &awsLogs.DescribeLogGroupsOutput{LogGroups: []types.LogGroup{{LogGroupName: aws.String("/app/other")}}}, nil
		},
	})
	ref := model.ResourceRef{Service: "logs", Type: "log_group", ID: "/app/missing"}
	if _, err := adapter.Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{}); err == nil {
		t.Fatal("capturing a missing log group must fail")
	}
}
