// Package cloudwatchlogs captures CloudWatch Logs group topology without log events.
package cloudwatchlogs

import (
	"context"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsLogs "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type Client interface {
	DescribeLogGroups(context.Context, *awsLogs.DescribeLogGroupsInput, ...func(*awsLogs.Options)) (*awsLogs.DescribeLogGroupsOutput, error)
	ListTagsForResource(context.Context, *awsLogs.ListTagsForResourceInput, ...func(*awsLogs.Options)) (*awsLogs.ListTagsForResourceOutput, error)
}

type Adapter struct{ client Client }

func New(client ...Client) *Adapter {
	var c Client
	if len(client) > 0 {
		c = client[0]
	}
	return &Adapter{client: c}
}

func (*Adapter) Service() model.ServiceDescriptor {
	return model.ServiceDescriptor{Name: "logs", DisplayName: "CloudWatch Logs", Support: model.SupportStructureOnly}
}

func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	out := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.LogGroups))}
	for _, resource := range project.Resources.LogGroups {
		out.Selections = append(out.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "logs", Type: "log_group", ID: resource.Name, ARN: resource.ARN}})
		out.RequiredIAMActions = append(out.RequiredIAMActions, "logs:DescribeLogGroups", "logs:ListTagsForResource")
	}
	return out
}

func (*Adapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}
func (*Adapter) Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error) {
	return model.DiscoveryResult{}, nil
}
func (*Adapter) Dependencies(*model.Snapshot) []model.Dependency              { return nil }
func (*Adapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if opts.IncludeData {
		return nil, model.ErrValidation
	}
	structure := map[string]any{"name": ref.ID, "arn": ref.ARN, "tags": map[string]string{}}
	if a.client != nil {
		groups, err := a.client.DescribeLogGroups(ctx, &awsLogs.DescribeLogGroupsInput{LogGroupNamePrefix: aws.String(ref.ID)})
		if err != nil {
			return nil, err
		}
		var group *struct {
			Arn             *string
			RetentionInDays *int32
			StoredBytes     *int64
			LogGroupClass   string
		}
		for i := range groups.LogGroups {
			candidate := &groups.LogGroups[i]
			if aws.ToString(candidate.LogGroupName) == ref.ID {
				group = &struct {
					Arn             *string
					RetentionInDays *int32
					StoredBytes     *int64
					LogGroupClass   string
				}{candidate.Arn, candidate.RetentionInDays, candidate.StoredBytes, string(candidate.LogGroupClass)}
				break
			}
		}
		if group == nil {
			return nil, model.ErrValidation
		}
		structure["retention_days"] = group.RetentionInDays
		structure["stored_bytes"] = group.StoredBytes
		structure["log_group_class"] = group.LogGroupClass
		tags, err := a.client.ListTagsForResource(ctx, &awsLogs.ListTagsForResourceInput{ResourceArn: group.Arn})
		if err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(tags.Tags))
		for key := range tags.Tags {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		ordered := make(map[string]string, len(keys))
		for _, key := range keys {
			ordered[key] = tags.Tags[key]
		}
		structure["tags"] = ordered
	}
	return model.NewSnapshot(ref, "logs", structure)
}
