// Package cloudwatchlogs captures CloudWatch Logs group topology without log events.
package cloudwatchlogs

import (
	"context"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsLogs "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/services/structureonly"
)

type Client interface {
	DescribeLogGroups(context.Context, *awsLogs.DescribeLogGroupsInput, ...func(*awsLogs.Options)) (*awsLogs.DescribeLogGroupsOutput, error)
	ListTagsForResource(context.Context, *awsLogs.ListTagsForResourceInput, ...func(*awsLogs.Options)) (*awsLogs.ListTagsForResourceOutput, error)
}

type Adapter struct {
	structureonly.Base
	client Client
}

var _ catalog.Adapter = (*Adapter)(nil)

func New(client ...Client) *Adapter {
	var c Client
	if len(client) > 0 {
		c = client[0]
	}
	return &Adapter{
		Base: structureonly.New(structureonly.Descriptor{
			ServiceName:  "logs",
			DisplayName:  "CloudWatch Logs",
			ResourceType: "log_group",
			IAMActions:   []string{"logs:DescribeLogGroups", "logs:ListTagsForResource"},
			Resources: func(project config.Project) []structureonly.Named {
				return structureonly.Select(project.Resources.LogGroups, func(r config.LogGroupResource) (string, string) { return r.Name, r.ARN })
			},
		}),
		client: c,
	}
}

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if err := a.Base.CheckStructureOnly(opts); err != nil {
		return nil, err
	}
	structure := map[string]any{"name": ref.ID, "arn": ref.ARN, "tags": map[string]string{}}
	if a.client != nil {
		group, err := a.findGroup(ctx, ref.ID)
		if err != nil {
			return nil, err
		}
		structure["retention_days"] = group.RetentionInDays
		structure["stored_bytes"] = group.StoredBytes
		structure["log_group_class"] = string(group.LogGroupClass)
		if group.Arn != nil {
			structure["arn"] = aws.ToString(group.Arn)
		}
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
	return a.Base.Snapshot(ref, structure)
}

// findGroup returns the exact log group by name. DescribeLogGroups pages at
// 50 groups, so a prefix search must be paginated or an exact match beyond the
// first page is silently missed.
func (a *Adapter) findGroup(ctx context.Context, name string) (*types.LogGroup, error) {
	paginator := awsLogs.NewDescribeLogGroupsPaginator(a.client, &awsLogs.DescribeLogGroupsInput{LogGroupNamePrefix: aws.String(name)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for i := range page.LogGroups {
			if aws.ToString(page.LogGroups[i].LogGroupName) == name {
				return &page.LogGroups[i], nil
			}
		}
	}
	return nil, model.ErrValidation
}
