// Package stepfunctions captures Step Functions state-machine topology without
// definitions, executions, or input/output data.
package stepfunctions

import (
	"context"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/services/structureonly"
)

type Client interface {
	DescribeStateMachine(context.Context, *awsSfn.DescribeStateMachineInput, ...func(*awsSfn.Options)) (*awsSfn.DescribeStateMachineOutput, error)
	ListTagsForResource(context.Context, *awsSfn.ListTagsForResourceInput, ...func(*awsSfn.Options)) (*awsSfn.ListTagsForResourceOutput, error)
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
			ServiceName:  "stepfunctions",
			DisplayName:  "Step Functions",
			ResourceType: "state_machine",
			IAMActions:   []string{"states:DescribeStateMachine", "states:ListTagsForResource"},
			Resources: func(project config.Project) []structureonly.Named {
				return structureonly.Select(project.Resources.StateMachines, func(r config.StateMachineResource) (string, string) { return r.Name, r.ARN })
			},
		}),
		client: c,
	}
}

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if err := a.Base.CheckStructureOnly(opts); err != nil {
		return nil, err
	}
	structure := map[string]any{"name": ref.ID, "arn": ref.ARN, "tags": []map[string]string{}}
	if a.client != nil {
		description, err := a.client.DescribeStateMachine(ctx, &awsSfn.DescribeStateMachineInput{StateMachineArn: aws.String(ref.ARN)})
		if err != nil {
			return nil, err
		}
		structure["type"] = string(description.Type)
		structure["role_arn"] = aws.ToString(description.RoleArn)
		if description.LoggingConfiguration != nil {
			structure["logging_level"] = string(description.LoggingConfiguration.Level)
		}
		structure["tracing_enabled"] = description.TracingConfiguration != nil && description.TracingConfiguration.Enabled
		tags, err := a.client.ListTagsForResource(ctx, &awsSfn.ListTagsForResourceInput{ResourceArn: aws.String(ref.ARN)})
		if err != nil {
			return nil, err
		}
		values := make([]map[string]string, 0, len(tags.Tags))
		for _, tag := range tags.Tags {
			values = append(values, map[string]string{"key": aws.ToString(tag.Key), "value": aws.ToString(tag.Value)})
		}
		sort.Slice(values, func(i, j int) bool { return values[i]["key"] < values[j]["key"] })
		structure["tags"] = values
	}
	return a.Base.Snapshot(ref, structure)
}
