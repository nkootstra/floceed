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
)

type Client interface {
	DescribeStateMachine(context.Context, *awsSfn.DescribeStateMachineInput, ...func(*awsSfn.Options)) (*awsSfn.DescribeStateMachineOutput, error)
	ListTagsForResource(context.Context, *awsSfn.ListTagsForResourceInput, ...func(*awsSfn.Options)) (*awsSfn.ListTagsForResourceOutput, error)
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
	return model.ServiceDescriptor{Name: "stepfunctions", DisplayName: "Step Functions", Support: model.SupportStructureOnly}
}

func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	out := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.StateMachines))}
	for _, resource := range project.Resources.StateMachines {
		out.Selections = append(out.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "stepfunctions", Type: "state_machine", ID: resource.Name, ARN: resource.ARN}})
		out.RequiredIAMActions = append(out.RequiredIAMActions, "states:DescribeStateMachine", "states:ListTagsForResource")
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
	structure := map[string]any{"name": ref.ID, "arn": ref.ARN, "tags": []map[string]string{}}
	if a.client != nil {
		description, err := a.client.DescribeStateMachine(ctx, &awsSfn.DescribeStateMachineInput{StateMachineArn: aws.String(ref.ARN)})
		if err != nil {
			return nil, err
		}
		structure["type"] = string(description.Type)
		structure["role_arn"] = aws.ToString(description.RoleArn)
		structure["logging_level"] = string(description.LoggingConfiguration.Level)
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
	return model.NewSnapshot(ref, "stepfunctions", structure)
}
