// Package lambda captures Lambda function configuration without executable code.
package lambda

import (
	"context"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsLambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type Client interface {
	GetFunctionConfiguration(context.Context, *awsLambda.GetFunctionConfigurationInput, ...func(*awsLambda.Options)) (*awsLambda.GetFunctionConfigurationOutput, error)
	ListAliases(context.Context, *awsLambda.ListAliasesInput, ...func(*awsLambda.Options)) (*awsLambda.ListAliasesOutput, error)
	ListEventSourceMappings(context.Context, *awsLambda.ListEventSourceMappingsInput, ...func(*awsLambda.Options)) (*awsLambda.ListEventSourceMappingsOutput, error)
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
	return model.ServiceDescriptor{Name: "lambda", DisplayName: "Lambda", Support: model.SupportStructureOnly}
}
func (*Adapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}
func (*Adapter) Dependencies(*model.Snapshot) []model.Dependency              { return nil }
func (*Adapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }
func (*Adapter) Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error) {
	return model.DiscoveryResult{}, nil
}
func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	out := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.Lambda))}
	for _, resource := range project.Resources.Lambda {
		out.Selections = append(out.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "lambda", Type: "function", ID: resource.Name, ARN: resource.ARN}})
	}
	return out
}
func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if opts.IncludeData {
		return nil, model.ErrValidation
	}
	structure := map[string]any{"name": ref.ID, "arn": ref.ARN, "aliases": []any{}, "event_source_mappings": []any{}}
	if a.client != nil {
		configuration, err := a.client.GetFunctionConfiguration(ctx, &awsLambda.GetFunctionConfigurationInput{FunctionName: aws.String(functionName(ref))})
		if err != nil {
			return nil, err
		}
		structure["runtime"] = string(configuration.Runtime)
		structure["handler"] = aws.ToString(configuration.Handler)
		structure["timeout"] = configuration.Timeout
		structure["memory_size"] = configuration.MemorySize
		structure["architectures"] = configuration.Architectures
		aliases, err := a.client.ListAliases(ctx, &awsLambda.ListAliasesInput{FunctionName: aws.String(functionName(ref))})
		if err != nil {
			return nil, err
		}
		for _, alias := range aliases.Aliases {
			structure["aliases"] = append(structure["aliases"].([]any), map[string]string{"name": aws.ToString(alias.Name), "function_version": aws.ToString(alias.FunctionVersion)})
		}
		mappings, err := a.client.ListEventSourceMappings(ctx, &awsLambda.ListEventSourceMappingsInput{FunctionName: aws.String(functionName(ref))})
		if err != nil {
			return nil, err
		}
		for _, mapping := range mappings.EventSourceMappings {
			structure["event_source_mappings"] = append(structure["event_source_mappings"].([]any), map[string]any{"uuid": aws.ToString(mapping.UUID), "event_source_arn": aws.ToString(mapping.EventSourceArn), "state": aws.ToString(mapping.State), "batch_size": mapping.BatchSize})
		}
		sort.Slice(structure["aliases"], func(i, j int) bool {
			return structure["aliases"].([]any)[i].(map[string]string)["name"] < structure["aliases"].([]any)[j].(map[string]string)["name"]
		})
	}
	return model.NewSnapshot(ref, "lambda", structure)
}

func functionName(r model.ResourceRef) string {
	if r.ID != "" {
		return r.ID
	}
	return r.ARN
}
