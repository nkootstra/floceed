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
	"github.com/nkootstra/floceed/internal/services/structureonly"
)

type Client interface {
	GetFunctionConfiguration(context.Context, *awsLambda.GetFunctionConfigurationInput, ...func(*awsLambda.Options)) (*awsLambda.GetFunctionConfigurationOutput, error)
	ListAliases(context.Context, *awsLambda.ListAliasesInput, ...func(*awsLambda.Options)) (*awsLambda.ListAliasesOutput, error)
	ListEventSourceMappings(context.Context, *awsLambda.ListEventSourceMappingsInput, ...func(*awsLambda.Options)) (*awsLambda.ListEventSourceMappingsOutput, error)
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
			ServiceName:  "lambda",
			DisplayName:  "Lambda",
			ResourceType: "function",
			IAMActions:   []string{"lambda:GetFunctionConfiguration", "lambda:ListAliases", "lambda:ListEventSourceMappings"},
			Resources: func(project config.Project) []structureonly.Named {
				return structureonly.Select(project.Resources.Lambda, func(r config.LambdaResource) (string, string) { return r.Name, r.ARN })
			},
		}),
		client: c,
	}
}

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if err := a.Base.CheckStructureOnly(opts); err != nil {
		return nil, err
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
		aliases, err := a.collectAliases(ctx, ref)
		if err != nil {
			return nil, err
		}
		sort.Slice(aliases, func(i, j int) bool { return aliases[i].Name < aliases[j].Name })
		structure["aliases"] = aliases
		var mappingMarker *string
		for {
			mappings, err := a.client.ListEventSourceMappings(ctx, &awsLambda.ListEventSourceMappingsInput{FunctionName: aws.String(functionName(ref)), Marker: mappingMarker})
			if err != nil {
				return nil, err
			}
			for _, mapping := range mappings.EventSourceMappings {
				structure["event_source_mappings"] = append(structure["event_source_mappings"].([]any), map[string]any{"uuid": aws.ToString(mapping.UUID), "event_source_arn": aws.ToString(mapping.EventSourceArn), "state": aws.ToString(mapping.State), "batch_size": mapping.BatchSize})
			}
			if mappings.NextMarker == nil || *mappings.NextMarker == "" {
				break
			}
			mappingMarker = mappings.NextMarker
		}
	}
	return a.Base.Snapshot(ref, structure)
}

type alias struct {
	Name            string `json:"name"`
	FunctionVersion string `json:"function_version"`
}

// collectAliases paginates ListAliases, which caps at 50 aliases per page.
func (a *Adapter) collectAliases(ctx context.Context, ref model.ResourceRef) ([]alias, error) {
	var out []alias
	var marker *string
	for {
		aliases, err := a.client.ListAliases(ctx, &awsLambda.ListAliasesInput{FunctionName: aws.String(functionName(ref)), Marker: marker})
		if err != nil {
			return nil, err
		}
		for _, value := range aliases.Aliases {
			out = append(out, alias{Name: aws.ToString(value.Name), FunctionVersion: aws.ToString(value.FunctionVersion)})
		}
		if aliases.NextMarker == nil || *aliases.NextMarker == "" {
			return out, nil
		}
		marker = aliases.NextMarker
	}
}

func functionName(r model.ResourceRef) string {
	if r.ID != "" {
		return r.ID
	}
	return r.ARN
}
