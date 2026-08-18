// Package apigateway captures API Gateway HTTP/API topology without traffic.
package apigateway

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsAPI "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"sort"
)

type Client interface {
	GetApi(context.Context, *awsAPI.GetApiInput, ...func(*awsAPI.Options)) (*awsAPI.GetApiOutput, error)
	GetRoutes(context.Context, *awsAPI.GetRoutesInput, ...func(*awsAPI.Options)) (*awsAPI.GetRoutesOutput, error)
	GetIntegrations(context.Context, *awsAPI.GetIntegrationsInput, ...func(*awsAPI.Options)) (*awsAPI.GetIntegrationsOutput, error)
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
	return model.ServiceDescriptor{Name: "apigateway", DisplayName: "API Gateway", Support: model.SupportStructureOnly}
}
func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	out := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.APIs))}
	for _, r := range project.Resources.APIs {
		out.Selections = append(out.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "apigateway", Type: "api", ID: r.Name, ARN: r.ARN}})
		out.RequiredIAMActions = append(out.RequiredIAMActions, "apigateway:GET")
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
	structure := map[string]any{"name": ref.ID, "arn": ref.ARN, "routes": []any{}, "integrations": []any{}}
	if a.client != nil {
		api, err := a.client.GetApi(ctx, &awsAPI.GetApiInput{ApiId: aws.String(ref.ID)})
		if err != nil {
			return nil, err
		}
		structure["protocol_type"] = string(api.ProtocolType)
		structure["description"] = aws.ToString(api.Description)
		structure["api_endpoint"] = aws.ToString(api.ApiEndpoint)
		routes, err := a.client.GetRoutes(ctx, &awsAPI.GetRoutesInput{ApiId: aws.String(ref.ID)})
		if err != nil {
			return nil, err
		}
		for _, route := range routes.Items {
			structure["routes"] = append(structure["routes"].([]any), map[string]string{"id": aws.ToString(route.RouteId), "key": aws.ToString(route.RouteKey), "target": aws.ToString(route.Target)})
		}
		integrations, err := a.client.GetIntegrations(ctx, &awsAPI.GetIntegrationsInput{ApiId: aws.String(ref.ID)})
		if err != nil {
			return nil, err
		}
		for _, integration := range integrations.Items {
			structure["integrations"] = append(structure["integrations"].([]any), map[string]string{"id": aws.ToString(integration.IntegrationId), "type": string(integration.IntegrationType), "uri": aws.ToString(integration.IntegrationUri), "connection_type": string(integration.ConnectionType)})
		}
		sort.Slice(structure["routes"], func(i, j int) bool {
			return structure["routes"].([]any)[i].(map[string]string)["key"] < structure["routes"].([]any)[j].(map[string]string)["key"]
		})
		sort.Slice(structure["integrations"], func(i, j int) bool {
			return structure["integrations"].([]any)[i].(map[string]string)["id"] < structure["integrations"].([]any)[j].(map[string]string)["id"]
		})
	}
	return model.NewSnapshot(ref, "apigateway", structure)
}
