// Package apigateway captures API Gateway HTTP/API topology without traffic.
package apigateway

import (
	"context"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsAPI "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/services/structureonly"
)

type Client interface {
	GetApi(context.Context, *awsAPI.GetApiInput, ...func(*awsAPI.Options)) (*awsAPI.GetApiOutput, error)
	GetRoutes(context.Context, *awsAPI.GetRoutesInput, ...func(*awsAPI.Options)) (*awsAPI.GetRoutesOutput, error)
	GetIntegrations(context.Context, *awsAPI.GetIntegrationsInput, ...func(*awsAPI.Options)) (*awsAPI.GetIntegrationsOutput, error)
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
			ServiceName:  "apigateway",
			DisplayName:  "API Gateway",
			ResourceType: "api",
			IAMActions:   []string{"apigateway:GET"},
			Resources: func(project config.Project) []structureonly.Named {
				return structureonly.Select(project.Resources.APIs, func(r config.APIResource) (string, string) { return r.Name, r.ARN })
			},
		}),
		client: c,
	}
}

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if err := a.Base.CheckStructureOnly(opts); err != nil {
		return nil, err
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
		routes, err := a.collectRoutes(ctx, ref.ID)
		if err != nil {
			return nil, err
		}
		sort.Slice(routes, func(i, j int) bool { return routes[i].Key < routes[j].Key })
		structure["routes"] = routes
		integrations, err := a.collectIntegrations(ctx, ref.ID)
		if err != nil {
			return nil, err
		}
		sort.Slice(integrations, func(i, j int) bool { return integrations[i].ID < integrations[j].ID })
		structure["integrations"] = integrations
	}
	return a.Base.Snapshot(ref, structure)
}

type route struct {
	ID     string `json:"id"`
	Key    string `json:"key"`
	Target string `json:"target"`
}

type integration struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	URI            string `json:"uri"`
	ConnectionType string `json:"connection_type"`
}

// collectRoutes and collectIntegrations are paginated: GetRoutes and
// GetIntegrations cap at 50 items per page, so a bare call silently truncates
// larger APIs.
func (a *Adapter) collectRoutes(ctx context.Context, apiID string) ([]route, error) {
	var out []route
	var token *string
	for {
		page, err := a.client.GetRoutes(ctx, &awsAPI.GetRoutesInput{ApiId: aws.String(apiID), NextToken: token})
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			out = append(out, route{ID: aws.ToString(item.RouteId), Key: aws.ToString(item.RouteKey), Target: aws.ToString(item.Target)})
		}
		if page.NextToken == nil || *page.NextToken == "" {
			return out, nil
		}
		token = page.NextToken
	}
}

func (a *Adapter) collectIntegrations(ctx context.Context, apiID string) ([]integration, error) {
	var out []integration
	var token *string
	for {
		page, err := a.client.GetIntegrations(ctx, &awsAPI.GetIntegrationsInput{ApiId: aws.String(apiID), NextToken: token})
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			out = append(out, integration{ID: aws.ToString(item.IntegrationId), Type: string(item.IntegrationType), URI: aws.ToString(item.IntegrationUri), ConnectionType: string(item.ConnectionType)})
		}
		if page.NextToken == nil || *page.NextToken == "" {
			return out, nil
		}
		token = page.NextToken
	}
}
