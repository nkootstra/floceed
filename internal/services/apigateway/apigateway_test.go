package apigateway

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsAPI "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func TestPlanAndCaptureAreStructureOnly(t *testing.T) {
	project := config.Project{Resources: config.Resources{APIs: []config.APIResource{{
		Name: "api-1", ARN: "arn:aws:apigateway:eu-west-1::/apis/api-1",
	}}}}
	selection := New().Plan(project, true).Selections
	if len(selection) != 1 || selection[0].Resource.Service != "apigateway" || selection[0].Resource.Type != "api" {
		t.Fatalf("selection = %#v", selection)
	}
	if got := New().Plan(project, true).RequiredIAMActions; len(got) != 1 || got[0] != "apigateway:GET" {
		t.Fatalf("IAM actions = %#v", got)
	}
	snapshot, err := New().Capture(context.Background(), model.SourceScope{}, selection[0].Resource, selection[0].Options)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Data != nil || snapshot.Dataset != nil {
		t.Fatalf("API Gateway capture contains data: %#v", snapshot)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureRejectsDataMode(t *testing.T) {
	ref := model.ResourceRef{Service: "apigateway", Type: "api", ID: "api-1", ARN: "arn:aws:apigateway:eu-west-1::/apis/api-1"}
	if _, err := New().Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{IncludeData: true}); err == nil {
		t.Fatal("expected structure-only capture to reject data mode")
	}
}

type pagedClient struct {
	routesPages      []*awsAPI.GetRoutesOutput
	integrationsPage *awsAPI.GetIntegrationsOutput
	routeCalls       int
}

func (p *pagedClient) GetApi(context.Context, *awsAPI.GetApiInput, ...func(*awsAPI.Options)) (*awsAPI.GetApiOutput, error) {
	return &awsAPI.GetApiOutput{ProtocolType: types.ProtocolTypeHttp, Description: aws.String("d"), ApiEndpoint: aws.String("https://api.example.com")}, nil
}
func (p *pagedClient) GetRoutes(_ context.Context, _ *awsAPI.GetRoutesInput, _ ...func(*awsAPI.Options)) (*awsAPI.GetRoutesOutput, error) {
	page := p.routesPages[p.routeCalls]
	p.routeCalls++
	return page, nil
}
func (p *pagedClient) GetIntegrations(context.Context, *awsAPI.GetIntegrationsInput, ...func(*awsAPI.Options)) (*awsAPI.GetIntegrationsOutput, error) {
	return p.integrationsPage, nil
}

func TestCapturePaginatesRoutesAndIntegrations(t *testing.T) {
	client := &pagedClient{
		routesPages: []*awsAPI.GetRoutesOutput{
			{Items: []types.Route{{RouteId: aws.String("r2"), RouteKey: aws.String("POST /b"), Target: aws.String("i2")}}, NextToken: aws.String("p1")},
			{Items: []types.Route{{RouteId: aws.String("r1"), RouteKey: aws.String("GET /a"), Target: aws.String("i1")}}, NextToken: nil},
		},
		integrationsPage: &awsAPI.GetIntegrationsOutput{Items: []types.Integration{
			{IntegrationId: aws.String("i2"), IntegrationType: types.IntegrationTypeAwsProxy, IntegrationUri: aws.String("arn:aws:lambda:eu-west-1:123456789012:function:f"), ConnectionType: types.ConnectionTypeInternet},
			{IntegrationId: aws.String("i1"), IntegrationType: types.IntegrationTypeHttpProxy, IntegrationUri: aws.String("https://upstream.example.com"), ConnectionType: types.ConnectionTypeVpcLink},
		}},
	}

	ref := model.ResourceRef{Service: "apigateway", Type: "api", ID: "api-1", ARN: "arn:aws:apigateway:eu-west-1::/apis/api-1"}
	snapshot, err := New(client).Capture(context.Background(), model.SourceScope{}, ref, model.CaptureOptions{})
	if err != nil {
		t.Fatal(err)
	}
	structure, err := model.DecodeStructure[map[string]any](snapshot)
	if err != nil {
		t.Fatal(err)
	}
	routes := structure["routes"].([]any)
	if len(routes) != 2 {
		t.Fatalf("routes aggregated across pages = %#v, want 2", routes)
	}
	if got := routes[0].(map[string]any)["key"]; got != "GET /a" {
		t.Fatalf("routes not sorted by key: %#v", routes)
	}
	integrations := structure["integrations"].([]any)
	if len(integrations) != 2 {
		t.Fatalf("integrations = %#v, want 2", integrations)
	}
	if got := integrations[0].(map[string]any)["id"]; got != "i1" {
		t.Fatalf("integrations not sorted by id: %#v", integrations)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
}
