// Package ssm captures parameter metadata and never reads parameter values.
package ssm

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsSSM "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type Client interface {
	DescribeParameters(context.Context, *awsSSM.DescribeParametersInput, ...func(*awsSSM.Options)) (*awsSSM.DescribeParametersOutput, error)
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
	return model.ServiceDescriptor{Name: "ssm", DisplayName: "SSM Parameter Store", Support: model.SupportStructureOnly}
}
func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	out := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.Parameters))}
	for _, r := range project.Resources.Parameters {
		out.Selections = append(out.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "ssm", Type: "parameter", ID: r.Name, ARN: r.ARN}})
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
	structure := map[string]any{"name": ref.ID, "arn": ref.ARN, "value_captured": false}
	if a.client != nil {
		out, err := a.client.DescribeParameters(ctx, &awsSSM.DescribeParametersInput{ParameterFilters: []types.ParameterStringFilter{{Key: aws.String("Name"), Values: []string{ref.ID}}}})
		if err != nil {
			return nil, err
		}
		if len(out.Parameters) > 0 {
			parameter := out.Parameters[0]
			structure["type"] = string(parameter.Type)
			structure["data_type"] = aws.ToString(parameter.DataType)
			structure["version"] = parameter.Version
			structure["last_modified_date"] = parameter.LastModifiedDate
			structure["tier"] = string(parameter.Tier)
		}
	}
	return model.NewSnapshot(ref, "ssm", structure)
}
