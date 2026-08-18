// Package ssm captures parameter metadata and never reads parameter values.
package ssm

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSSM "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/services/structureonly"
)

type Client interface {
	DescribeParameters(context.Context, *awsSSM.DescribeParametersInput, ...func(*awsSSM.Options)) (*awsSSM.DescribeParametersOutput, error)
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
			ServiceName:  "ssm",
			DisplayName:  "SSM Parameter Store",
			ResourceType: "parameter",
			IAMActions:   []string{"ssm:DescribeParameters"},
			Resources: func(project config.Project) []structureonly.Named {
				return structureonly.Select(project.Resources.Parameters, func(r config.ParameterResource) (string, string) { return r.Name, r.ARN })
			},
		}),
		client: c,
	}
}

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if err := a.Base.CheckStructureOnly(opts); err != nil {
		return nil, err
	}
	structure := map[string]any{"name": ref.ID, "arn": ref.ARN, "value_captured": false}
	if a.client != nil {
		out, err := a.client.DescribeParameters(ctx, &awsSSM.DescribeParametersInput{ParameterFilters: []types.ParameterStringFilter{{Key: aws.String("Name"), Values: []string{ref.ID}}}})
		if err != nil {
			return nil, err
		}
		if len(out.Parameters) == 0 {
			return nil, fmt.Errorf("SSM parameter %q was not found", ref.ID)
		}
		parameter := out.Parameters[0]
		structure["type"] = string(parameter.Type)
		structure["data_type"] = aws.ToString(parameter.DataType)
		structure["version"] = parameter.Version
		structure["last_modified_date"] = parameter.LastModifiedDate
		structure["tier"] = string(parameter.Tier)
	}
	return a.Base.Snapshot(ref, structure)
}
