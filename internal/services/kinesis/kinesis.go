// Package kinesis implements metadata-only capture of explicitly selected streams.
package kinesis

import (
	"context"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsKinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type Client interface {
	ListStreams(context.Context, *awsKinesis.ListStreamsInput, ...func(*awsKinesis.Options)) (*awsKinesis.ListStreamsOutput, error)
	DescribeStreamSummary(context.Context, *awsKinesis.DescribeStreamSummaryInput, ...func(*awsKinesis.Options)) (*awsKinesis.DescribeStreamSummaryOutput, error)
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
	return model.ServiceDescriptor{Name: "kinesis", DisplayName: "Kinesis", Support: model.SupportStructureOnly}
}

func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	contribution := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.Kinesis))}
	for _, resource := range project.Resources.Kinesis {
		contribution.Selections = append(contribution.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "kinesis", Type: "stream", ID: resource.Name, ARN: resource.ARN}})
	}
	return contribution
}

func (*Adapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}

func (a *Adapter) Discover(ctx context.Context, scope model.SourceScope) (model.DiscoveryResult, error) {
	if a.client == nil {
		return model.DiscoveryResult{}, nil
	}
	var result model.DiscoveryResult
	start := ""
	for {
		out, err := a.client.ListStreams(ctx, &awsKinesis.ListStreamsInput{ExclusiveStartStreamName: valueOrNil(start)})
		if err != nil {
			return result, err
		}
		for _, name := range out.StreamNames {
			description, err := a.client.DescribeStreamSummary(ctx, &awsKinesis.DescribeStreamSummaryInput{StreamName: &name})
			if err != nil {
				result.Findings = append(result.Findings, model.Finding{Code: "KINESIS_STREAM_DISCOVERY_FAILED", Severity: model.SeverityWarning, Support: model.SupportPartial, Resource: name, Message: err.Error()})
				continue
			}
			arn := ""
			if description.StreamDescriptionSummary != nil && description.StreamDescriptionSummary.StreamARN != nil {
				arn = *description.StreamDescriptionSummary.StreamARN
			}
			result.Resources = append(result.Resources, model.ResourceSummary{Ref: model.ResourceRef{Service: "kinesis", Type: "stream", ID: name, ARN: arn}, Name: name, Region: scope.Region})
		}
		if !aws.ToBool(out.HasMoreStreams) || len(out.StreamNames) == 0 {
			break
		}
		start = out.StreamNames[len(out.StreamNames)-1]
	}
	sort.Slice(result.Resources, func(i, j int) bool { return result.Resources[i].Name < result.Resources[j].Name })
	return result, nil
}

func (*Adapter) Capture(_ context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if opts.IncludeData {
		return nil, model.ErrValidation
	}
	return model.NewSnapshot(ref, "kinesis", map[string]string{"name": ref.ID, "arn": ref.ARN})
}

func (*Adapter) Dependencies(*model.Snapshot) []model.Dependency { return nil }

func (*Adapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }

func valueOrNil(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
