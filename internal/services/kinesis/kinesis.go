// Package kinesis captures Kinesis stream structure and bounded/full records.
package kinesis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsKinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/services/ndjson"
)

type Client interface {
	ListStreams(context.Context, *awsKinesis.ListStreamsInput, ...func(*awsKinesis.Options)) (*awsKinesis.ListStreamsOutput, error)
	DescribeStreamSummary(context.Context, *awsKinesis.DescribeStreamSummaryInput, ...func(*awsKinesis.Options)) (*awsKinesis.DescribeStreamSummaryOutput, error)
}

type RecordClient interface {
	Client
	ListShards(context.Context, *awsKinesis.ListShardsInput, ...func(*awsKinesis.Options)) (*awsKinesis.ListShardsOutput, error)
	GetShardIterator(context.Context, *awsKinesis.GetShardIteratorInput, ...func(*awsKinesis.Options)) (*awsKinesis.GetShardIteratorOutput, error)
	GetRecords(context.Context, *awsKinesis.GetRecordsInput, ...func(*awsKinesis.Options)) (*awsKinesis.GetRecordsOutput, error)
}

type Adapter struct{ client Client }

var _ catalog.Adapter = (*Adapter)(nil)

func New(client ...Client) *Adapter {
	var c Client
	if len(client) > 0 {
		c = client[0]
	}
	return &Adapter{client: c}
}

func (*Adapter) Service() model.ServiceDescriptor {
	return model.ServiceDescriptor{Name: "kinesis", DisplayName: "Kinesis", Support: model.SupportPartial}
}

func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	contribution := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.Kinesis))}
	dataCapture := false
	for _, resource := range project.Resources.Kinesis {
		selection := catalog.Selection{Resource: model.ResourceRef{Service: "kinesis", Type: "stream", ID: resource.Name, ARN: resource.ARN}}
		if resource.Data != nil {
			// Gate the data actions on the project configuration so a plan
			// reports the same RequiredIAMActions a subsequent pull needs;
			// the run-level includeData flag is enforced separately in
			// buildCaptureJobs, which forces IncludeData off for
			// structure-only pulls.
			dataCapture = dataCapture || resource.Data.Enabled
			selection.Options.IncludeData = resource.Data.Enabled
			selection.Options.Mode = string(resource.Data.Mode)
			selection.Options.Limits.MaxItems = resource.Data.MaxRecords
			selection.Options.Limits.MaxTotalBytes = resource.Data.MaxBytes
		}
		contribution.Selections = append(contribution.Selections, selection)
	}
	if len(project.Resources.Kinesis) != 0 {
		contribution.RequiredIAMActions = []string{"kinesis:ListStreams", "kinesis:DescribeStreamSummary"}
		if dataCapture {
			contribution.RequiredIAMActions = append(contribution.RequiredIAMActions, "kinesis:ListShards", "kinesis:GetShardIterator", "kinesis:GetRecords")
		}
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
		out, err := a.client.ListStreams(ctx, &awsKinesis.ListStreamsInput{ExclusiveStartStreamName: awsconfig.StringOrNil(start)})
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

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if !opts.IncludeData {
		return model.NewSnapshot(ref, "kinesis", map[string]any{"name": ref.ID, "arn": ref.ARN})
	}
	recordClient, ok := a.client.(RecordClient)
	if !ok {
		return nil, fmt.Errorf("Kinesis record client is required for data capture: %w", model.ErrValidation)
	}
	shards, err := a.listShards(ctx, recordClient, ref)
	if err != nil {
		return nil, err
	}
	snapshot, err := model.NewSnapshot(ref, "kinesis", map[string]any{"name": ref.ID, "arn": ref.ARN, "shard_count": len(shards)})
	if err != nil {
		return nil, err
	}
	if opts.ArtifactDirectory == "" {
		return nil, fmt.Errorf("Kinesis artifact directory is required: %w", model.ErrValidation)
	}
	artifact, records, sourceBytes, err := captureRecords(ctx, recordClient, ref, shards, opts)
	if err != nil {
		return nil, err
	}
	snapshot.Dataset = &model.Dataset{Format: "kinesis-records-ndjson-v1", Records: records, SourceBytes: sourceBytes, Consistency: "best_effort", Chunks: []model.DataChunk{{Data: artifact, Records: records, SourceBytes: sourceBytes}}}
	return snapshot, nil
}

func (*Adapter) Dependencies(*model.Snapshot) []model.Dependency { return nil }

func (*Adapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }

func (a *Adapter) listShards(ctx context.Context, client RecordClient, ref model.ResourceRef) ([]string, error) {
	var shards []string
	var token *string
	for {
		out, err := client.ListShards(ctx, &awsKinesis.ListShardsInput{StreamARN: awsconfig.StringOrNil(ref.ARN), NextToken: token})
		if err != nil {
			return nil, err
		}
		for _, shard := range out.Shards {
			if shard.ShardId != nil {
				shards = append(shards, *shard.ShardId)
			}
		}
		if out.NextToken == nil || *out.NextToken == "" {
			break
		}
		token = out.NextToken
	}
	sort.Strings(shards)
	return shards, nil
}

func captureRecords(ctx context.Context, client RecordClient, ref model.ResourceRef, shards []string, opts model.CaptureOptions) (model.ArtifactRef, int64, int64, error) {
	writer, err := ndjson.Create(opts.ArtifactDirectory, "bundle/data/kinesis/"+ref.ID+".ndjson", opts.Limits.MaxTotalBytes)
	if err != nil {
		return model.ArtifactRef{}, 0, 0, err
	}
	var records, sourceBytes int64
	maxRecords, maxBytes := int64(opts.Limits.MaxItems), opts.Limits.MaxTotalBytes
	limitReached := false
	for _, shardID := range shards {
		if limitReached || maxRecords > 0 && records >= maxRecords {
			break
		}
		iterator, err := client.GetShardIterator(ctx, &awsKinesis.GetShardIteratorInput{StreamARN: awsconfig.StringOrNil(ref.ARN), ShardId: &shardID, ShardIteratorType: types.ShardIteratorTypeTrimHorizon})
		if err != nil {
			writer.Abort()
			return model.ArtifactRef{}, 0, 0, err
		}
		for iterator != nil && iterator.ShardIterator != nil {
			limit := int32(1000)
			if maxRecords > 0 && maxRecords-records < int64(limit) {
				limit = int32(maxRecords - records)
			}
			out, err := client.GetRecords(ctx, &awsKinesis.GetRecordsInput{ShardIterator: iterator.ShardIterator, Limit: &limit})
			if err != nil {
				writer.Abort()
				return model.ArtifactRef{}, 0, 0, err
			}
			if len(out.Records) == 0 {
				break
			}
			for _, record := range out.Records {
				if err := ctx.Err(); err != nil {
					writer.Abort()
					return model.ArtifactRef{}, 0, 0, err
				}
				encoded, err := json.Marshal(map[string]any{"partition_key": aws.ToString(record.PartitionKey), "sequence_number": aws.ToString(record.SequenceNumber), "data_base64": base64.StdEncoding.EncodeToString(record.Data)})
				if err != nil {
					writer.Abort()
					return model.ArtifactRef{}, 0, 0, err
				}
				written, err := writer.Write(encoded)
				if err != nil {
					writer.Abort()
					return model.ArtifactRef{}, 0, 0, err
				}
				if !written {
					limitReached = true
					break
				}
				records++
				sourceBytes = writer.Size()
				if opts.Progress != nil {
					opts.Progress(model.ProgressEvent{Operation: "pull", Phase: "capture", Service: "kinesis", Resource: ref.ID, CompletedRecords: records, CompletedBytes: sourceBytes, TotalRecords: maxRecords, TotalBytes: maxBytes, TotalPrecision: "unknown"})
				}
			}
			if limitReached || maxRecords > 0 && records >= maxRecords {
				break
			}
			iterator.ShardIterator = out.NextShardIterator
		}
	}
	if err := ctx.Err(); err != nil {
		writer.Abort()
		return model.ArtifactRef{}, 0, 0, err
	}
	artifact, err := writer.Commit()
	if err != nil {
		return model.ArtifactRef{}, 0, 0, err
	}
	return artifact, records, artifact.Size, nil
}
