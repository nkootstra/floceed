// Package kinesis captures Kinesis stream structure and bounded/full records.
package kinesis

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsKinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
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
	for _, resource := range project.Resources.Kinesis {
		contribution.RequiredIAMActions = append(contribution.RequiredIAMActions, "kinesis:ListStreams", "kinesis:DescribeStreamSummary")
		selection := catalog.Selection{Resource: model.ResourceRef{Service: "kinesis", Type: "stream", ID: resource.Name, ARN: resource.ARN}}
		if resource.Data != nil {
			contribution.RequiredIAMActions = append(contribution.RequiredIAMActions, "kinesis:ListShards", "kinesis:GetShardIterator", "kinesis:GetRecords")
			selection.Options.IncludeData = resource.Data.Enabled
			selection.Options.Mode = string(resource.Data.Mode)
			selection.Options.Limits.MaxItems = resource.Data.MaxRecords
			selection.Options.Limits.MaxTotalBytes = resource.Data.MaxBytes
		}
		contribution.Selections = append(contribution.Selections, selection)
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

func valueOrNil(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func (a *Adapter) listShards(ctx context.Context, client RecordClient, ref model.ResourceRef) ([]string, error) {
	var shards []string
	var token *string
	for {
		out, err := client.ListShards(ctx, &awsKinesis.ListShardsInput{StreamARN: valueOrNil(ref.ARN), NextToken: token})
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
	name := "bundle/data/kinesis/" + ref.ID + ".ndjson"
	clean := filepath.Clean(filepath.FromSlash(name))
	if filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return model.ArtifactRef{}, 0, 0, fmt.Errorf("unsafe Kinesis artifact path: %w", model.ErrValidation)
	}
	destination := filepath.Join(opts.ArtifactDirectory, clean)
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return model.ArtifactRef{}, 0, 0, err
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return model.ArtifactRef{}, 0, 0, err
	}
	hash := sha256.New()
	writer := bufio.NewWriter(io.MultiWriter(file, hash))
	var records, sourceBytes int64
	maxRecords, maxBytes := int64(opts.Limits.MaxItems), opts.Limits.MaxTotalBytes
	limitReached := false
	for _, shardID := range shards {
		if limitReached || maxRecords > 0 && records >= maxRecords {
			break
		}
		iterator, err := client.GetShardIterator(ctx, &awsKinesis.GetShardIteratorInput{StreamARN: valueOrNil(ref.ARN), ShardId: &shardID, ShardIteratorType: types.ShardIteratorTypeTrimHorizon})
		if err != nil {
			file.Close()
			return model.ArtifactRef{}, 0, 0, err
		}
		for iterator != nil && iterator.ShardIterator != nil {
			limit := int32(1000)
			if maxRecords > 0 && maxRecords-records < int64(limit) {
				limit = int32(maxRecords - records)
			}
			out, err := client.GetRecords(ctx, &awsKinesis.GetRecordsInput{ShardIterator: iterator.ShardIterator, Limit: &limit})
			if err != nil {
				file.Close()
				return model.ArtifactRef{}, 0, 0, err
			}
			if len(out.Records) == 0 {
				break
			}
			for _, record := range out.Records {
				encoded, _ := json.Marshal(map[string]any{"partition_key": aws.ToString(record.PartitionKey), "sequence_number": aws.ToString(record.SequenceNumber), "data_base64": base64.StdEncoding.EncodeToString(record.Data)})
				encoded = append(encoded, '\n')
				if maxBytes > 0 && sourceBytes+int64(len(encoded)) > maxBytes {
					limitReached = true
					break
				}
				if _, err := writer.Write(encoded); err != nil {
					file.Close()
					return model.ArtifactRef{}, 0, 0, err
				}
				records++
				sourceBytes += int64(len(encoded))
				if opts.Progress != nil {
					opts.Progress(model.ProgressEvent{Operation: "pull", Phase: "capture", Service: "kinesis", Resource: ref.ID, CompletedRecords: records, CompletedBytes: sourceBytes, TotalRecords: maxRecords, TotalBytes: maxBytes, TotalPrecision: "unknown"})
				}
			}
			if limitReached || maxRecords > 0 && records >= maxRecords || maxBytes > 0 && sourceBytes >= maxBytes {
				break
			}
			iterator.ShardIterator = out.NextShardIterator
		}
	}
	if err := writer.Flush(); err != nil {
		file.Close()
		return model.ArtifactRef{}, 0, 0, err
	}
	if err := file.Close(); err != nil {
		return model.ArtifactRef{}, 0, 0, err
	}
	return model.ArtifactRef{Path: filepath.ToSlash(name), SHA256: hex.EncodeToString(hash.Sum(nil)), Size: sourceBytes}, records, sourceBytes, nil
}
