package kinesis

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsKinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type discoveryClient struct{}

func (discoveryClient) ListStreams(context.Context, *awsKinesis.ListStreamsInput, ...func(*awsKinesis.Options)) (*awsKinesis.ListStreamsOutput, error) {
	return &awsKinesis.ListStreamsOutput{StreamNames: []string{"events"}}, nil
}
func (discoveryClient) DescribeStreamSummary(context.Context, *awsKinesis.DescribeStreamSummaryInput, ...func(*awsKinesis.Options)) (*awsKinesis.DescribeStreamSummaryOutput, error) {
	return &awsKinesis.DescribeStreamSummaryOutput{StreamDescriptionSummary: &types.StreamDescriptionSummary{StreamARN: aws.String("arn:aws:kinesis:eu-west-1:123456789012:stream/events")}}, nil
}

type recordClient struct{ discoveryClient }

func (recordClient) ListShards(context.Context, *awsKinesis.ListShardsInput, ...func(*awsKinesis.Options)) (*awsKinesis.ListShardsOutput, error) {
	return &awsKinesis.ListShardsOutput{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}}, nil
}
func (recordClient) GetShardIterator(context.Context, *awsKinesis.GetShardIteratorInput, ...func(*awsKinesis.Options)) (*awsKinesis.GetShardIteratorOutput, error) {
	return &awsKinesis.GetShardIteratorOutput{ShardIterator: aws.String("iterator")}, nil
}
func (recordClient) GetRecords(context.Context, *awsKinesis.GetRecordsInput, ...func(*awsKinesis.Options)) (*awsKinesis.GetRecordsOutput, error) {
	return &awsKinesis.GetRecordsOutput{Records: []types.Record{{PartitionKey: aws.String("partition"), SequenceNumber: aws.String("1"), Data: []byte("hello")}}}, nil
}

func TestDiscoverReturnsStreamsForTUISelection(t *testing.T) {
	result, err := New(discoveryClient{}).Discover(context.Background(), model.SourceScope{Region: "eu-west-1"})
	if err != nil || len(result.Resources) != 1 || result.Resources[0].Ref.ID != "events" {
		t.Fatalf("discovery = %#v, %v", result, err)
	}
}

func TestMetadataOnlyStreamCapture(t *testing.T) {
	adapter := New()
	project := config.Project{Resources: config.Resources{Kinesis: []config.KinesisResource{{Name: "events", ARN: "arn:aws:kinesis:eu-west-1:123456789012:stream/events"}}}}
	selection := adapter.Plan(project, true).Selections[0]
	snapshot, err := adapter.Capture(context.Background(), model.SourceScope{}, selection.Resource, selection.Options)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Data != nil || snapshot.Dataset != nil {
		t.Fatalf("metadata snapshot contains data: %#v", snapshot)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCapturesBoundedRecords(t *testing.T) {
	root := t.TempDir()
	project := config.Project{Resources: config.Resources{Kinesis: []config.KinesisResource{{Name: "events", ARN: "arn:aws:kinesis:eu-west-1:123456789012:stream/events", Data: &config.KinesisDataPolicy{Enabled: true, Mode: config.DataModeBounded, MaxRecords: 10}}}}}
	selection := New(recordClient{}).Plan(project, true).Selections[0]
	selection.Options.ArtifactDirectory = root
	snapshot, err := New(recordClient{}).Capture(context.Background(), model.SourceScope{}, selection.Resource, selection.Options)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Dataset == nil || snapshot.Dataset.Records != 1 || snapshot.Dataset.Format != "kinesis-records-ndjson-v1" {
		t.Fatalf("unexpected dataset: %#v", snapshot.Dataset)
	}
	path := filepath.Join(root, filepath.FromSlash(snapshot.Dataset.Chunks[0].Data.Path))
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		t.Fatalf("record artifact: %v, %q", err, data)
	}
	if err := (model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Snapshots: []model.Snapshot{*snapshot}}).Validate(); err != nil {
		t.Fatal(err)
	}
}
