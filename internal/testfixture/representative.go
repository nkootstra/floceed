package testfixture

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/compose"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

const (
	representativeAccount = "123456789012"
	representativeRegion  = "eu-west-1"
)

// GenerateRepresentativeBundle writes a small, deterministic bundle that
// exercises every currently replayable service. S3, DynamoDB, and Kinesis
// include data; SNS and SQS contain structure/topology.
func GenerateRepresentativeBundle(root string) error {
	artifacts := root
	if err := os.MkdirAll(artifacts, 0o700); err != nil {
		return err
	}
	object := []byte("representative floceed fixture\n")
	item, err := json.Marshal(map[string]any{
		"id":   map[string]any{"S": "fixture-1"},
		"name": map[string]any{"S": "representative"},
	})
	if err != nil {
		return err
	}
	item = append(item, '\n')
	pack, index, err := representativeS3Artifacts(object)
	if err != nil {
		return err
	}
	objectRef, err := writeRepresentativeArtifact(artifacts, "bundle/data/s3/pack-000001.tar.gz", pack, "application/gzip")
	if err != nil {
		return err
	}
	indexRef, err := writeRepresentativeArtifact(artifacts, "bundle/data/s3/pack-000001.index.gz", index, "application/gzip")
	if err != nil {
		return err
	}
	itemRef, err := writeRepresentativeArtifact(artifacts, "bundle/data/dynamodb/items-000001.ndjson", item, "application/x-ndjson")
	if err != nil {
		return err
	}
	kinesisData, err := json.Marshal(map[string]any{"partition_key": "representative", "sequence_number": "1", "data_base64": base64.StdEncoding.EncodeToString([]byte("representative kinesis record\n"))})
	if err != nil {
		return err
	}
	kinesisData = append(kinesisData, '\n')
	kinesisRef, err := writeRepresentativeArtifact(artifacts, "bundle/data/kinesis/floceed-example-stream.ndjson", kinesisData, "application/x-ndjson")
	if err != nil {
		return err
	}
	sqsData, err := json.Marshal(map[string]any{"body_base64": base64.StdEncoding.EncodeToString([]byte("representative queue message\n")), "message_attributes": map[string]any{}})
	if err != nil {
		return err
	}
	sqsData = append(sqsData, '\n')
	sqsRef, err := writeRepresentativeArtifact(artifacts, "bundle/data/sqs/floceed-example-events.ndjson", sqsData, "application/x-ndjson")
	if err != nil {
		return err
	}

	queueARN := "arn:aws:sqs:" + representativeRegion + ":" + representativeAccount + ":floceed-example-events"
	topicARN := "arn:aws:sns:" + representativeRegion + ":" + representativeAccount + ":floceed-example-events"
	bucketARN := "arn:aws:s3:::floceed-example-assets"
	selected := []model.ResourceRef{
		{Service: "dynamodb", Type: "table", ID: "floceed-example-items"},
		{Service: "kinesis", Type: "stream", ID: "floceed-example-stream", ARN: "arn:aws:kinesis:" + representativeRegion + ":" + representativeAccount + ":stream/floceed-example-stream"},
		{Service: "s3", Type: "bucket", ID: "floceed-example-assets", ARN: bucketARN},
		{Service: "sns", Type: "topic", ID: "floceed-example-events", ARN: topicARN},
		{Service: "sqs", Type: "queue", ID: "floceed-example-events", ARN: queueARN},
	}
	dynamo, err := model.NewSnapshot(selected[0], "dynamodb", map[string]any{
		"name": "floceed-example-items", "attribute_definitions": []map[string]any{{"name": "id", "type": "S"}},
		"key_schema": []map[string]any{{"name": "id", "type": "HASH"}}, "billing_mode": "PAY_PER_REQUEST",
	})
	if err != nil {
		return err
	}
	dynamo.Dataset = &model.Dataset{Format: "dynamodb-ndjson-v1", Records: 1, SourceBytes: int64(len(item)), Consistency: "best_effort", Chunks: []model.DataChunk{{Data: itemRef, Records: 1, SourceBytes: int64(len(item))}}}
	stream, err := model.NewSnapshot(selected[1], "kinesis", map[string]any{"name": selected[1].ID, "arn": selected[1].ARN, "shard_count": 1})
	if err != nil {
		return err
	}
	stream.Dataset = &model.Dataset{Format: "kinesis-records-ndjson-v1", Records: 1, SourceBytes: int64(len(kinesisData)), Consistency: "best_effort", Chunks: []model.DataChunk{{Data: kinesisRef, Records: 1, SourceBytes: int64(len(kinesisData))}}}
	s3, err := model.NewSnapshot(selected[2], "s3", map[string]any{
		"name": "floceed-example-assets", "region": representativeRegion,
		"notifications": map[string]any{"QueueConfigurations": []map[string]any{{"Id": "events", "QueueArn": queueARN, "Events": []string{"s3:ObjectCreated:*"}}}, "TopicConfigurations": []map[string]any{{"Id": "events", "TopicArn": topicARN, "Events": []string{"s3:ObjectCreated:Put"}}}},
	})
	if err != nil {
		return err
	}
	s3.Dataset = &model.Dataset{Format: "s3-tar-gzip-v1", Records: 1, SourceBytes: int64(len(object)), Consistency: "best_effort", Chunks: []model.DataChunk{{Data: objectRef, Index: &indexRef, Records: 1, SourceBytes: int64(len(object))}}}
	queue, err := model.NewSnapshot(selected[4], "sqs", map[string]any{"name": selected[4].ID, "arn": queueARN})
	if err != nil {
		return err
	}
	queue.Dataset = &model.Dataset{Format: "sqs-messages-ndjson-v1", Records: 1, SourceBytes: int64(len(sqsData)), Consistency: "best_effort", Chunks: []model.DataChunk{{Data: sqsRef, Records: 1, SourceBytes: int64(len(sqsData))}}}
	topic, err := model.NewSnapshot(selected[3], "sns", map[string]any{"name": selected[3].ID, "arn": topicARN})
	if err != nil {
		return err
	}
	manifest := model.Manifest{
		SchemaVersion: model.CurrentManifestSchemaVersion,
		Tool:          model.ToolMetadata{Version: "representative-fixture"},
		Target:        model.TargetMetadata{FlociVersion: config.DefaultFlociVersion, Image: compose.Image},
		Source:        model.SourceMetadata{AccountID: representativeAccount, Region: representativeRegion},
		Capture:       model.CaptureMetadata{CapturedAt: time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)},
		Selected:      selected, Snapshots: []model.Snapshot{*dynamo, *stream, *s3, *topic, *queue},
		Operations: []model.Operation{{ID: "base:dynamodb:floceed-example-items", Stage: model.StageBase, Service: "dynamodb", ResourceID: "floceed-example-items", Action: "ensure"}},
	}
	project := config.Project{SchemaVersion: config.CurrentSchemaVersion, Source: config.Source{Region: representativeRegion, ExpectedAccountID: representativeAccount}, Target: config.Target{FlociVersion: config.DefaultFlociVersion, Port: config.DefaultPort, HookTimeoutSeconds: config.DefaultHookTimeoutSeconds}, Output: config.Output{Directory: ".floceed"}}
	if err := bundle.Render(context.Background(), filepath.Join(root, ".floceed"), project, manifest, bundle.RenderOptions{ArtifactRoot: artifacts, ValidateCompose: func(context.Context, string) error { return nil }}); err != nil {
		return err
	}
	return nil
}

func representativeS3Artifacts(object []byte) ([]byte, []byte, error) {
	var pack bytes.Buffer
	gz := gzip.NewWriter(&pack)
	tarWriter := tar.NewWriter(gz)
	if err := tarWriter.WriteHeader(&tar.Header{Name: "fixtures/hello.txt", Mode: 0o600, Size: int64(len(object)), ModTime: time.Unix(0, 0)}); err != nil {
		return nil, nil, err
	}
	if _, err := tarWriter.Write(object); err != nil {
		return nil, nil, err
	}
	if err := tarWriter.Close(); err != nil {
		return nil, nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, nil, err
	}
	digest := sha256.Sum256(object)
	entry := map[string]any{"key": "fixtures/hello.txt", "path": "fixtures/hello.txt", "size": len(object), "sha256": hex.EncodeToString(digest[:]), "content_type": "text/plain", "overwrite": "if-different"}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return nil, nil, err
	}
	encoded = append(encoded, '\n')
	var index bytes.Buffer
	indexGz := gzip.NewWriter(&index)
	if _, err := indexGz.Write(encoded); err != nil {
		return nil, nil, err
	}
	if err := indexGz.Close(); err != nil {
		return nil, nil, err
	}
	return pack.Bytes(), index.Bytes(), nil
}

func writeRepresentativeArtifact(root, relative string, data []byte, mediaType string) (model.ArtifactRef, error) {
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return model.ArtifactRef{}, err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return model.ArtifactRef{}, err
	}
	digest := sha256.Sum256(data)
	return model.ArtifactRef{Path: relative, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data)), MediaType: mediaType}, nil
}
