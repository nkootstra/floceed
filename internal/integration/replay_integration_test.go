//go:build integration

package integration_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/compose"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	accountID  = "123456789012"
	region     = "eu-west-1"
	bucket     = "floceed-integration-bucket"
	table      = "floceed-integration-items"
	eventQueue = "floceed-integration-events"
	eventTopic = "floceed-integration-topic"
)

func TestGeneratedBundleReplaysIdempotentlyWithPersistentState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	bundleRoot := renderSyntheticBundle(t, ctx, replayFixture{
		SchemaVersion: 2,
		ObjectBody:    "hello from floceed\n",
		ItemID:        "fixture-1",
	})
	persistence := t.TempDir()

	first := startFloci(t, ctx, bundleRoot, persistence)
	endpoint := endpointFor(t, ctx, first)
	waitForReady(t, ctx, first, endpoint)
	verifySnapshot(t, ctx, endpoint, "hello from floceed\n", "fixture-1")
	firstVersions := objectVersions(t, ctx, endpoint)
	if firstVersions != 1 {
		t.Fatalf("first replay created %d object versions, want 1", firstVersions)
	}
	if err := first.Terminate(ctx); err != nil {
		t.Fatalf("terminate first Floci container: %v", err)
	}

	second := startFloci(t, ctx, bundleRoot, persistence)
	defer testcontainers.CleanupContainer(t, second)
	endpoint = endpointFor(t, ctx, second)
	waitForReady(t, ctx, second, endpoint)
	verifySnapshot(t, ctx, endpoint, "hello from floceed\n", "fixture-1")
	if got := objectVersions(t, ctx, endpoint); got != firstVersions {
		t.Fatalf("second replay changed S3 version count from %d to %d", firstVersions, got)
	}
}

func TestGovernedSchema3BundleReplaysPreparedTransformedArtifacts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	const transformedObject = "fictional fixture content\n"
	const transformedItemID = "pseudonym/v1:09f0a1b2c3d4"
	bundleRoot := renderSyntheticBundle(t, ctx, replayFixture{
		SchemaVersion: 3,
		ObjectBody:    transformedObject,
		ItemID:        transformedItemID,
		Governance: &model.GovernanceAudit{
			Profile:        "share-safe",
			PolicyIdentity: "policy-opaque-001",
			CohortIdentity: "cohort-opaque-001",
			KeyIDs:         []string{"fixtures-2026-08"},
			Algorithms:     []string{"cohort-rank/v1", "pseudonym/v1"},
			Rules: []model.GovernanceRuleAudit{
				{RuleID: "customer-email", Action: "pseudonymize", Count: model.CountBucket1To9},
				{RuleID: "text-body", Action: "replace", Count: model.CountBucket1To9},
			},
			Cohorts: []model.GovernanceCohortAudit{{ResourceIdentity: "resource-opaque-001", Eligible: model.CountBucket1To9, Retained: model.CountBucket1To9}},
		},
	})

	container := startFloci(t, ctx, bundleRoot, t.TempDir())
	defer testcontainers.CleanupContainer(t, container)
	endpoint := endpointFor(t, ctx, container)
	waitForReady(t, ctx, container, endpoint)
	verifySnapshot(t, ctx, endpoint, transformedObject, transformedItemID)
}

func TestReplayUsesStandaloneBundleAfterCaptureStateIsDetached(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	bundleRoot := renderSyntheticBundle(t, ctx, replayFixture{
		SchemaVersion: 3,
		ObjectBody:    "standalone reused and refreshed fixture\n",
		ItemID:        "standalone-fixture",
	})
	captureState := filepath.Join(filepath.Dir(bundleRoot), "artifacts")
	detachedState := filepath.Join(filepath.Dir(bundleRoot), "detached-capture-state")
	if err := os.Rename(captureState, detachedState); err != nil {
		t.Fatalf("detach capture state: %v", err)
	}
	if err := bundle.ValidateGenerated(bundleRoot); err != nil {
		t.Fatalf("validate bundle without capture state: %v", err)
	}

	container := startFloci(t, ctx, bundleRoot, t.TempDir())
	defer testcontainers.CleanupContainer(t, container)
	endpoint := endpointFor(t, ctx, container)
	waitForReady(t, ctx, container, endpoint)
	verifySnapshot(t, ctx, endpoint, "standalone reused and refreshed fixture\n", "standalone-fixture")
}

func TestReplayCreatesSelectedEventTargetsBeforeS3Links(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	bundleRoot := renderSyntheticBundle(t, ctx, replayFixture{SchemaVersion: 3, ObjectBody: "event-linked\n", ItemID: "event-linked", EventDependencies: true})
	persistence := t.TempDir()
	container := startFloci(t, ctx, bundleRoot, persistence)
	endpoint := endpointFor(t, ctx, container)
	waitForReady(t, ctx, container, endpoint)
	assertEventTargets(t, ctx, endpoint)
	if err := container.Terminate(ctx); err != nil {
		t.Fatalf("terminate first replay: %v", err)
	}
	second := startFloci(t, ctx, bundleRoot, persistence)
	defer testcontainers.CleanupContainer(t, second)
	endpoint = endpointFor(t, ctx, second)
	waitForReady(t, ctx, second, endpoint)
	assertEventTargets(t, ctx, endpoint)
}

func assertEventTargets(t *testing.T, ctx context.Context, endpoint string) {
	t.Helper()
	awsCredentials := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(accountID, "test", ""))
	awsConfig := aws.Config{Region: region, Credentials: awsCredentials}
	sqsClient := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(endpoint) })
	snsClient := sns.NewFromConfig(awsConfig, func(options *sns.Options) { options.BaseEndpoint = aws.String(endpoint) })
	s3Client := s3.NewFromConfig(awsConfig, func(options *s3.Options) { options.BaseEndpoint = aws.String(endpoint); options.UsePathStyle = true })
	queue, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(eventQueue)})
	if err != nil {
		t.Fatalf("get replayed queue: %v", err)
	}
	queueAttributes, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: queue.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatalf("get replayed queue attributes: %v", err)
	}
	queueARN := queueAttributes.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	notifications, err := s3Client.GetBucketNotificationConfiguration(ctx, &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("get replayed notifications: %v", err)
	}
	if len(notifications.QueueConfigurations) != 1 || aws.ToString(notifications.QueueConfigurations[0].QueueArn) != queueARN {
		t.Fatalf("replayed queue notification = %#v", notifications.QueueConfigurations)
	}
	if len(notifications.TopicConfigurations) != 1 || aws.ToString(notifications.TopicConfigurations[0].TopicArn) == "" {
		t.Fatalf("replayed topic notification = %#v", notifications.TopicConfigurations)
	}
	topicARN := aws.ToString(notifications.TopicConfigurations[0].TopicArn)
	if _, err := snsClient.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(topicARN)}); err != nil {
		t.Fatalf("get replayed topic attributes: %v", err)
	}
	filter := notifications.QueueConfigurations[0].Filter
	if filter == nil || filter.Key == nil || len(filter.Key.FilterRules) != 1 || aws.ToString(filter.Key.FilterRules[0].Value) != "incoming/" {
		t.Fatalf("replayed queue filter = %#v", filter)
	}
}

type replayFixture struct {
	SchemaVersion     int
	ObjectBody        string
	ItemID            string
	Governance        *model.GovernanceAudit
	EventDependencies bool
}

func renderSyntheticBundle(t *testing.T, ctx context.Context, fixture replayFixture) string {
	t.Helper()
	root := t.TempDir()
	artifacts := filepath.Join(root, "artifacts")
	itemPath := "bundle/data/dynamodb/items.ndjson"
	object := []byte(fixture.ObjectBody)
	item := []byte(fmt.Sprintf(`{"id":{"S":%q},"payload":{"M":{"active":{"BOOL":true},"count":{"N":"42"}}}}`, fixture.ItemID) + "\n")
	packPath := "bundle/data/s3/pack-000001.tar.gz"
	indexPath := "bundle/data/s3/pack-000001.index.ndjson.gz"
	entryName := "object.bin"
	var pack bytes.Buffer
	packGzip := gzip.NewWriter(&pack)
	packGzip.Header.ModTime = time.Unix(0, 0)
	archive := tar.NewWriter(packGzip)
	if err := archive.WriteHeader(&tar.Header{Name: entryName, Mode: 0o600, Size: int64(len(object)), ModTime: time.Unix(0, 0)}); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.Write(object); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := packGzip.Close(); err != nil {
		t.Fatal(err)
	}
	objectDigest := sha256.Sum256(object)
	indexRecord := map[string]any{"key": "fixtures/hello.txt", "path": entryName, "size": len(object), "sha256": hex.EncodeToString(objectDigest[:]), "content_type": "text/plain", "overwrite": "if-different"}
	var index bytes.Buffer
	indexGzip := gzip.NewWriter(&index)
	indexGzip.Header.ModTime = time.Unix(0, 0)
	if err := json.NewEncoder(indexGzip).Encode(indexRecord); err != nil {
		t.Fatal(err)
	}
	if err := indexGzip.Close(); err != nil {
		t.Fatal(err)
	}
	packRef := writeArtifact(t, artifacts, packPath, pack.Bytes(), "application/gzip")
	indexRef := writeArtifact(t, artifacts, indexPath, index.Bytes(), "application/gzip")
	itemRef := writeArtifact(t, artifacts, itemPath, item, "application/x-ndjson")
	dynamoSnapshot := testSnapshot(t, model.Snapshot{Resource: model.ResourceRef{Service: "dynamodb", Type: "table", ID: table}, Service: "dynamodb"}, map[string]any{"name": table, "attribute_definitions": []map[string]any{{"name": "id", "type": "S"}}, "key_schema": []map[string]any{{"name": "id", "type": "HASH"}}, "billing_mode": "PAY_PER_REQUEST", "source_billing_mode": "PAY_PER_REQUEST", "stream": map[string]any{"enabled": false}, "ttl": map[string]any{"enabled": false}, "tags": []map[string]any{{"key": "floceed", "value": "integration"}}}, nil)
	dynamoSnapshot.Dataset = &model.Dataset{Format: "dynamodb-ndjson-v1", Records: 1, SourceBytes: int64(len(item)), Consistency: "best_effort", Chunks: []model.DataChunk{{Data: itemRef, Records: 1, SourceBytes: int64(len(item))}}}
	s3Snapshot := testSnapshot(t, model.Snapshot{Resource: model.ResourceRef{Service: "s3", Type: "bucket", ID: bucket, ARN: "arn:aws:s3:::" + bucket}, Service: "s3"}, map[string]any{"name": bucket, "region": region, "versioning": "Enabled", "tags": []map[string]any{{"key": "floceed", "value": "integration"}}}, nil)
	s3Snapshot.Dataset = &model.Dataset{Format: "s3-tar-gzip-v1", Records: 1, SourceBytes: int64(len(object)), Consistency: "best_effort", Chunks: []model.DataChunk{{Data: packRef, Index: &indexRef, Records: 1, SourceBytes: int64(len(object))}}}
	selected := []model.ResourceRef{
		{Service: "dynamodb", Type: "table", ID: table},
		{Service: "s3", Type: "bucket", ID: bucket, ARN: "arn:aws:s3:::" + bucket},
	}
	snapshots := []model.Snapshot{dynamoSnapshot, s3Snapshot}
	operations := []model.Operation{
		{ID: "base:dynamodb:" + table, Stage: model.StageBase, Service: "dynamodb", ResourceID: table, Action: "ensure"},
		{ID: "base:s3:" + bucket, Stage: model.StageBase, Service: "s3", ResourceID: bucket, Action: "ensure"},
		{ID: "data:dynamodb:" + table, Stage: model.StageData, Service: "dynamodb", ResourceID: table, Action: "upsert"},
		{ID: "data:s3:" + bucket, Stage: model.StageData, Service: "s3", ResourceID: bucket, Action: "upsert"},
	}
	if fixture.EventDependencies {
		queueARN := "arn:aws:sqs:" + region + ":" + accountID + ":" + eventQueue
		topicARN := "arn:aws:sns:" + region + ":" + accountID + ":" + eventTopic
		s3Structure := map[string]any{"name": bucket, "region": region, "versioning": "Enabled", "tags": []map[string]any{{"key": "floceed", "value": "integration"}}, "notifications": map[string]any{
			"QueueConfigurations": []map[string]any{{"Id": "queue-link", "QueueArn": queueARN, "Events": []string{"s3:ObjectCreated:*"}, "Filter": map[string]any{"Key": map[string]any{"FilterRules": []map[string]any{{"Name": "prefix", "Value": "incoming/"}}}}}},
			"TopicConfigurations": []map[string]any{{"Id": "topic-link", "TopicArn": topicARN, "Events": []string{"s3:ObjectCreated:Put"}}},
		}}
		s3Snapshot = testSnapshot(t, model.Snapshot{Resource: model.ResourceRef{Service: "s3", Type: "bucket", ID: bucket, ARN: "arn:aws:s3:::" + bucket}, Service: "s3"}, s3Structure, nil)
		sqsSnapshot := testSnapshot(t, model.Snapshot{Resource: model.ResourceRef{Service: "sqs", Type: "queue", ID: eventQueue, ARN: queueARN}, Service: "sqs"}, map[string]any{"name": eventQueue, "arn": queueARN}, nil)
		snsSnapshot := testSnapshot(t, model.Snapshot{Resource: model.ResourceRef{Service: "sns", Type: "topic", ID: eventTopic, ARN: topicARN}, Service: "sns"}, map[string]any{"name": eventTopic, "arn": topicARN}, nil)
		selected = append(selected, model.ResourceRef{Service: "sqs", Type: "queue", ID: eventQueue, ARN: queueARN}, model.ResourceRef{Service: "sns", Type: "topic", ID: eventTopic, ARN: topicARN})
		snapshots = []model.Snapshot{dynamoSnapshot, s3Snapshot, sqsSnapshot, snsSnapshot}
		operations = append(operations,
			model.Operation{ID: "base:sqs:" + eventQueue, Stage: model.StageBase, Service: "sqs", ResourceID: eventQueue, Action: "ensure"},
			model.Operation{ID: "base:sns:" + eventTopic, Stage: model.StageBase, Service: "sns", ResourceID: eventTopic, Action: "ensure"},
		)
	}
	manifest := model.Manifest{
		SchemaVersion: fixture.SchemaVersion,
		Tool:          model.ToolMetadata{Version: "integration-test"},
		Target:        model.TargetMetadata{FlociVersion: config.DefaultFlociVersion, Image: compose.Image},
		Source:        model.SourceMetadata{AccountID: accountID, Region: region},
		Capture:       model.CaptureMetadata{CapturedAt: time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)},
		Selected:      selected,
		Snapshots:     snapshots,
		Operations:    operations,
		Governance:    fixture.Governance,
	}
	project := config.Project{
		SchemaVersion: 1,
		Source:        config.Source{Region: region, ExpectedAccountID: accountID},
		Target: config.Target{
			FlociVersion: config.DefaultFlociVersion, Port: config.DefaultPort,
			HookTimeoutSeconds: config.DefaultHookTimeoutSeconds,
		},
		Output: config.Output{Directory: ".floceed"},
	}
	generated := filepath.Join(root, ".floceed")
	if err := bundle.Render(ctx, generated, project, manifest, bundle.RenderOptions{
		ArtifactRoot:    artifacts,
		ValidateCompose: func(context.Context, string) error { return nil },
	}); err != nil {
		t.Fatalf("render synthetic bundle: %v", err)
	}
	return generated
}

func writeArtifact(t *testing.T, root, relative string, data []byte, mediaType string) model.ArtifactRef {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return model.ArtifactRef{Path: relative, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data)), MediaType: mediaType}
}

func startFloci(t *testing.T, ctx context.Context, bundleRoot, persistence string) testcontainers.Container {
	t.Helper()
	request := testcontainers.ContainerRequest{
		Image:        compose.Image,
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"FLOCI_INIT_HOOKS_TIMEOUT_SECONDS": "300",
			"FLOCI_STORAGE_MODE":               "persistent",
			"FLOCI_STORAGE_PERSISTENT_PATH":    "/app/data",
		},
		Mounts: testcontainers.ContainerMounts{
			{Source: testcontainers.GenericBindMountSource{HostPath: bundleRoot}, Target: "/floceed", ReadOnly: true},
			{Source: testcontainers.GenericBindMountSource{HostPath: filepath.Join(bundleRoot, "init", "ready.d")}, Target: "/etc/floci/init/ready.d", ReadOnly: true},
			{Source: testcontainers.GenericBindMountSource{HostPath: persistence}, Target: "/app/data"},
		},
		WaitingFor: wait.ForListeningPort("4566/tcp").WithStartupTimeout(2 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: request, Started: true})
	if err != nil {
		t.Fatalf("start Floci: %v", err)
	}
	t.Cleanup(func() {
		if container.IsRunning() {
			_ = container.Terminate(context.Background())
		}
	})
	return container
}

func endpointFor(t *testing.T, ctx context.Context, container testcontainers.Container) string {
	t.Helper()
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "4566/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

func waitForReady(t *testing.T, ctx context.Context, container testcontainers.Container, endpoint string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("wait for Floci init: %v", ctx.Err())
		case <-ticker.C:
			response, err := client.Get(endpoint + "/_floci/init")
			if err != nil {
				state, stateErr := container.State(ctx)
				if stateErr == nil && !state.Running {
					logs, logsErr := container.Logs(ctx)
					if logsErr != nil {
						t.Fatalf("Floci exited with code %d before ready (logs unavailable: %v)", state.ExitCode, logsErr)
					}
					body, _ := io.ReadAll(logs)
					logs.Close()
					t.Fatalf("Floci exited with code %d before ready:\n%s", state.ExitCode, body)
				}
				continue
			}
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil || response.StatusCode/100 != 2 {
				continue
			}
			var status struct {
				Completed struct {
					Ready bool `json:"ready"`
				} `json:"completed"`
				Scripts struct {
					Ready []struct {
						Script string `json:"script"`
						State  string `json:"state"`
					} `json:"ready"`
				} `json:"scripts"`
			}
			if err := json.Unmarshal(body, &status); err != nil {
				continue
			}
			for _, script := range status.Scripts.Ready {
				if strings.EqualFold(script.State, "failed") || strings.EqualFold(script.State, "error") {
					t.Fatalf("Floci hook %s failed; init response: %s", script.Script, body)
				}
			}
			if status.Completed.Ready {
				return
			}
		}
	}
}

func clients(endpoint string) (*s3.Client, *dynamodb.Client) {
	credentials := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(accountID, "test", ""))
	awsConfig := aws.Config{Region: region, Credentials: credentials}
	s3Client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	ddbClient := dynamodb.NewFromConfig(awsConfig, func(options *dynamodb.Options) { options.BaseEndpoint = aws.String(endpoint) })
	return s3Client, ddbClient
}

func verifySnapshot(t *testing.T, ctx context.Context, endpoint, expectedObject, expectedItemID string) {
	t.Helper()
	s3Client, ddbClient := clients(endpoint)
	location, err := s3Client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("get bucket location: %v", err)
	}
	if got := string(location.LocationConstraint); got != region {
		t.Fatalf("bucket region = %q, want %q", got, region)
	}
	versioning, err := s3Client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil || string(versioning.Status) != "Enabled" {
		t.Fatalf("bucket versioning = %q, err %v", versioning.Status, err)
	}
	object, err := s3Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("fixtures/hello.txt")})
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	body, err := io.ReadAll(object.Body)
	object.Body.Close()
	if err != nil || string(body) != expectedObject {
		t.Fatalf("object body = %q, err %v", body, err)
	}
	bucketTags, err := s3Client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	if err != nil || !hasS3Tag(bucketTags.TagSet, "floceed", "integration") {
		t.Fatalf("bucket tags do not contain floceed=integration: %#v, err %v", bucketTags.TagSet, err)
	}

	description, err := ddbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		t.Fatalf("describe table: %v", err)
	}
	wantARNPart := ":" + region + ":" + accountID + ":table/" + table
	if !strings.Contains(aws.ToString(description.Table.TableArn), wantARNPart) {
		t.Fatalf("table ARN %q does not preserve region/account", aws.ToString(description.Table.TableArn))
	}
	tableTags, err := ddbClient.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{ResourceArn: description.Table.TableArn})
	if err != nil || !hasDynamoDBTag(tableTags.Tags, "floceed", "integration") {
		t.Fatalf("table tags do not contain floceed=integration: %#v, err %v", tableTags.Tags, err)
	}
	item, err := ddbClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table), Key: map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: expectedItemID}},
	})
	if err != nil {
		t.Fatalf("get DynamoDB item: %v", err)
	}
	if _, ok := item.Item["payload"].(*ddbtypes.AttributeValueMemberM); !ok {
		t.Fatalf("typed nested DynamoDB value was not preserved: %#v", item.Item)
	}
}

func hasS3Tag(tags []s3types.Tag, key, value string) bool {
	for _, tag := range tags {
		if aws.ToString(tag.Key) == key && aws.ToString(tag.Value) == value {
			return true
		}
	}
	return false
}

func testSnapshot(t *testing.T, snapshot model.Snapshot, structure any, data []model.ArtifactRef) model.Snapshot {
	t.Helper()
	snapshot.StructureVersion = model.CurrentSnapshotStructureVersion
	if err := model.SetStructure(&snapshot, structure); err != nil {
		t.Fatal(err)
	}
	snapshot.Data = data
	return snapshot
}

func hasDynamoDBTag(tags []ddbtypes.Tag, key, value string) bool {
	for _, tag := range tags {
		if aws.ToString(tag.Key) == key && aws.ToString(tag.Value) == value {
			return true
		}
	}
	return false
}

func objectVersions(t *testing.T, ctx context.Context, endpoint string) int {
	t.Helper()
	s3Client, _ := clients(endpoint)
	output, err := s3Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Prefix: aws.String("fixtures/hello.txt")})
	if err != nil {
		t.Fatalf("list object versions: %v", err)
	}
	return len(output.Versions)
}
