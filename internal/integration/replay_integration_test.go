//go:build integration

package integration_test

import (
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
	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/compose"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	accountID = "123456789012"
	region    = "eu-west-1"
	bucket    = "floceed-integration-bucket"
	table     = "floceed-integration-items"
)

func TestGeneratedBundleReplaysIdempotentlyWithPersistentState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	bundleRoot := renderSyntheticBundle(t, ctx)
	persistence := t.TempDir()

	first := startFloci(t, ctx, bundleRoot, persistence)
	endpoint := endpointFor(t, ctx, first)
	waitForReady(t, ctx, first, endpoint)
	verifySnapshot(t, ctx, endpoint)
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
	verifySnapshot(t, ctx, endpoint)
	if got := objectVersions(t, ctx, endpoint); got != firstVersions {
		t.Fatalf("second replay changed S3 version count from %d to %d", firstVersions, got)
	}
}

func renderSyntheticBundle(t *testing.T, ctx context.Context) string {
	t.Helper()
	root := t.TempDir()
	artifacts := filepath.Join(root, "artifacts")
	objectPath := "bundle/data/s3/object.bin"
	itemPath := "bundle/data/dynamodb/items.ndjson"
	object := []byte("hello from floceed\n")
	item := []byte(`{"id":{"S":"fixture-1"},"payload":{"M":{"active":{"BOOL":true},"count":{"N":"42"}}}}` + "\n")
	objectRef := writeArtifact(t, artifacts, objectPath, object, "application/octet-stream")
	itemRef := writeArtifact(t, artifacts, itemPath, item, "application/x-ndjson")

	manifest := model.Manifest{
		SchemaVersion: model.CurrentManifestSchemaVersion,
		Tool:          model.ToolMetadata{Version: "integration-test"},
		Target:        model.TargetMetadata{FlociVersion: config.DefaultFlociVersion, Image: compose.Image},
		Source:        model.SourceMetadata{AccountID: accountID, Region: region},
		Capture:       model.CaptureMetadata{CapturedAt: time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)},
		Selected: []model.ResourceRef{
			{Service: "dynamodb", Type: "table", ID: table},
			{Service: "s3", Type: "bucket", ID: bucket, ARN: "arn:aws:s3:::" + bucket},
		},
		Snapshots: []model.Snapshot{
			testSnapshot(t, model.Snapshot{
				Resource: model.ResourceRef{Service: "dynamodb", Type: "table", ID: table},
				Service:  "dynamodb",
			}, map[string]any{
				"name":                  table,
				"attribute_definitions": []map[string]any{{"name": "id", "type": "S"}},
				"key_schema":            []map[string]any{{"name": "id", "type": "HASH"}},
				"billing_mode":          "PAY_PER_REQUEST",
				"source_billing_mode":   "PAY_PER_REQUEST",
				"stream":                map[string]any{"enabled": false},
				"ttl":                   map[string]any{"enabled": false},
				"tags":                  []map[string]any{{"key": "floceed", "value": "integration"}},
			}, []model.ArtifactRef{itemRef}),
			testSnapshot(t, model.Snapshot{
				Resource: model.ResourceRef{Service: "s3", Type: "bucket", ID: bucket, ARN: "arn:aws:s3:::" + bucket},
				Service:  "s3",
			}, map[string]any{
				"name":       bucket,
				"region":     region,
				"versioning": "Enabled",
				"tags":       []map[string]any{{"key": "floceed", "value": "integration"}},
				"objects": []map[string]any{{
					"key": "fixtures/hello.txt", "path": objectPath, "size": len(object),
					"sha256": objectRef.SHA256, "content_type": "text/plain", "overwrite": "if-different",
				}},
			}, []model.ArtifactRef{objectRef}),
		},
		Operations: []model.Operation{
			{ID: "base:dynamodb:" + table, Stage: model.StageBase, Service: "dynamodb", ResourceID: table, Action: "ensure"},
			{ID: "base:s3:" + bucket, Stage: model.StageBase, Service: "s3", ResourceID: bucket, Action: "ensure"},
			{ID: "data:dynamodb:" + table, Stage: model.StageData, Service: "dynamodb", ResourceID: table, Action: "upsert"},
			{ID: "data:s3:" + bucket, Stage: model.StageData, Service: "s3", ResourceID: bucket, Action: "upsert"},
		},
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

func verifySnapshot(t *testing.T, ctx context.Context, endpoint string) {
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
	if err != nil || string(body) != "hello from floceed\n" {
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
		TableName: aws.String(table), Key: map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "fixture-1"}},
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
