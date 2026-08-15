package s3

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func TestPlanOwnsS3SelectionOptionsAndIAM(t *testing.T) {
	project := config.Project{Resources: config.Resources{S3: []config.S3Resource{{Name: "assets", Data: &config.S3DataPolicy{Enabled: true, Prefixes: []string{"images/"}, MaxObjects: 4, MaxObjectBytes: 5, MaxTotalBytes: 6, Overwrite: config.OverwriteAlways}}}}}
	contribution := New(nil).Plan(project, true)
	if len(contribution.Selections) != 1 {
		t.Fatalf("selections = %#v", contribution.Selections)
	}
	selection := contribution.Selections[0]
	if selection.Resource != (model.ResourceRef{Service: "s3", Type: "bucket", ID: "assets", ARN: "arn:aws:s3:::assets"}) {
		t.Fatalf("resource = %#v", selection.Resource)
	}
	if !selection.Options.IncludeData || !reflect.DeepEqual(selection.Options.Prefixes, []string{"images/"}) || selection.Options.Overwrite != "always" || selection.Options.Limits != (model.DataLimits{MaxObjects: 4, MaxObjectBytes: 5, MaxTotalBytes: 6}) {
		t.Fatalf("options = %#v", selection.Options)
	}
	for _, action := range []string{"s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject", "s3:GetObjectTagging"} {
		if !contains(contribution.RequiredIAMActions, action) {
			t.Errorf("required IAM actions missing %q: %v", action, contribution.RequiredIAMActions)
		}
	}
}

func TestPlanStructureOnlyRequiresListBucketWithoutObjectReads(t *testing.T) {
	project := config.Project{Resources: config.Resources{S3: []config.S3Resource{{Name: "assets"}}}}
	contribution := New(nil).Plan(project, false)

	if !contains(contribution.RequiredIAMActions, "s3:ListBucket") {
		t.Fatalf("required IAM actions missing s3:ListBucket: %v", contribution.RequiredIAMActions)
	}
	for _, action := range []string{"s3:GetObject", "s3:GetObjectTagging"} {
		if contains(contribution.RequiredIAMActions, action) {
			t.Errorf("structure-only IAM actions unexpectedly contain %q: %v", action, contribution.RequiredIAMActions)
		}
	}
}

func TestFinalizePlanningDisablesUnresolvedNotifications(t *testing.T) {
	snapshot, err := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets", Region: "eu-west-1", Notifications: map[string]any{"queue": "jobs"}})
	if err != nil {
		t.Fatal(err)
	}
	dependency := model.Dependency{Kind: "notifications", To: model.ResourceRef{Service: "sqs", ID: "jobs"}}
	findings, err := New(nil).FinalizePlanning(snapshot, []model.Dependency{dependency})
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := model.DecodeStructure[Bucket](snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if bucket.Notifications != nil {
		t.Fatalf("notifications were not disabled: %#v", snapshot.Structure)
	}
	if len(findings) != 1 || findings[0].Code != "DEPENDENCY_NOT_SELECTED" || findings[0].Property != "notifications" {
		t.Fatalf("findings = %#v", findings)
	}
}

func TestFinalizePlanningRejectsInvalidStructure(t *testing.T) {
	snapshot := &model.Snapshot{Resource: model.ResourceRef{Service: "s3", ID: "assets"}, Service: "s3", StructureVersion: model.CurrentSnapshotStructureVersion, Structure: []byte("{")}
	dependency := model.Dependency{Kind: "notifications", To: model.ResourceRef{Service: "sqs", ID: "jobs"}}

	if _, err := New(nil).FinalizePlanning(snapshot, []model.Dependency{dependency}); err == nil {
		t.Fatal("expected invalid snapshot structure to fail planning finalization")
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

type discoveryClient struct {
	Client
	lists int
	heads []string
}

func (c *discoveryClient) ListBuckets(_ context.Context, input *awss3.ListBucketsInput, _ ...func(*awss3.Options)) (*awss3.ListBucketsOutput, error) {
	c.lists++
	if input.ContinuationToken == nil {
		return &awss3.ListBucketsOutput{Buckets: []types.Bucket{{Name: aws.String("elsewhere"), BucketRegion: aws.String("eu-west-1")}, {Name: aws.String("zeta")}}, ContinuationToken: aws.String("next")}, nil
	}
	return &awss3.ListBucketsOutput{Buckets: []types.Bucket{{Name: aws.String("alpha"), BucketRegion: aws.String("eu-west-1")}}}, nil
}

func (c *discoveryClient) HeadBucket(_ context.Context, input *awss3.HeadBucketInput, _ ...func(*awss3.Options)) (*awss3.HeadBucketOutput, error) {
	c.heads = append(c.heads, aws.ToString(input.Bucket))
	return &awss3.HeadBucketOutput{BucketRegion: aws.String("eu-west-1")}, nil
}

func TestDiscoverPaginatesSortsAndFallsBackToHeadBucket(t *testing.T) {
	client := &discoveryClient{}
	got, err := New(client).Discover(context.Background(), model.SourceScope{Region: "eu-west-1"})
	if err != nil {
		t.Fatal(err)
	}
	if client.lists != 2 {
		t.Fatalf("ListBuckets calls = %d, want 2", client.lists)
	}
	if strings.Join(client.heads, ",") != "zeta" {
		t.Fatalf("HeadBucket calls = %v, want [zeta]", client.heads)
	}
	want := []string{"alpha", "elsewhere", "zeta"}
	for i, resource := range got.Resources {
		if resource.Name != want[i] {
			t.Fatalf("resource[%d] = %q, want %q", i, resource.Name, want[i])
		}
	}
}

func TestWriteObjectUsesHashedPathAndPreservesMetadata(t *testing.T) {
	root := t.TempDir()
	output := &awss3.GetObjectOutput{
		Body:            io.NopCloser(bytes.NewBufferString("fixture")),
		ContentType:     aws.String("text/plain"),
		ContentEncoding: aws.String("identity"),
		CacheControl:    aws.String("max-age=10"),
		Metadata:        map[string]string{"owner": "floceed"},
		ChecksumSHA256:  aws.String("source-checksum"),
	}
	object, artifact, err := writeObject(context.Background(), "bucket", "../../unsafe/key", `"etag"`, output, model.CaptureOptions{ArtifactDirectory: root, Limits: model.DataLimits{MaxObjectBytes: 100}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(object.Path, "unsafe") || strings.Contains(object.Path, "..") {
		t.Fatalf("unsafe source key leaked into artifact path %q", object.Path)
	}
	if object.ContentType != "text/plain" || object.Metadata["owner"] != "floceed" || object.Checksums["sha256"] != "source-checksum" {
		t.Fatalf("metadata was not preserved: %#v", object)
	}
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(artifact.Path)))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "fixture" || artifact.SHA256 != object.SHA256 {
		t.Fatalf("artifact = %q/%s, object digest %s", data, artifact.SHA256, object.SHA256)
	}
	info, err := os.Stat(filepath.Join(root, filepath.FromSlash(artifact.Path)))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("artifact mode = %o, want 600", info.Mode().Perm())
	}
}

func TestWriteObjectRemovesPartialFileWhenStreamExceedsLimit(t *testing.T) {
	root := t.TempDir()
	_, _, err := writeObject(context.Background(), "bucket", "large", "", &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewBufferString("too large"))}, model.CaptureOptions{ArtifactDirectory: root, Limits: model.DataLimits{MaxObjectBytes: 3}})
	if err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("error = %v, want object limit error", err)
	}
	var files []string
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			files = append(files, path)
		}
		return err
	})
	if len(files) != 0 {
		t.Fatalf("partial files remain: %v", files)
	}
}

func TestTypedCORSNormalizationPreservesRequestShape(t *testing.T) {
	shape := normalize(corsShape{CORSRules: []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}}}).(map[string]any)
	if _, ok := shape["ResultMetadata"]; ok {
		t.Fatal("SDK result metadata crossed normalization boundary")
	}
	rules, ok := shape["CORSRules"].([]any)
	if !ok || len(rules) != 1 {
		t.Fatalf("CORSRules missing from %#v", shape)
	}
	rule := rules[0].(map[string]any)
	if !reflect.DeepEqual(rule["AllowedMethods"], []any{"GET"}) || !reflect.DeepEqual(rule["AllowedOrigins"], []any{"*"}) {
		t.Fatalf("unexpected normalized CORS rule: %#v", rule)
	}
}
