package s3

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nkootstra/floceed/internal/captureledger"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/governance"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/storage"
)

func TestAdapterSupportsReusableCapture(t *testing.T) {
	var _ catalog.ReusableAdapter = (*Adapter)(nil)
}

func TestReusableS3CaptureInventoriesButDoesNotDownloadUnchangedObjects(t *testing.T) {
	client := &packedDataClient{}
	adapter := New(client)
	scope := model.SourceScope{AccountID: "123456789012", Region: "eu-west-1"}
	ref := model.ResourceRef{Service: "s3", Type: "bucket", ID: "assets"}
	firstRoot := t.TempDir()
	firstSnapshot, _ := model.NewSnapshot(ref, "s3", Bucket{Name: ref.ID})
	firstOptions := model.CaptureOptions{IncludeData: true, Mode: "full", ArtifactDirectory: filepath.Join(firstRoot, "artifacts"), CheckpointDirectory: filepath.Join(firstRoot, "checkpoint"), Overwrite: "if-different", GovernanceAudit: governance.NewAudit()}
	first, err := adapter.captureObjectsReusable(context.Background(), scope, ref, &Bucket{Name: ref.ID}, firstSnapshot, firstOptions, catalog.ReuseRequest{Materialize: func(captureledger.Artifact) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if client.gets != 2 {
		t.Fatalf("first GetObject calls = %d, want 2", client.gets)
	}

	secondRoot := t.TempDir()
	secondSnapshot, _ := model.NewSnapshot(ref, "s3", Bucket{Name: ref.ID})
	secondOptions := firstOptions
	secondOptions.ArtifactDirectory, secondOptions.CheckpointDirectory = filepath.Join(secondRoot, "artifacts"), filepath.Join(secondRoot, "checkpoint")
	second, err := adapter.captureObjectsReusable(context.Background(), scope, ref, &Bucket{Name: ref.ID}, secondSnapshot, secondOptions, catalog.ReuseRequest{Candidates: []captureledger.Resource{*first}, Materialize: copyLedgerArtifact(firstOptions.ArtifactDirectory, secondOptions.ArtifactDirectory)})
	if err != nil {
		t.Fatal(err)
	}
	if client.gets != 2 {
		t.Fatalf("unchanged capture made %d total GetObject calls, want 2", client.gets)
	}
	if len(second.Units) != 1 || second.Units[0].Outcome != captureledger.UnitOutcomeReused || secondSnapshot.Dataset.Records != 2 || secondSnapshot.Dataset.SourceBytes != 2 {
		t.Fatalf("reuse result = %#v, dataset = %#v", second, secondSnapshot.Dataset)
	}
	for identity := range second.Units[0].Freshness.Components {
		if strings.Contains(identity, "a.txt") || strings.Contains(identity, "b.txt") {
			t.Fatalf("raw key leaked in ledger identity %q", identity)
		}
	}
}

func TestReusableS3CaptureReconstructsCheckpointPacksForPublicationAndReuse(t *testing.T) {
	client := &packedDataClient{}
	adapter := New(client)
	scope := model.SourceScope{AccountID: "123456789012", Region: "eu-west-1"}
	ref := model.ResourceRef{Service: "s3", Type: "bucket", ID: "assets"}
	root := t.TempDir()
	options := model.CaptureOptions{IncludeData: true, Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint"), Overwrite: "if-different", GovernanceAudit: governance.NewAudit()}
	firstSnapshot, _ := model.NewSnapshot(ref, "s3", Bucket{Name: ref.ID})
	if _, err := adapter.captureObjectsReusable(context.Background(), scope, ref, &Bucket{Name: ref.ID}, firstSnapshot, options, catalog.ReuseRequest{Materialize: func(captureledger.Artifact) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	resumedSnapshot, _ := model.NewSnapshot(ref, "s3", Bucket{Name: ref.ID})
	resumed, err := adapter.captureObjectsReusable(context.Background(), scope, ref, &Bucket{Name: ref.ID}, resumedSnapshot, options, catalog.ReuseRequest{Materialize: func(captureledger.Artifact) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if client.gets != 2 {
		t.Fatalf("checkpoint resume refetched bodies: GetObject calls = %d, want 2", client.gets)
	}
	if got, want := len(resumed.Units), len(resumedSnapshot.Dataset.Chunks); got != want || got != 1 {
		t.Fatalf("resumed ledger units = %d, dataset chunks = %d; want one per completed chunk", got, want)
	}
	if resumed.Units[0].Freshness.Digest == "" || len(resumed.Units[0].Artifacts) != 2 {
		t.Fatalf("resumed unit lacks durable inventory evidence/artifacts: %#v", resumed.Units[0])
	}

	reuseRoot := t.TempDir()
	reuseOptions := options
	reuseOptions.ArtifactDirectory, reuseOptions.CheckpointDirectory = filepath.Join(reuseRoot, "artifacts"), filepath.Join(reuseRoot, "checkpoint")
	reusedSnapshot, _ := model.NewSnapshot(ref, "s3", Bucket{Name: ref.ID})
	reused, err := adapter.captureObjectsReusable(context.Background(), scope, ref, &Bucket{Name: ref.ID}, reusedSnapshot, reuseOptions, catalog.ReuseRequest{Candidates: []captureledger.Resource{*resumed}, Materialize: copyLedgerArtifact(options.ArtifactDirectory, reuseOptions.ArtifactDirectory)})
	if err != nil {
		t.Fatal(err)
	}
	if client.gets != 2 || reused.Units[0].Outcome != captureledger.UnitOutcomeReused {
		t.Fatalf("published resumed unit was not reusable: gets=%d units=%#v", client.gets, reused.Units)
	}
}

func TestMissingS3UnitsUsesCompleteInventoryAcrossShiftedPackBoundaries(t *testing.T) {
	entries := []inventoryEntry{{Key: "a", ETag: "a", Size: 1}, {Key: "b", ETag: "b", Size: 1}, {Key: "c", ETag: "c", Size: 1}}
	first := s3PackFreshness(entries[:2])
	second := s3PackFreshness(entries[2:])
	candidate := captureledger.Resource{Units: []captureledger.Unit{{ID: "pack-000001", Freshness: first}, {ID: "pack-000002", Freshness: second}}}
	// Inserting an earlier object can move b or c into another deterministic
	// pack. Resource-wide membership still proves neither source identity is missing.
	current := map[string]struct{}{}
	for identity := range s3PackFreshness(entries).Components {
		current[identity] = struct{}{}
	}
	if missing := missingS3Units(candidate, current); len(missing) != 0 {
		t.Fatalf("boundary shift reported moved objects missing: %#v", missing)
	}
}

func copyLedgerArtifact(sourceRoot, destinationRoot string) func(captureledger.Artifact) error {
	return func(artifact captureledger.Artifact) error {
		payload, err := os.ReadFile(filepath.Join(sourceRoot, filepath.FromSlash(artifact.Path)))
		if err != nil {
			return err
		}
		destination := filepath.Join(destinationRoot, filepath.FromSlash(artifact.Path))
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			return err
		}
		return os.WriteFile(destination, payload, 0o600)
	}
}

func TestReusableS3CaptureRefreshReasonsAndMissingObjects(t *testing.T) {
	for _, test := range []struct {
		name        string
		inventory   []types.Object
		changeOpts  func(*model.CaptureOptions)
		materialize func(string, string) func(captureledger.Artifact) error
		wantReason  captureledger.Reason
		wantRecords int64
		wantMissing bool
	}{
		{name: "changed etag", inventory: []types.Object{{Key: aws.String("a.txt"), ETag: aws.String("changed"), Size: aws.Int64(1)}, {Key: aws.String("b.txt"), ETag: aws.String("b"), Size: aws.Int64(1)}}, wantReason: captureledger.ReasonSourceContentChanged, wantRecords: 2},
		{name: "new object", inventory: []types.Object{{Key: aws.String("a.txt"), ETag: aws.String("a"), Size: aws.Int64(1)}, {Key: aws.String("b.txt"), ETag: aws.String("b"), Size: aws.Int64(1)}, {Key: aws.String("c.txt"), ETag: aws.String("c"), Size: aws.Int64(1)}}, wantReason: captureledger.ReasonSourceContentChanged, wantRecords: 3},
		{name: "missing object", inventory: []types.Object{{Key: aws.String("a.txt"), ETag: aws.String("a"), Size: aws.Int64(1)}}, wantReason: captureledger.ReasonSourceContentChanged, wantRecords: 1, wantMissing: true},
		{name: "definition changed", changeOpts: func(options *model.CaptureOptions) { options.Overwrite = "always" }, wantReason: captureledger.ReasonCaptureDefinitionChanged, wantRecords: 2},
		{name: "corrupt artifact", materialize: func(_, _ string) func(captureledger.Artifact) error {
			return func(captureledger.Artifact) error {
				return &captureledger.InvalidationError{Reason: captureledger.ReasonArtifactCorrupt, Err: errors.New("corrupt")}
			}
		}, wantReason: captureledger.ReasonArtifactCorrupt, wantRecords: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &packedDataClient{}
			adapter := New(client)
			scope := model.SourceScope{AccountID: "123456789012", Region: "eu-west-1"}
			ref := model.ResourceRef{Service: "s3", Type: "bucket", ID: "assets"}
			firstRoot := t.TempDir()
			firstOptions := model.CaptureOptions{IncludeData: true, Mode: "full", ArtifactDirectory: filepath.Join(firstRoot, "artifacts"), CheckpointDirectory: filepath.Join(firstRoot, "checkpoint"), Overwrite: "if-different", GovernanceAudit: governance.NewAudit()}
			firstSnapshot, _ := model.NewSnapshot(ref, "s3", Bucket{Name: ref.ID})
			first, err := adapter.captureObjectsReusable(context.Background(), scope, ref, &Bucket{Name: ref.ID}, firstSnapshot, firstOptions, catalog.ReuseRequest{Materialize: func(captureledger.Artifact) error { return nil }})
			if err != nil {
				t.Fatal(err)
			}
			client.inventory = test.inventory
			secondRoot := t.TempDir()
			secondOptions := firstOptions
			secondOptions.ArtifactDirectory, secondOptions.CheckpointDirectory = filepath.Join(secondRoot, "artifacts"), filepath.Join(secondRoot, "checkpoint")
			if test.changeOpts != nil {
				test.changeOpts(&secondOptions)
			}
			materialize := copyLedgerArtifact(firstOptions.ArtifactDirectory, secondOptions.ArtifactDirectory)
			if test.materialize != nil {
				materialize = test.materialize(firstOptions.ArtifactDirectory, secondOptions.ArtifactDirectory)
			}
			secondSnapshot, _ := model.NewSnapshot(ref, "s3", Bucket{Name: ref.ID})
			second, err := adapter.captureObjectsReusable(context.Background(), scope, ref, &Bucket{Name: ref.ID}, secondSnapshot, secondOptions, catalog.ReuseRequest{Candidates: []captureledger.Resource{*first}, Materialize: materialize})
			if err != nil {
				t.Fatal(err)
			}
			if secondSnapshot.Dataset.Records != test.wantRecords || secondSnapshot.Dataset.SourceBytes != test.wantRecords {
				t.Fatalf("dataset totals = %#v", secondSnapshot.Dataset)
			}
			var packReason captureledger.Reason
			for _, unit := range second.Units {
				if strings.HasPrefix(unit.ID, "pack-") {
					packReason = unit.Reason
				}
			}
			if packReason != test.wantReason {
				t.Fatalf("reason = %q, want %q; units=%#v", packReason, test.wantReason, second.Units)
			}
			missing := false
			for _, unit := range second.Units {
				if unit.Reason == captureledger.ReasonSourceUnitMissing {
					missing = true
					if len(unit.Artifacts) != 0 {
						t.Fatal("missing source unit emitted artifacts")
					}
				}
			}
			if missing != test.wantMissing {
				t.Fatalf("missing outcome = %v, want %v", missing, test.wantMissing)
			}
			if test.name == "changed etag" {
				matched := false
				for _, etag := range client.ifMatches {
					matched = matched || etag == "changed"
				}
				if !matched {
					t.Fatalf("conditional reads = %#v, want changed inventory ETag", client.ifMatches)
				}
			}
		})
	}
}

type packedDataClient struct {
	Client
	gets      int
	inventory []types.Object
	ifMatches []string
}

func (c packedDataClient) ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	contents := c.inventory
	if contents == nil {
		contents = []types.Object{{Key: aws.String("a.txt"), ETag: aws.String("a"), Size: aws.Int64(1)}, {Key: aws.String("b.txt"), ETag: aws.String("b"), Size: aws.Int64(1)}}
	}
	return &awss3.ListObjectsV2Output{Contents: contents}, nil
}
func (c *packedDataClient) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	c.gets++
	c.ifMatches = append(c.ifMatches, aws.ToString(in.IfMatch))
	return &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(aws.ToString(in.Key)[:1]))}, nil
}
func (packedDataClient) GetObjectTagging(context.Context, *awss3.GetObjectTaggingInput, ...func(*awss3.Options)) (*awss3.GetObjectTaggingOutput, error) {
	return &awss3.GetObjectTaggingOutput{}, nil
}

func TestFullS3CaptureUsesPackedDatasetInsteadOfOneFilePerObject(t *testing.T) {
	root := t.TempDir()
	snapshot, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets", Region: "eu-west-1"})
	bucket := Bucket{Name: "assets", Region: "eu-west-1"}
	opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint"), Overwrite: "if-different"}
	client := &packedDataClient{}
	if err := New(client).captureObjects(context.Background(), "assets", &bucket, snapshot, opts); err != nil {
		t.Fatal(err)
	}
	if snapshot.Dataset == nil || snapshot.Dataset.Records != 2 || len(snapshot.Dataset.Chunks) != 1 || snapshot.Dataset.Chunks[0].Index == nil {
		t.Fatalf("dataset = %#v", snapshot.Dataset)
	}
	if len(bucket.Objects) != 0 {
		t.Fatalf("objects leaked into manifest structure: %#v", bucket.Objects)
	}
	packPath := filepath.Join(root, "artifacts", filepath.FromSlash(snapshot.Dataset.Chunks[0].Data.Path))
	f, err := os.Open(packPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	count := 0
	for {
		_, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("tar entries = %d", count)
	}
	resumed, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets", Region: "eu-west-1"})
	if err := New(client).captureObjects(context.Background(), "assets", &bucket, resumed, opts); err != nil {
		t.Fatal(err)
	}
	if client.gets != 2 || resumed.Dataset == nil || !resumed.Dataset.Resumed {
		t.Fatalf("resume redownloaded objects or omitted state: gets=%d dataset=%#v", client.gets, resumed.Dataset)
	}
}

type governedDataClient struct {
	Client
	body        string
	contentType string
	metadata    map[string]string
}

type governedLargeDataClient struct {
	Client
	size int64
	body []byte
	gets int
}

func (c *governedLargeDataClient) ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	return &awss3.ListObjectsV2Output{Contents: []types.Object{{Key: aws.String("large.txt"), ETag: aws.String("etag"), Size: aws.Int64(c.size)}}}, nil
}

func (c *governedLargeDataClient) GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	c.gets++
	if c.body != nil {
		return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(c.body)), ContentType: aws.String("text/plain")}, nil
	}
	return &awss3.GetObjectOutput{Body: struct {
		io.Reader
		io.Closer
	}{Reader: &boundedZeroReader{remaining: c.size, maxRead: 32 << 10}, Closer: io.NopCloser(nilReader{})}, ContentType: aws.String("text/plain")}, nil
}

func (*governedLargeDataClient) GetObjectTagging(context.Context, *awss3.GetObjectTaggingInput, ...func(*awss3.Options)) (*awss3.GetObjectTaggingOutput, error) {
	return &awss3.GetObjectTaggingOutput{}, nil
}

type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }

type boundedZeroReader struct {
	remaining int64
	maxRead   int
}

func (r *boundedZeroReader) Read(p []byte) (int, error) {
	if len(p) > r.maxRead {
		return 0, errors.New("unbounded source read")
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	clear(p)
	r.remaining -= int64(len(p))
	return len(p), nil
}

func (c governedDataClient) ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	return &awss3.ListObjectsV2Output{Contents: []types.Object{{Key: aws.String("customer.txt"), ETag: aws.String("etag"), Size: aws.Int64(int64(len(c.body)))}}}, nil
}

func (c governedDataClient) GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	return &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(c.body)), ContentType: aws.String(c.contentType), Metadata: c.metadata}, nil
}

func (governedDataClient) GetObjectTagging(context.Context, *awss3.GetObjectTaggingInput, ...func(*awss3.Options)) (*awss3.GetObjectTaggingOutput, error) {
	return &awss3.GetObjectTaggingOutput{}, nil
}

func TestGovernedS3CaptureTransformsMetadataAndTextBodyBeforePersistence(t *testing.T) {
	root := t.TempDir()
	policy, err := governance.NewEffectivePolicy("share-safe", []governance.Rule{
		{ID: "body-001", Service: governance.ServiceS3, Resource: "assets", Target: governance.Target{Kind: governance.TargetS3TextBody}, Action: governance.ActionReplace, Replacement: "safe body", ContentTypes: []string{"text/plain"}},
		{ID: "meta-001", Service: governance.ServiceS3, Resource: "assets", Target: governance.Target{Kind: governance.TargetS3Metadata, Path: "owner"}, Action: governance.ActionReplace, Replacement: "safe owner"},
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets", Region: "eu-west-1"})
	bucket := Bucket{Name: "assets", Region: "eu-west-1"}
	opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint"), Governance: policy}
	client := governedDataClient{body: "protected body", contentType: "text/plain; charset=utf-8", metadata: map[string]string{"owner": "protected owner", "purpose": "testing"}}
	if err := New(client).captureObjects(context.Background(), "assets", &bucket, snapshot, opts); err != nil {
		t.Fatal(err)
	}

	body, object := readOnlyPackedObject(t, opts.ArtifactDirectory, snapshot.Dataset.Chunks[0])
	if string(body) != "safe body" {
		t.Fatalf("body = %q", body)
	}
	if object.Key != "customer.txt" || object.Metadata["owner"] != "safe owner" || object.Metadata["purpose"] != "testing" {
		t.Fatalf("object metadata = %#v", object)
	}
	if object.Size != int64(len(body)) || object.SHA256 != sha256Hex(body) {
		t.Fatalf("object digest/size describes source rather than transformed body: %#v", object)
	}
	for _, protected := range []string{"protected body", "protected owner"} {
		assertAbsentFromFiles(t, root, protected)
	}
}

func TestGovernedS3ResumeRestoresAuditForSkippedCompletedPacks(t *testing.T) {
	policy, err := governance.NewEffectivePolicy("share-safe", []governance.Rule{
		{ID: "body-001", Service: governance.ServiceS3, Resource: "assets", Target: governance.Target{Kind: governance.TargetS3TextBody}, Action: governance.ActionReplace, Replacement: "safe body", ContentTypes: []string{"text/plain"}},
		{ID: "meta-001", Service: governance.ServiceS3, Resource: "assets", Target: governance.Target{Kind: governance.TargetS3Metadata, Path: "owner"}, Action: governance.ActionReplace, Replacement: "safe owner"},
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	client := governedDataClient{body: "protected body", contentType: "text/plain", metadata: map[string]string{"owner": "protected owner"}}
	opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint"), Governance: policy, GovernanceAudit: governance.NewAudit()}
	snapshot, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets", Region: "eu-west-1"})
	bucket := Bucket{Name: "assets", Region: "eu-west-1"}
	if err := New(client).captureObjects(context.Background(), "assets", &bucket, snapshot, opts); err != nil {
		t.Fatal(err)
	}
	opts.GovernanceAudit = governance.NewAudit()
	resumed, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets", Region: "eu-west-1"})
	if err := New(client).captureObjects(context.Background(), "assets", &bucket, resumed, opts); err != nil {
		t.Fatal(err)
	}
	counts := opts.GovernanceAudit.RuleCounts()
	if counts["body-001"] != 1 || counts["meta-001"] != 1 {
		t.Fatalf("resumed exact internal counts = %#v, want completed pack audit", counts)
	}
	checkpoint, err := os.ReadFile(filepath.Join(opts.CheckpointDirectory, "checkpoint.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(checkpoint, []byte("protected body")) || bytes.Contains(checkpoint, []byte("protected owner")) {
		t.Fatalf("checkpoint leaked protected source: %s", checkpoint)
	}
}

func TestGovernedS3CaptureRejectsUnlistedContentTypeBeforeChunkCommit(t *testing.T) {
	root := t.TempDir()
	policy, err := governance.NewEffectivePolicy("share-safe", []governance.Rule{{
		ID: "body-001", Service: governance.ServiceS3, Resource: "assets", Target: governance.Target{Kind: governance.TargetS3TextBody}, Action: governance.ActionReplace, Replacement: "safe", ContentTypes: []string{"text/plain"},
	}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets"})
	opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint"), Governance: policy}
	client := governedDataClient{body: "protected body", contentType: "application/octet-stream"}
	err = New(client).captureObjects(context.Background(), "assets", &Bucket{Name: "assets"}, snapshot, opts)
	if err == nil || !strings.Contains(err.Error(), "not allowed") || !strings.Contains(err.Error(), "body-001") {
		t.Fatalf("error = %v", err)
	}
	var chunks []string
	err = filepath.Walk(opts.ArtifactDirectory, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			chunks = append(chunks, path)
		}
		return err
	})
	if err != nil || len(chunks) != 0 {
		t.Fatalf("durable chunks after rejection = %v, err = %v", chunks, err)
	}
	assertAbsentFromFiles(t, root, "protected body")
}

func TestGovernedS3CaptureCheckpointIncludesPolicyIdentity(t *testing.T) {
	root := t.TempDir()
	newPolicy := func(replacement string) *governance.EffectivePolicy {
		policy, err := governance.NewEffectivePolicy("share-safe", []governance.Rule{{
			ID: "body-001", Service: governance.ServiceS3, Resource: "assets", Target: governance.Target{Kind: governance.TargetS3TextBody}, Action: governance.ActionReplace, Replacement: replacement, ContentTypes: []string{"text/plain"},
		}}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return policy
	}
	newSnapshot := func() *model.Snapshot {
		snapshot, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets"})
		return snapshot
	}
	client := governedDataClient{body: "protected", contentType: "text/plain"}
	opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint"), Governance: newPolicy("safe")}
	if err := New(client).captureObjects(context.Background(), "assets", &Bucket{Name: "assets"}, newSnapshot(), opts); err != nil {
		t.Fatal(err)
	}
	equivalent := opts
	equivalent.Governance = newPolicy("safe")
	resumed := newSnapshot()
	if err := New(client).captureObjects(context.Background(), "assets", &Bucket{Name: "assets"}, resumed, equivalent); err != nil || resumed.Dataset == nil || !resumed.Dataset.Resumed {
		t.Fatalf("equivalent policy did not resume: dataset=%#v err=%v", resumed.Dataset, err)
	}
	changed := opts
	changed.Governance = newPolicy("different safe value")
	err := New(client).captureObjects(context.Background(), "assets", &Bucket{Name: "assets"}, newSnapshot(), changed)
	if err == nil || !strings.Contains(err.Error(), "incompatible S3 capture checkpoint") {
		t.Fatalf("changed-policy error = %v", err)
	}
}

func TestGovernedS3CaptureScansCredentialsAfterBodyTransformation(t *testing.T) {
	const credential = "AKIAIOSFODNN7EXAMPLE"
	newPolicy := func(replacement string) *governance.EffectivePolicy {
		policy, err := governance.NewEffectivePolicy("share-safe", []governance.Rule{{
			ID: "body-001", Service: governance.ServiceS3, Resource: "assets", Target: governance.Target{Kind: governance.TargetS3TextBody}, Action: governance.ActionReplace, Replacement: replacement, ContentTypes: []string{"text/plain"},
		}}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return policy
	}
	run := func(body, replacement string) error {
		root := t.TempDir()
		snapshot, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets"})
		opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint"), Governance: newPolicy(replacement)}
		return New(governedDataClient{body: body, contentType: "text/plain"}).captureObjects(context.Background(), "assets", &Bucket{Name: "assets"}, snapshot, opts)
	}
	if err := run(credential, "safe"); err != nil {
		t.Fatalf("source credential was scanned before replacement: %v", err)
	}
	if err := run("protected", credential); err == nil || !strings.Contains(err.Error(), "credential") {
		t.Fatalf("replacement credential error = %v", err)
	}
}

func TestGovernedS3CaptureSupportsEveryWholeBodyAction(t *testing.T) {
	secret := bytes.Repeat([]byte("s"), 32)
	for _, action := range []governance.Action{governance.ActionOmit, governance.ActionHash, governance.ActionPseudonymize} {
		t.Run(string(action), func(t *testing.T) {
			rule := governance.Rule{ID: "body-001", Service: governance.ServiceS3, Resource: "assets", Target: governance.Target{Kind: governance.TargetS3TextBody}, Action: action, ContentTypes: []string{"text/plain"}}
			if action == governance.ActionPseudonymize {
				rule.KeyID = "key-001"
				rule.Algorithm = governance.PseudonymAlgorithm
			}
			policy, err := governance.NewEffectivePolicy("share-safe", []governance.Rule{rule}, nil, secret)
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			snapshot, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets"})
			opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint"), Governance: policy}
			if err := New(governedDataClient{body: "protected", contentType: "text/plain"}).captureObjects(context.Background(), "assets", &Bucket{Name: "assets"}, snapshot, opts); err != nil {
				t.Fatal(err)
			}
			body, object := readOnlyPackedObject(t, opts.ArtifactDirectory, snapshot.Dataset.Chunks[0])
			want, err := governance.NewEngine(policy.Profile, policy.Secret()).Apply(policy.Rules[0], []byte("protected"))
			if err != nil {
				t.Fatal(err)
			}
			if want.Omit {
				want.Value = []byte{}
			}
			if !bytes.Equal(body, want.Value) || object.Size != int64(len(want.Value)) || object.SHA256 != sha256Hex(want.Value) {
				t.Fatalf("body/object = %q/%#v, want %q", body, object, want.Value)
			}
		})
	}
}

func TestGovernedS3CaptureStreamsLargeBodyWithBoundedReads(t *testing.T) {
	const sourceSize = int64(32 << 20)
	policy, err := governance.NewEffectivePolicy("share-safe", []governance.Rule{{
		ID: "body-001", Service: governance.ServiceS3, Resource: "assets", Target: governance.Target{Kind: governance.TargetS3TextBody}, Action: governance.ActionHash, ContentTypes: []string{"text/plain"},
	}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	snapshot, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets"})
	opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint"), Governance: policy}
	if err := New(&governedLargeDataClient{size: sourceSize}).captureObjects(context.Background(), "assets", &Bucket{Name: "assets"}, snapshot, opts); err != nil {
		t.Fatal(err)
	}
	body, object := readOnlyPackedObject(t, opts.ArtifactDirectory, snapshot.Dataset.Chunks[0])
	if !bytes.HasPrefix(body, []byte(governance.HashAlgorithm+":")) || object.Size != int64(len(body)) || snapshot.Dataset.SourceBytes != sourceSize {
		t.Fatalf("body=%q object=%#v dataset=%#v", body, object, snapshot.Dataset)
	}
}

func TestGovernedS3DiskPreflightUsesTransformedOutputWithoutReadingBodies(t *testing.T) {
	const sourceSize = int64(8 << 20)
	const available = int64(6 << 20)
	secret := bytes.Repeat([]byte("s"), 32)
	for _, tc := range []struct {
		name        string
		action      governance.Action
		replacement string
	}{
		{name: "omit", action: governance.ActionOmit},
		{name: "hash", action: governance.ActionHash},
		{name: "pseudonymize", action: governance.ActionPseudonymize},
		{name: "replace", action: governance.ActionReplace, replacement: "safe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule := governance.Rule{ID: "body-001", Service: governance.ServiceS3, Resource: "assets", Target: governance.Target{Kind: governance.TargetS3TextBody}, Action: tc.action, Replacement: tc.replacement, ContentTypes: []string{"text/plain"}}
			if tc.action == governance.ActionPseudonymize {
				rule.KeyID, rule.Algorithm = "key-001", governance.PseudonymAlgorithm
			}
			policy, err := governance.NewEffectivePolicy("share-safe", []governance.Rule{rule}, nil, secret)
			if err != nil {
				t.Fatal(err)
			}
			client := &governedLargeDataClient{size: sourceSize, body: make([]byte, sourceSize)}
			root := t.TempDir()
			snapshot, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets"})
			opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint"), Governance: policy}
			original := requireS3Available
			requireS3Available = func(_ string, payload, copies int64) error {
				if copies != 1 || payload > available {
					return &storage.InsufficientSpaceError{Required: payload, Available: available}
				}
				return nil
			}
			t.Cleanup(func() { requireS3Available = original })
			if err := New(client).captureObjects(context.Background(), "assets", &Bucket{Name: "assets"}, snapshot, opts); err != nil {
				t.Fatalf("capture rejected transformed output: %v", err)
			}
			if client.gets != 1 {
				t.Fatalf("GetObject calls = %d, want one after preflight", client.gets)
			}
		})
	}
}

func TestUngovernedS3DiskPreflightRejectsSourceSizeBeforeReadingBodies(t *testing.T) {
	const sourceSize = int64(8 << 20)
	const available = int64(6 << 20)
	client := &governedLargeDataClient{size: sourceSize, body: make([]byte, sourceSize)}
	root := t.TempDir()
	snapshot, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets"})
	opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	original := requireS3Available
	requireS3Available = func(_ string, payload, _ int64) error {
		if payload > available {
			return &storage.InsufficientSpaceError{Required: payload, Available: available}
		}
		return nil
	}
	t.Cleanup(func() { requireS3Available = original })
	err := New(client).captureObjects(context.Background(), "assets", &Bucket{Name: "assets"}, snapshot, opts)
	if err == nil || !strings.Contains(err.Error(), "insufficient disk space") {
		t.Fatalf("error = %v, want insufficient disk space", err)
	}
	if client.gets != 0 {
		t.Fatalf("preflight read %d object bodies", client.gets)
	}
}

func TestS3DiskPreflightExcludesCompletedPacks(t *testing.T) {
	first, err := json.Marshal(inventoryEntry{Key: "complete", Size: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(inventoryEntry{Key: "remaining", Size: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	inventory := append(append(append([]byte{}, first...), '\n'), second...)
	inventory = append(inventory, '\n')
	path := filepath.Join(t.TempDir(), "inventory.ndjson")
	if err := os.WriteFile(path, inventory, 0o600); err != nil {
		t.Fatal(err)
	}

	remaining, err := estimateS3RemainingArtifactBytes(path, int64(len(first)+1), nil)
	if err != nil {
		t.Fatal(err)
	}
	all, err := estimateS3RemainingArtifactBytes(path, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	complete, err := estimateS3RemainingArtifactBytes(path, int64(len(inventory)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if remaining >= all || complete != 0 {
		t.Fatalf("remaining/all/complete estimates = %d/%d/%d", remaining, all, complete)
	}
}

func readOnlyPackedObject(t *testing.T, artifactRoot string, chunk model.DataChunk) ([]byte, Object) {
	t.Helper()
	pack, err := os.Open(filepath.Join(artifactRoot, filepath.FromSlash(chunk.Data.Path)))
	if err != nil {
		t.Fatal(err)
	}
	defer pack.Close()
	packGzip, err := gzip.NewReader(pack)
	if err != nil {
		t.Fatal(err)
	}
	tarReader := tar.NewReader(packGzip)
	if _, err = tarReader.Next(); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(tarReader)
	if err != nil {
		t.Fatal(err)
	}
	index, err := os.Open(filepath.Join(artifactRoot, filepath.FromSlash(chunk.Index.Path)))
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	indexGzip, err := gzip.NewReader(index)
	if err != nil {
		t.Fatal(err)
	}
	var object Object
	if err = json.NewDecoder(indexGzip).Decode(&object); err != nil {
		t.Fatal(err)
	}
	return body, object
}

func sha256Hex(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func assertAbsentFromFiles(t *testing.T, root, protected string) {
	t.Helper()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, []byte(protected)) {
			t.Errorf("protected value persisted in %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFullS3CaptureRejectsChangedCaptureOptions(t *testing.T) {
	root := t.TempDir()
	newSnapshot := func() *model.Snapshot {
		s, _ := model.NewSnapshot(model.ResourceRef{Service: "s3", ID: "assets"}, "s3", Bucket{Name: "assets", Region: "eu-west-1"})
		return s
	}
	bucket := Bucket{Name: "assets", Region: "eu-west-1"}
	fullOpts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint"), Overwrite: "if-different"}
	client := &packedDataClient{}
	if err := New(client).captureObjects(context.Background(), "assets", &bucket, newSnapshot(), fullOpts); err != nil {
		t.Fatal(err)
	}
	boundedOpts := model.CaptureOptions{Mode: "bounded", Limits: model.DataLimits{MaxObjects: 1, MaxObjectBytes: 1, MaxTotalBytes: 1}, ArtifactDirectory: fullOpts.ArtifactDirectory, CheckpointDirectory: fullOpts.CheckpointDirectory, Overwrite: "if-different"}
	err := New(client).captureObjects(context.Background(), "assets", &bucket, newSnapshot(), boundedOpts)
	if err == nil || !strings.Contains(err.Error(), "incompatible S3 capture checkpoint") {
		t.Fatalf("changed-options error = %v", err)
	}
}

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
