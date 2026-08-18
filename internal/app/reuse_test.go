package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/captureledger"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	inspection "github.com/nkootstra/floceed/internal/inspect"
	"github.com/nkootstra/floceed/internal/model"
)

type reusableTestAdapter struct {
	mu                 sync.Mutex
	freshCaptures      int
	candidateCalls     int
	failCorruptRefresh bool
	invalidReasons     []captureledger.Reason
	refresh            map[string]bool
	payloadSuffix      string
}

func (*reusableTestAdapter) Service() model.ServiceDescriptor {
	return model.ServiceDescriptor{Name: "s3"}
}
func (*reusableTestAdapter) Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error) {
	return model.DiscoveryResult{}, nil
}
func (*reusableTestAdapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	var result catalog.PlanContribution
	for _, resource := range project.Resources.S3 {
		result.Selections = append(result.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "s3", Type: "bucket", ID: resource.Name}})
	}
	return result
}
func (*reusableTestAdapter) Capture(context.Context, model.SourceScope, model.ResourceRef, model.CaptureOptions) (*model.Snapshot, error) {
	panic("reuse-aware orchestration must use CaptureReusable")
}
func (*reusableTestAdapter) Dependencies(*model.Snapshot) []model.Dependency { return nil }
func (*reusableTestAdapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding {
	return nil
}
func (*reusableTestAdapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}

func (a *reusableTestAdapter) CaptureReusable(_ context.Context, scope model.SourceScope, ref model.ResourceRef, options model.CaptureOptions, request catalog.ReuseRequest) (catalog.ReuseResult, error) {
	snapshot, err := model.NewSnapshot(ref, "s3", map[string]any{"name": ref.ID, "region": scope.Region})
	if err != nil {
		return catalog.ReuseResult{}, err
	}
	candidateInvalid := request.Candidate != nil && len(request.Candidate.Units) != 0 && request.Candidate.Units[0].Outcome == captureledger.UnitOutcomeInvalidated
	if request.Candidate != nil && request.Validate != nil {
		for _, artifact := range request.Candidate.Units[0].Artifacts {
			if err := request.Validate(artifact); err != nil {
				candidateInvalid = true
				if reason, ok := captureledger.InvalidationReason(err); ok {
					request.InvalidationReason = reason
				}
				break
			}
		}
	}
	if request.Candidate != nil && !candidateInvalid && !a.refresh[ref.ID] {
		a.mu.Lock()
		a.candidateCalls++
		a.mu.Unlock()
		candidate := *request.Candidate
		var refs []model.ArtifactRef
		for _, artifact := range candidate.Units[0].Artifacts {
			if err := request.Materialize(artifact); err != nil {
				return catalog.ReuseResult{}, err
			}
			refs = append(refs, model.ArtifactRef{Path: artifact.Path, SHA256: artifact.SHA256, Size: artifact.Size, MediaType: artifact.MediaType})
		}
		snapshot.Dataset = &model.Dataset{Format: "s3-tar-gzip-v1", Records: 1, SourceBytes: refs[0].Size, Consistency: "best_effort", Chunks: []model.DataChunk{{Data: refs[0], Index: &refs[1], Records: 1, SourceBytes: refs[0].Size}}}
		candidate.Units[0].Outcome = captureledger.UnitOutcomeReused
		candidate.Units[0].Reason = captureledger.ReasonReused
		return catalog.ReuseResult{Snapshot: snapshot, Resource: &candidate}, nil
	}
	reason := request.InvalidationReason
	if request.Candidate != nil && reason == "" {
		reason = captureledger.ReasonSourceContentChanged
	}
	a.mu.Lock()
	a.freshCaptures++
	a.invalidReasons = append(a.invalidReasons, reason)
	a.mu.Unlock()
	if a.failCorruptRefresh && request.InvalidationReason == captureledger.ReasonArtifactCorrupt {
		return catalog.ReuseResult{}, errors.New("fresh capture failed")
	}
	payload := []byte("payload-" + ref.ID + a.payloadSuffix)
	files := []struct {
		path, mediaType string
		payload         []byte
	}{
		{"bundle/data/s3/" + ref.ID + "/data.tar.gz", "application/gzip", payload},
		{"bundle/data/s3/" + ref.ID + "/index.json", "application/json", []byte(`{"entries":[]}`)},
	}
	var artifacts []captureledger.Artifact
	var refs []model.ArtifactRef
	for _, file := range files {
		filename := filepath.Join(options.ArtifactDirectory, filepath.FromSlash(file.path))
		if err := os.MkdirAll(filepath.Dir(filename), 0o700); err != nil {
			return catalog.ReuseResult{}, err
		}
		if err := os.WriteFile(filename, file.payload, 0o600); err != nil {
			return catalog.ReuseResult{}, err
		}
		sum := sha256.Sum256(file.payload)
		digest := hex.EncodeToString(sum[:])
		artifacts = append(artifacts, captureledger.Artifact{Path: file.path, SHA256: digest, Size: int64(len(file.payload)), MediaType: file.mediaType})
		refs = append(refs, model.ArtifactRef{Path: file.path, SHA256: digest, Size: int64(len(file.payload)), MediaType: file.mediaType})
	}
	digest := artifacts[0].SHA256
	snapshot.Dataset = &model.Dataset{Format: "s3-tar-gzip-v1", Records: 1, SourceBytes: int64(len(payload)), Consistency: "best_effort", Chunks: []model.DataChunk{{Data: refs[0], Index: &refs[1], Records: 1, SourceBytes: int64(len(payload))}}}
	resource := captureledger.Resource{
		Descriptor:        captureledger.ResourceDescriptor{Service: ref.Service, Type: ref.Type, ID: ref.ID},
		CaptureDefinition: digest,
		Units:             []captureledger.Unit{{ID: "unit", Freshness: captureledger.FreshnessEvidence{Kind: "test", Digest: digest}, Artifacts: artifacts, Outcome: captureledger.UnitOutcomeRefreshed, Reason: reason, CapturedAt: time.Unix(1, 0).UTC()}},
	}
	return catalog.ReuseResult{Snapshot: snapshot, Resource: &resource}, nil
}

func TestPullCombinesReusedAndFreshArtifactsInOneBundle(t *testing.T) {
	projectDir := t.TempDir()
	workDir := filepath.Join(projectDir, "work")
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	project.Resources.S3 = []config.S3Resource{{Name: "assets"}, {Name: "uploads"}}
	adapter := &reusableTestAdapter{}
	service := New("test")
	service.Factory = adapterFactory{adapter: adapter}
	service.ComposeValidator = func(context.Context, string) error { return nil }
	service.Now = func() time.Time { return time.Unix(10, 0).UTC() }
	if _, err := service.PullWithOptions(context.Background(), project, projectDir, "", "", PullOptions{WorkDir: workDir}); err != nil {
		t.Fatal(err)
	}
	adapter.refresh = map[string]bool{"uploads": true}
	result, err := service.PullWithOptions(context.Background(), project, projectDir, "", "", PullOptions{WorkDir: workDir})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.candidateCalls != 1 || adapter.freshCaptures != 3 {
		t.Fatalf("candidate calls = %d, fresh captures = %d; want 1, 3", adapter.candidateCalls, adapter.freshCaptures)
	}
	if len(result.Snapshots) != 2 {
		t.Fatalf("snapshots = %d, want 2", len(result.Snapshots))
	}
	if result.Receipt == nil || len(result.Receipt.Resources) != 2 {
		t.Fatalf("mixed reuse receipt = %#v", result.Receipt)
	}
	changes := make(map[string]inspection.ResourceChange, len(result.Receipt.Resources))
	for _, change := range result.Receipt.Resources {
		key := change.Resource.Service + "/" + change.Resource.ID
		changes[key] = change
	}
	if got := changes["s3/assets"].Units; len(got) != 1 || got[0].Outcome != "reused" || got[0].Reason != "reused" || got[0].Generation == "" || got[0].PreviousGeneration == "" {
		t.Fatalf("reused unit decisions = %#v", got)
	}
	if got := changes["s3/uploads"].Units; len(got) != 1 || got[0].Outcome != "refreshed" || got[0].Reason != "source_content_changed" {
		t.Fatalf("refreshed unit decisions = %#v", got)
	}
	firstJSON, err := json.Marshal(result.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(result.Receipt)
	if err != nil || !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("receipt serialization drifted: %q %q, %v", firstJSON, secondJSON, err)
	}
	root := filepath.Join(projectDir, project.Output.Directory)
	if err := bundle.ValidateGenerated(root); err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(root, "bundle/manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(manifest, []byte("ledger")) {
		t.Fatal("mixed bundle contains a ledger reference")
	}
	detachedWorkDir := filepath.Join(projectDir, "detached-work")
	if err := os.Rename(workDir, detachedWorkDir); err != nil {
		t.Fatalf("detach capture work and ledger: %v", err)
	}
	if err := bundle.ValidateGenerated(root); err != nil {
		t.Fatalf("mixed bundle depends on detached capture state: %v", err)
	}
	for _, snapshot := range result.Snapshots {
		if snapshot.Dataset == nil || len(snapshot.Dataset.Chunks) != 1 {
			t.Fatalf("detached snapshot dataset = %#v", snapshot.Dataset)
		}
		chunk := snapshot.Dataset.Chunks[0]
		if chunk.Index == nil {
			t.Fatalf("detached snapshot chunk has no index: %#v", chunk)
		}
		for _, ref := range []model.ArtifactRef{chunk.Data, *chunk.Index} {
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(ref.Path))); err != nil {
				t.Fatalf("standalone artifact %q: %v", ref.Path, err)
			}
		}
	}
}

func TestPullReusesVerifiedCandidateIntoCompleteStandaloneBundle(t *testing.T) {
	projectDir := t.TempDir()
	workDir := filepath.Join(projectDir, "work")
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	project.Resources.S3 = []config.S3Resource{{Name: "assets"}}
	adapter := &reusableTestAdapter{}
	service := New("test")
	service.Factory = adapterFactory{adapter: adapter}
	service.ComposeValidator = func(context.Context, string) error { return nil }
	service.Now = func() time.Time { return time.Unix(10, 0).UTC() }

	if _, err := service.PullWithOptions(context.Background(), project, projectDir, "", "", PullOptions{WorkDir: workDir}); err != nil {
		t.Fatal(err)
	}
	restarted, err := service.PullWithOptions(context.Background(), project, projectDir, "", "", PullOptions{WorkDir: workDir, Restart: true})
	if err != nil {
		t.Fatal(err)
	}
	serialized, err := json.Marshal(restarted)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(serialized, []byte(`"outcome":"reused"`)) || bytes.Contains(bytes.ToLower(serialized), []byte("ledger cleared")) {
		t.Fatalf("restart receipt = %s", serialized)
	}
	if adapter.freshCaptures != 1 || adapter.candidateCalls != 1 {
		t.Fatalf("fresh captures = %d, candidate calls = %d; want 1, 1", adapter.freshCaptures, adapter.candidateCalls)
	}
	target := filepath.Join(projectDir, project.Output.Directory)
	if err := bundle.ValidateGenerated(target); err != nil {
		t.Fatalf("reused bundle is invalid: %v", err)
	}
	payload, err := os.ReadFile(filepath.Join(target, "bundle/data/s3/assets/data.tar.gz"))
	if err != nil || string(payload) != "payload-assets" {
		t.Fatalf("materialized payload = %q, %v", payload, err)
	}
	manifest, err := os.ReadFile(filepath.Join(target, "bundle/manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(manifest, []byte("ledger")) {
		t.Fatal("manifest unexpectedly depends on ledger")
	}
}

func TestPullRejectsCorruptCandidateAndPreservesBundleAndLedgerOnRefreshFailure(t *testing.T) {
	projectDir := t.TempDir()
	workDir := filepath.Join(projectDir, "work")
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	project.Resources.S3 = []config.S3Resource{{Name: "assets"}}
	adapter := &reusableTestAdapter{}
	service := New("test")
	service.Factory = adapterFactory{adapter: adapter}
	service.ComposeValidator = func(context.Context, string) error { return nil }
	service.Now = func() time.Time { return time.Unix(10, 0).UTC() }
	if _, err := service.PullWithOptions(context.Background(), project, projectDir, "", "", PullOptions{WorkDir: workDir}); err != nil {
		t.Fatal(err)
	}
	targetManifest := filepath.Join(projectDir, project.Output.Directory, "bundle/manifest.json")
	beforeBundle, err := os.ReadFile(targetManifest)
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := filepath.Glob(filepath.Join(workDir, "ledger/generations/*/*/*/*/current.json"))
	if err != nil || len(indexes) != 1 {
		t.Fatalf("ledger indexes = %v, %v", indexes, err)
	}
	beforeIndex, err := os.ReadFile(indexes[0])
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := filepath.Glob(filepath.Join(workDir, "ledger/blobs/*/*"))
	if err != nil || len(blobs) == 0 {
		t.Fatalf("ledger blobs = %v, %v", blobs, err)
	}
	if err := os.Remove(blobs[0]); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blobs[0], []byte("corrupt"), 0o400); err != nil {
		t.Fatal(err)
	}
	adapter.failCorruptRefresh = true

	if _, err := service.PullWithOptions(context.Background(), project, projectDir, "", "", PullOptions{WorkDir: workDir}); err == nil {
		t.Fatal("pull succeeded despite corrupt candidate and failed refresh")
	}
	afterBundle, err := os.ReadFile(targetManifest)
	if err != nil {
		t.Fatal(err)
	}
	afterIndex, err := os.ReadFile(indexes[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeBundle, afterBundle) {
		t.Fatal("failed refresh changed installed bundle")
	}
	if !bytes.Equal(beforeIndex, afterIndex) {
		t.Fatal("failed refresh changed ledger generation")
	}
	if got := adapter.invalidReasons[len(adapter.invalidReasons)-1]; got != captureledger.ReasonArtifactCorrupt {
		t.Fatalf("invalidation reason = %q, want artifact_corrupt", got)
	}
	adapter.failCorruptRefresh = false
	adapter.payloadSuffix = "-refreshed"
	result, err := service.PullWithOptions(context.Background(), project, projectDir, "", "", PullOptions{WorkDir: workDir})
	if err != nil {
		t.Fatal(err)
	}
	serialized, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(serialized, []byte(`"reason":"artifact_corrupt"`)) {
		t.Fatalf("successful fallback did not explain corruption: %s", serialized)
	}
	for _, forbidden := range [][]byte{[]byte(workDir), []byte(blobs[0]), []byte("payload-assets")} {
		if bytes.Contains(serialized, forbidden) {
			t.Fatalf("receipt disclosed private capture data %q: %s", forbidden, serialized)
		}
	}
}

func TestPullPublishesLedgerOnlyAfterSuccessfulInstall(t *testing.T) {
	projectDir := t.TempDir()
	workDir := filepath.Join(projectDir, "work")
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	project.Resources.S3 = []config.S3Resource{{Name: "assets"}, {Name: "uploads"}}
	adapter := &reusableTestAdapter{}
	service := New("test")
	service.Factory = adapterFactory{adapter: adapter}
	service.ComposeValidator = func(context.Context, string) error { return nil }
	service.Now = func() time.Time { return time.Unix(10, 0).UTC() }
	if _, err := service.PullWithOptions(context.Background(), project, projectDir, "", "", PullOptions{WorkDir: workDir}); err != nil {
		t.Fatal(err)
	}
	indexes, err := filepath.Glob(filepath.Join(workDir, "ledger/generations/*/*/*/*/current.json"))
	if err != nil || len(indexes) != 2 {
		t.Fatalf("ledger indexes = %v, %v", indexes, err)
	}
	beforeIndexes := make(map[string][]byte, len(indexes))
	for _, index := range indexes {
		beforeIndexes[index], err = os.ReadFile(index)
		if err != nil {
			t.Fatal(err)
		}
	}
	target := filepath.Join(projectDir, project.Output.Directory)
	beforeManifest, err := os.ReadFile(filepath.Join(target, "bundle/manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	adapter.refresh = map[string]bool{"uploads": true}
	service.Now = func() time.Time { return time.Unix(20, 0).UTC() }
	service.ComposeValidator = func(context.Context, string) error { return errors.New("render failed") }
	if _, err := service.PullWithOptions(context.Background(), project, projectDir, "", "", PullOptions{WorkDir: workDir}); err == nil {
		t.Fatal("pull succeeded despite render failure")
	}
	if adapter.candidateCalls != 1 {
		t.Fatalf("reused candidate calls = %d, want 1", adapter.candidateCalls)
	}
	for _, index := range indexes {
		after, readErr := os.ReadFile(index)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !bytes.Equal(beforeIndexes[index], after) {
			t.Fatalf("failed mixed render published ledger generation %q", index)
		}
	}
	afterManifest, err := os.ReadFile(filepath.Join(target, "bundle/manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeManifest, afterManifest) {
		t.Fatal("failed mixed render changed the installed bundle")
	}
	if err := bundle.ValidateGenerated(target); err != nil {
		t.Fatalf("prior bundle is not replayable after failed mixed render: %v", err)
	}
}

func TestPullSucceedsWhenReuseCachePublicationFailsAfterInstall(t *testing.T) {
	projectDir := t.TempDir()
	workDir := filepath.Join(projectDir, "work")
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	project.Resources.S3 = []config.S3Resource{{Name: "assets"}}
	adapter := &reusableTestAdapter{}
	service := New("test")
	service.Factory = adapterFactory{adapter: adapter}
	service.ComposeValidator = func(context.Context, string) error { return nil }
	if _, err := service.PullWithOptions(context.Background(), project, projectDir, "", "", PullOptions{WorkDir: workDir}); err != nil {
		t.Fatal(err)
	}
	index, err := filepath.Glob(filepath.Join(workDir, "ledger/generations/*/*/*/*/current.json"))
	if err != nil || len(index) != 1 {
		t.Fatalf("ledger indexes = %v, %v", index, err)
	}
	beforeIndex, _ := os.ReadFile(index[0])
	beforeManifest, _ := os.ReadFile(filepath.Join(projectDir, project.Output.Directory, "bundle/manifest.json"))
	adapter.refresh = map[string]bool{"assets": true}
	adapter.payloadSuffix = "-new"
	service.publishLedger = func(*captureledger.Store, captureledger.Generation, string) error {
		return errors.New("cache unavailable")
	}
	var events []model.ProgressEvent
	result, err := service.PullWithOptions(context.Background(), project, projectDir, "", "", PullOptions{WorkDir: workDir, Progress: func(event model.ProgressEvent) { events = append(events, event) }})
	if err != nil {
		t.Fatalf("valid installed bundle reported failure: %v", err)
	}
	afterManifest, _ := os.ReadFile(filepath.Join(projectDir, project.Output.Directory, "bundle/manifest.json"))
	if bytes.Equal(beforeManifest, afterManifest) || result.Receipt == nil {
		t.Fatal("new bundle was not installed with a receipt")
	}
	afterIndex, _ := os.ReadFile(index[0])
	if !bytes.Equal(beforeIndex, afterIndex) {
		t.Fatal("failed cache publication changed the prior ledger index")
	}
	foundWarning := false
	for _, event := range events {
		foundWarning = foundWarning || event.Phase == "ledger"
	}
	if !foundWarning {
		t.Fatal("cache publication failure was not reported in progress")
	}
}
