package bundle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func TestLinkOrCopyFileFallsBackToCopyWhenLinkFails(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "nested", "destination")
	payload := []byte("artifact")
	if err := os.WriteFile(source, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := linkOrCopyFileWith(destination, source, func(string, string) error { return errors.New("cross-device link") }); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("copied artifact = %q, %v", got, err)
	}
}

func TestRenderIsDeterministicAndComplete(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, ".floceed")
	p := config.Project{SchemaVersion: 1, Source: config.Source{Region: "eu-west-1"}, Target: config.Target{FlociVersion: "1.6.0", Port: 4566, HookTimeoutSeconds: 300}}
	m := model.Manifest{SchemaVersion: 1, Tool: model.ToolMetadata{Version: "test"}, Target: model.TargetMetadata{FlociVersion: "1.6.0"}, Source: model.SourceMetadata{AccountID: "123456789012", Region: "eu-west-1"}, Capture: model.CaptureMetadata{CapturedAt: time.Unix(1, 0)}}
	validate := func(context.Context, string) error { return nil }
	if err := Render(context.Background(), target, p, m, RenderOptions{ValidateCompose: validate}); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(filepath.Join(target, "checksums.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Render(context.Background(), target, p, m, RenderOptions{ValidateCompose: validate}); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(filepath.Join(target, "checksums.json"))
	if string(first) != string(second) {
		t.Fatal("checksums changed")
	}
	for _, name := range []string{ComposeFile, "bundle/manifest.json", "runtime/replay.py", "init/ready.d/10-replay.py", ".gitignore"} {
		if _, err := os.Stat(filepath.Join(target, filepath.FromSlash(name))); err != nil {
			t.Errorf("missing %s: %v", name, err)
		}
	}
}

func TestRenderRejectsCredentialLeakBeforeCutover(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, ".floceed")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "old"), []byte("valid"), 0600); err != nil {
		t.Fatal(err)
	}
	p := config.Project{SchemaVersion: 1, Source: config.Source{Region: "x"}, Target: config.Target{FlociVersion: "1.6.0", Port: 4566, HookTimeoutSeconds: 300}}
	m := model.Manifest{SchemaVersion: 1, Tool: model.ToolMetadata{Version: "AKIAABCDEFGHIJKLMNOP"}, Target: model.TargetMetadata{FlociVersion: "1.6.0"}, Source: model.SourceMetadata{AccountID: "123456789012", Region: "x"}}
	err := Render(context.Background(), target, p, m, RenderOptions{ValidateCompose: func(context.Context, string) error { return nil }})
	if err == nil {
		t.Fatal("expected leak rejection")
	}
	if _, err := os.Stat(filepath.Join(target, "old")); err != nil {
		t.Fatal("old bundle lost")
	}
}

func TestCredentialPatternsRejectCredentialProcessJSON(t *testing.T) {
	data := []byte(`{"AccessKeyId":"ASIAABCDEFGHIJKLMNOP","SecretAccessKey":"source-secret-value-long","SessionToken":"source-session-token-long"}`)
	for _, pattern := range credentialPatterns {
		if pattern.Match(data) {
			return
		}
	}
	t.Fatal("credential_process JSON was not detected")
}

func TestRenderCopiesManifestV2DatasetArtifacts(t *testing.T) {
	root := t.TempDir()
	artRoot := filepath.Join(root, "capture")
	rel := "bundle/data/dynamodb/table/part-000001.ndjson"
	if err := os.MkdirAll(filepath.Dir(filepath.Join(artRoot, rel)), 0o700); err != nil {
		t.Fatal(err)
	}
	payload := []byte("{\"pk\":{\"S\":\"1\"}}\n")
	if err := os.WriteFile(filepath.Join(artRoot, rel), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	sum, err := SumFile(filepath.Join(artRoot, rel))
	if err != nil {
		t.Fatal(err)
	}
	sum.Path = rel
	sumRef := model.ArtifactRef{Path: rel, SHA256: sum.SHA256, Size: sum.Size, MediaType: "application/x-ndjson"}
	snapshot, err := model.NewSnapshot(model.ResourceRef{Service: "dynamodb", Type: "table", ID: "orders"}, "dynamodb", map[string]any{"name": "orders", "attribute_definitions": []any{}, "key_schema": []any{map[string]any{"name": "pk"}}, "billing_mode": "PAY_PER_REQUEST"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Dataset = &model.Dataset{Format: "dynamodb-ndjson-v1", Records: 1, SourceBytes: int64(len(payload)), Consistency: "best_effort", Chunks: []model.DataChunk{{Data: sumRef, Records: 1, SourceBytes: int64(len(payload))}}}
	manifest := model.Manifest{SchemaVersion: 2, Source: model.SourceMetadata{AccountID: "123456789012", Region: "eu-west-1"}, Snapshots: []model.Snapshot{*snapshot}}
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	target := filepath.Join(root, ".floceed")
	if err := Render(context.Background(), target, project, manifest, RenderOptions{ArtifactRoot: artRoot, ValidateCompose: func(context.Context, string) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(target, rel))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("artifact = %q", got)
	}
}
