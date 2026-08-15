package bundle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

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
	for _, name := range []string{ComposeFile, "bundle/manifest.json", "runtime/replay.py", "init/ready.d/10-base-resources.py", "init/ready.d/30-resource-links.py", "init/ready.d/60-seed-data.py", ".gitignore"} {
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
