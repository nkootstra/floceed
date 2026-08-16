package bundle

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/nkootstra/floceed/internal/model"
)

func TestLoadGeneratedValidatesAndLoadsSupportedManifest(t *testing.T) {
	root := t.TempDir()
	manifest := model.Manifest{SchemaVersion: 1, Source: model.SourceMetadata{AccountID: "123456789012", Region: "eu-west-1"}}
	writeGeneratedFixture(t, root, manifest)

	got, err := LoadGenerated(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if got.Manifest.SchemaVersion != 1 || len(got.Checksums.Files) != 1 {
		t.Fatalf("LoadGenerated() = %#v", got)
	}
}

func TestLoadGeneratedSupportsEveryReadableManifestSchema(t *testing.T) {
	for schema := model.MinimumManifestSchemaVersion; schema <= model.CurrentManifestSchemaVersion; schema++ {
		t.Run(strconv.Itoa(schema), func(t *testing.T) {
			root := t.TempDir()
			writeGeneratedFixture(t, root, model.Manifest{SchemaVersion: schema})
			got, err := LoadGenerated(context.Background(), root)
			if err != nil || got.Manifest.SchemaVersion != schema {
				t.Fatalf("LoadGenerated() schema %d = %#v, %v", schema, got, err)
			}
		})
	}
}

func TestLoadGeneratedFailsClosedForInvalidArtifacts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string)
		want   string
	}{
		{"missing checksums", func(t *testing.T, root string) { os.Remove(filepath.Join(root, "checksums.json")) }, "checksums"},
		{"missing manifest", func(t *testing.T, root string) { os.Remove(filepath.Join(root, "bundle", "manifest.json")) }, "manifest"},
		{"checksum mismatch", func(t *testing.T, root string) {
			os.WriteFile(filepath.Join(root, "bundle", "manifest.json"), []byte("changed"), 0o600)
		}, "checksum mismatch"},
		{"unsafe checksum path", func(t *testing.T, root string) {
			b, _ := CanonicalJSON(Checksums{SchemaVersion: 1, Files: []Checksum{{Path: "../escape", SHA256: strings.Repeat("0", 64)}}})
			os.WriteFile(filepath.Join(root, "checksums.json"), b, 0o600)
		}, "unsafe bundle path"},
		{"unsupported checksum schema", func(t *testing.T, root string) {
			b, _ := CanonicalJSON(Checksums{SchemaVersion: 2})
			os.WriteFile(filepath.Join(root, "checksums.json"), b, 0o600)
		}, "unsupported checksum schema"},
		{"unsupported manifest schema", func(t *testing.T, root string) {
			writeGeneratedFixture(t, root, model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion + 1})
		}, "unsupported manifest schema"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			writeGeneratedFixture(t, root, model.Manifest{SchemaVersion: 1})
			test.mutate(t, root)
			if got, err := LoadGenerated(context.Background(), root); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadGenerated() = %#v, %v; want error containing %q", got, err, test.want)
			}
		})
	}
}

func TestLoadGeneratedRejectsSymlinkedRequiredFiles(t *testing.T) {
	for _, relative := range []string{"checksums.json", "bundle/manifest.json"} {
		t.Run(relative, func(t *testing.T) {
			root := t.TempDir()
			writeGeneratedFixture(t, root, model.Manifest{SchemaVersion: 1})
			target := filepath.Join(root, filepath.FromSlash(relative))
			contents, err := os.ReadFile(target)
			if err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(outside, contents, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(target); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, target); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadGenerated(context.Background(), root); err == nil || !strings.Contains(err.Error(), "symlink") {
				t.Fatalf("LoadGenerated() error = %v, want symlink rejection", err)
			}
		})
	}
}

func TestLoadGeneratedHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := LoadGenerated(ctx, t.TempDir())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadGenerated() error = %v, want context canceled", err)
	}
}

func TestLoadGeneratedRejectsFileMissingFromChecksumIndex(t *testing.T) {
	root := t.TempDir()
	writeGeneratedFixture(t, root, model.Manifest{SchemaVersion: 1})
	if err := os.WriteFile(filepath.Join(root, "unlisted.txt"), []byte("not covered"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadGenerated(context.Background(), root); err == nil {
		t.Fatal("LoadGenerated() accepted a file outside the checksum index")
	}
}

func TestLoadGeneratedRejectsManifestArtifactChecksumDisagreement(t *testing.T) {
	root := t.TempDir()
	content := []byte("fixture")
	manifest := model.Manifest{SchemaVersion: 1, Snapshots: []model.Snapshot{{
		Resource: model.ResourceRef{Service: "s3", Type: "bucket", ID: "assets"}, Service: "s3", StructureVersion: 1,
		Structure: json.RawMessage(`{"name":"assets","region":"eu-west-1"}`),
		Data:      []model.ArtifactRef{{Path: "bundle/data/assets", SHA256: strings.Repeat("0", 64), Size: int64(len(content))}},
	}}}
	writeGeneratedFixture(t, root, manifest)
	if err := os.MkdirAll(filepath.Join(root, "bundle", "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bundle", "data", "assets"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	sums, err := BuildChecksums(root, "checksums.json")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := CanonicalJSON(sums)
	if err := os.WriteFile(filepath.Join(root, "checksums.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadGenerated(context.Background(), root); err == nil || !strings.Contains(err.Error(), "artifact checksum") {
		t.Fatalf("LoadGenerated() error = %v, want manifest artifact checksum rejection", err)
	}
}

func writeGeneratedFixture(t *testing.T, root string, manifest model.Manifest) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "bundle"), 0o700); err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "bundle", "manifest.json")
	if err := os.WriteFile(manifestPath, b, 0o600); err != nil {
		t.Fatal(err)
	}
	sum, err := SumFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	sum.Path = "bundle/manifest.json"
	checksums, err := json.Marshal(Checksums{SchemaVersion: 1, Files: []Checksum{sum}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "checksums.json"), checksums, 0o600); err != nil {
		t.Fatal(err)
	}
}
