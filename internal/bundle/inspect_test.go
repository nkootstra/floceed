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
	if got.Manifest.SchemaVersion != 1 || len(got.Checksums.Files) != len(requiredGeneratedFiles) {
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

func TestLoadGeneratedDistinguishesMissingRootFromCorruptBundle(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := LoadGenerated(context.Background(), missing); !errors.Is(err, ErrGeneratedRootMissing) {
		t.Fatalf("missing root error = %v, want ErrGeneratedRootMissing", err)
	}

	corrupt := t.TempDir()
	if _, err := LoadGenerated(context.Background(), corrupt); err == nil || errors.Is(err, ErrGeneratedRootMissing) {
		t.Fatalf("corrupt bundle error = %v, must not be ErrGeneratedRootMissing", err)
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

func TestLoadGeneratedRequiresEveryRuntimeFileEvenWhenIndexMatches(t *testing.T) {
	for _, relative := range requiredGeneratedFiles {
		t.Run(relative, func(t *testing.T) {
			root := t.TempDir()
			writeGeneratedFixture(t, root, model.Manifest{SchemaVersion: 1})
			if err := os.Remove(filepath.Join(root, filepath.FromSlash(relative))); err != nil {
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
			if _, err := LoadGenerated(context.Background(), root); err == nil || !strings.Contains(err.Error(), "required file") {
				t.Fatalf("LoadGenerated() error = %v, want missing required file", err)
			}
		})
	}
}

func TestLoadGeneratedRejectsOversizedMetadataBeforeReadingIt(t *testing.T) {
	root := t.TempDir()
	checksums := filepath.Join(root, "checksums.json")
	f, err := os.Create(checksums)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(maxChecksumsBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadGenerated(context.Background(), root); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("LoadGenerated() error = %v, want metadata size rejection", err)
	}
}

func TestLoadGeneratedRejectsOversizedSparseManifest(t *testing.T) {
	root := t.TempDir()
	writeGeneratedFixture(t, root, model.Manifest{SchemaVersion: 1})
	manifestPath := filepath.Join(root, "bundle", "manifest.json")
	if err := os.Truncate(manifestPath, maxManifestBytes+1); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "checksums.json"))
	if err != nil {
		t.Fatal(err)
	}
	var sums Checksums
	if err := json.Unmarshal(data, &sums); err != nil {
		t.Fatal(err)
	}
	for index := range sums.Files {
		if sums.Files[index].Path == "bundle/manifest.json" {
			sums.Files[index].Size = maxManifestBytes + 1
		}
	}
	data, _ = CanonicalJSON(sums)
	if err := os.WriteFile(filepath.Join(root, "checksums.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadGenerated(context.Background(), root); err == nil || !strings.Contains(err.Error(), "manifest exceeds") {
		t.Fatalf("LoadGenerated() error = %v, want manifest size rejection", err)
	}
}

func TestLoadGeneratedRejectsSymlinkedParentDirectory(t *testing.T) {
	root := t.TempDir()
	writeGeneratedFixture(t, root, model.Manifest{SchemaVersion: 1})
	runtimeDir := filepath.Join(root, "runtime")
	outside := t.TempDir()
	data, err := os.ReadFile(filepath.Join(runtimeDir, "replay.py"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "replay.py"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(runtimeDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, runtimeDir); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadGenerated(context.Background(), root); err == nil || !errors.Is(err, ErrGeneratedPath) {
		t.Fatalf("LoadGenerated() error = %v, want unsafe path rejection", err)
	}
}

func TestVerifyChecksumRejectsDeclaredSizeBeforeReadingBody(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "payload"), []byte("body"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := verifyChecksum(context.Background(), root, Checksum{Path: "payload", Size: 5}, make([]byte, 8))
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("verifyChecksum() error = %v, want size mismatch", err)
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
	for relative, content := range map[string][]byte{
		ComposeFile:                 []byte("services: {}\n"),
		"runtime/replay.py":         []byte("# replay\n"),
		"init/ready.d/10-replay.py": []byte("# ready\n"),
	} {
		filename := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(filename), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	sums, err := BuildChecksums(root, "checksums.json")
	if err != nil {
		t.Fatal(err)
	}
	checksums, err := json.Marshal(sums)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "checksums.json"), checksums, 0o600); err != nil {
		t.Fatal(err)
	}
}
