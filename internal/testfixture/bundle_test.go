package testfixture

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/nkootstra/floceed/internal/bundle"
	inspection "github.com/nkootstra/floceed/internal/inspect"
	"github.com/nkootstra/floceed/internal/model"
)

func TestInspectFixturesAreDeterministicAndReadable(t *testing.T) {
	first, second := t.TempDir(), t.TempDir()
	if err := GenerateInspectFixtures(first); err != nil {
		t.Fatal(err)
	}
	if err := GenerateInspectFixtures(second); err != nil {
		t.Fatal(err)
	}
	compareTrees(t, first, second)

	for _, name := range []string{"baseline", "current", "governance-current"} {
		generated, err := bundle.LoadGenerated(context.Background(), filepath.Join(first, name, ".floceed"))
		if err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
		if generated.Manifest.SchemaVersion != model.CurrentManifestSchemaVersion {
			t.Fatalf("%s schema = %d", name, generated.Manifest.SchemaVersion)
		}
	}
}

func TestCommittedInspectFixturesMatchGenerator(t *testing.T) {
	generated := t.TempDir()
	if err := GenerateInspectFixtures(generated); err != nil {
		t.Fatal(err)
	}
	compareTrees(t, filepath.Join("..", "cli", "testdata", "inspect"), generated)
}

func TestSupportedSchemaFixturesAreReadable(t *testing.T) {
	for schema := model.MinimumManifestSchemaVersion; schema <= model.CurrentManifestSchemaVersion; schema++ {
		root := t.TempDir()
		if err := GenerateSchemaFixture(root, schema); err != nil {
			t.Fatalf("generate schema %d: %v", schema, err)
		}
		got, err := bundle.LoadGenerated(context.Background(), root)
		if err != nil {
			t.Fatalf("load schema %d: %v", schema, err)
		}
		if got.Manifest.SchemaVersion != schema {
			t.Fatalf("schema = %d, want %d", got.Manifest.SchemaVersion, schema)
		}
		projection, err := inspection.ProjectManifest(got.Manifest)
		if err != nil {
			t.Fatalf("inspect schema %d: %v", schema, err)
		}
		receipt := inspection.Compare(projection, projection)
		if receipt.Counts.Changed != 0 || receipt.Counts.Added != 0 || receipt.Counts.Removed != 0 {
			t.Fatalf("compare schema %d with itself = %#v", schema, receipt.Counts)
		}
	}
}

func compareTrees(t *testing.T, wantRoot, gotRoot string) {
	t.Helper()
	want := treeFiles(t, wantRoot)
	got := treeFiles(t, gotRoot)
	if len(want) != len(got) {
		t.Fatalf("file count = %d, want %d\ngot: %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("file %d = %q, want %q", i, got[i], want[i])
		}
		wantBytes, err := os.ReadFile(filepath.Join(wantRoot, want[i]))
		if err != nil {
			t.Fatal(err)
		}
		gotBytes, err := os.ReadFile(filepath.Join(gotRoot, got[i]))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wantBytes, gotBytes) {
			t.Fatalf("%s differs from generated fixture", want[i])
		}
	}
}

func treeFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	return files
}
