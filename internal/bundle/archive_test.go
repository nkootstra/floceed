package bundle

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/nkootstra/floceed/internal/model"
)

func makeArchiveFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	manifest, err := json.Marshal(model.Manifest{SchemaVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "payload.txt"), []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	sums, err := BuildChecksums(root)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(sums)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "checksums.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestPackUnpackFixturePreservesVerification(t *testing.T) {
	source := makeArchiveFixture(t)
	archive := filepath.Join(t.TempDir(), "fixture.tar.gz")
	if err := PackFixture(context.Background(), source, archive); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "unpacked")
	if err := UnpackFixture(context.Background(), archive, target); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyFixture(target); err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(t.TempDir(), "fixture-second.tar.gz")
	if err := PackFixture(context.Background(), target, second); err != nil {
		t.Fatal(err)
	}
	one, _ := os.ReadFile(archive)
	two, _ := os.ReadFile(second)
	if string(one) != string(two) {
		t.Fatal("repacked fixture archive is not deterministic")
	}
}

func TestUnpackRejectsTraversalBeforeTargetMutation(t *testing.T) {
	archive := filepath.Join(t.TempDir(), "bad.tar.gz")
	if err := os.WriteFile(archive, []byte("not an archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := UnpackFixture(context.Background(), archive, target); err == nil {
		t.Fatal("invalid archive accepted")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target exists after rejection: %v", err)
	}
}
