package ndjson

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nkootstra/floceed/internal/model"
)

func TestCommitWritesArtifactWithHashAndSize(t *testing.T) {
	root := t.TempDir()
	writer, err := Create(root, "bundle/data/kinesis/events.ndjson", 0)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := writer.Write([]byte(`{"a":1}`)); err != nil || !ok {
		t.Fatalf("Write() = %v, %v", ok, err)
	}
	if ok, err := writer.Write([]byte(`{"b":2}`)); err != nil || !ok {
		t.Fatalf("Write() = %v, %v", ok, err)
	}
	artifact, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Path != "bundle/data/kinesis/events.ndjson" || artifact.Size != 16 {
		t.Fatalf("artifact = %#v", artifact)
	}
	dest := filepath.Join(root, filepath.FromSlash(artifact.Path))
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "{\"a\":1}\n{\"b\":2}\n" {
		t.Fatalf("artifact content = %q", data)
	}
	if artifact.SHA256 == "" {
		t.Fatal("SHA256 must be populated")
	}
	sum := sha256.Sum256(data)
	if artifact.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("SHA256 = %q, want %q", artifact.SHA256, hex.EncodeToString(sum[:]))
	}
	// The temp file must be gone after Commit.
	matches, _ := filepath.Glob(filepath.Join(filepath.Dir(dest), ".ndjson-*"))
	if len(matches) != 0 {
		t.Fatalf("temp files left after Commit: %v", matches)
	}
}

func TestByteBoundRejectsOversizedRecord(t *testing.T) {
	writer, err := Create(t.TempDir(), "bundle/data/sqs/jobs.ndjson", 11)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := writer.Write([]byte("0123456789")); err != nil || !ok {
		t.Fatalf("exact-boundary write = %v, %v", ok, err)
	}
	if ok, err := writer.Write([]byte("x")); err != nil || ok {
		t.Fatalf("oversized write = %v, %v (want false, nil)", ok, err)
	}
	if writer.Size() != 11 {
		t.Fatalf("Size() = %d, want 11", writer.Size())
	}
}

func TestAbortDiscardsTemporaryFile(t *testing.T) {
	root := t.TempDir()
	writer, err := Create(root, "bundle/data/kinesis/events.ndjson", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	writer.Abort()
	// The temp name is random, so glob for any leftover temp files.
	matches, _ := filepath.Glob(filepath.Join(root, "bundle", "data", "kinesis", ".ndjson-*"))
	if len(matches) != 0 {
		t.Fatalf("temp files left after Abort: %v", matches)
	}
	if _, err := os.Stat(filepath.Join(root, "bundle", "data", "kinesis", "events.ndjson")); !os.IsNotExist(err) {
		t.Fatal("destination must not exist after Abort")
	}
}

func TestCreateRejectsUnsafePaths(t *testing.T) {
	for _, name := range []string{
		"/abs/path.ndjson",
		"bundle/../../escape.ndjson",
		"bundle/..",
	} {
		if _, err := Create(t.TempDir(), name, 0); !errors.Is(err, model.ErrValidation) {
			t.Fatalf("unsafe path %q: err = %v, want model.ErrValidation", name, err)
		}
	}
}

func TestCreateRejectsExactParentTraversal(t *testing.T) {
	if _, err := Create(t.TempDir(), "..", 0); !errors.Is(err, model.ErrValidation) {
		t.Fatalf("name cleaning to exactly \"..\" must be rejected: err = %v, want model.ErrValidation", err)
	}
}

func TestCreateProducesValidArtifactRef(t *testing.T) {
	root := t.TempDir()
	writer, err := Create(root, "bundle/data/sqs/jobs.ndjson", 0)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"body":"hello"}`)
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	artifact, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Path == "" || artifact.Size != int64(len(body))+1 {
		t.Fatalf("artifact = %#v", artifact)
	}
	sum := sha256.Sum256(append(append([]byte(nil), body...), '\n'))
	if artifact.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("SHA256 = %q, want %q", artifact.SHA256, hex.EncodeToString(sum[:]))
	}
}
