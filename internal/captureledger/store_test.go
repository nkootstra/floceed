package captureledger

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestStorePublishesAndReopensVerifiedGeneration(t *testing.T) {
	root, artifacts, generation, payload := storeFixture(t)
	artifact := &generation.Resources[0].Units[0].Artifacts[0]

	store, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(generation, artifacts); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Load(generation.Source, generation.Resources[0].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, _ := generation.CanonicalJSON()
	gotJSON, _ := got.CanonicalJSON()
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("reopened generation differs:\n%s\n%s", gotJSON, wantJSON)
	}
	blob, err := reopened.OpenArtifact(*artifact)
	if err != nil {
		t.Fatal(err)
	}
	defer blob.Close()
	gotPayload, err := io.ReadAll(blob)
	if err != nil || string(gotPayload) != string(payload) {
		t.Fatalf("read blob = %q, %v", gotPayload, err)
	}
}

func TestStoreRejectsUnusableBlobsWithStableReasons(t *testing.T) {
	tests := map[string]struct {
		mutate func(*Store, Artifact)
		want   Reason
	}{
		"missing": {func(store *Store, artifact Artifact) { _ = os.Remove(store.blobPath(artifact)) }, ReasonArtifactMissing},
		"symlink": {func(store *Store, artifact Artifact) {
			path := store.blobPath(artifact)
			_ = os.Remove(path)
			if err := os.Symlink("elsewhere", path); err != nil {
				t.Fatal(err)
			}
		}, ReasonArtifactMissing},
		"nonregular": {func(store *Store, artifact Artifact) {
			path := store.blobPath(artifact)
			_ = os.Remove(path)
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}, ReasonArtifactMissing},
		"wrong size": {func(store *Store, artifact Artifact) {
			if err := os.WriteFile(store.blobPath(artifact), []byte("short"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, ReasonArtifactCorrupt},
		"wrong hash": {func(store *Store, artifact Artifact) {
			if err := os.WriteFile(store.blobPath(artifact), []byte(strings.Repeat("x", int(artifact.Size))), 0o600); err != nil {
				t.Fatal(err)
			}
		}, ReasonArtifactCorrupt},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			root, artifacts, generation, _ := storeFixture(t)
			store, _ := OpenStore(root)
			if err := store.Publish(generation, artifacts); err != nil {
				t.Fatal(err)
			}
			artifact := generation.Resources[0].Units[0].Artifacts[0]
			test.mutate(store, artifact)
			_, err := store.Load(generation.Source, generation.Resources[0].Descriptor)
			if got, ok := InvalidationReason(err); !ok || got != test.want {
				t.Fatalf("reason = %q, %v; want %q", got, err, test.want)
			}
		})
	}
}

func TestLoadCandidatesPreservesCorruptUnitForTargetedRefresh(t *testing.T) {
	root, artifacts, generation, _ := storeFixture(t)
	store, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(generation, artifacts); err != nil {
		t.Fatal(err)
	}
	artifact := generation.Resources[0].Units[0].Artifacts[0]
	if err := os.WriteFile(store.blobPath(artifact), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadCandidates(generation.Source, generation.Resources[0].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	unit := loaded.Resources[0].Units[0]
	if unit.Outcome != UnitOutcomeInvalidated || unit.Reason != ReasonArtifactCorrupt {
		t.Fatalf("candidate unit = %#v, want targeted artifact_corrupt invalidation", unit)
	}
}

func TestStoreReusesMatchingBlobAndNeverOverwritesMismatch(t *testing.T) {
	root, artifacts, generation, payload := storeFixture(t)
	store, _ := OpenStore(root)
	artifact := generation.Resources[0].Units[0].Artifacts[0]
	blobPath := store.blobPath(artifact)
	if err := os.MkdirAll(filepath.Dir(blobPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blobPath, payload, 0o400); err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(generation, artifacts); err != nil {
		t.Fatalf("matching blob was not reused: %v", err)
	}

	mismatch := []byte(strings.Repeat("z", len(payload)))
	if err := os.Chmod(blobPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blobPath, mismatch, 0o600); err != nil {
		t.Fatal(err)
	}
	err := store.Publish(generation, artifacts)
	if reason, ok := InvalidationReason(err); !ok || reason != ReasonArtifactCorrupt {
		t.Fatalf("publish mismatch = %v, reason %q", err, reason)
	}
	got, readErr := os.ReadFile(blobPath)
	if readErr != nil || string(got) != string(mismatch) {
		t.Fatalf("mismatching blob was overwritten: %q, %v", got, readErr)
	}
}

func TestStoreFailedPublicationPreservesPreviousGeneration(t *testing.T) {
	root, artifacts, previous, _ := storeFixture(t)
	store, _ := OpenStore(root)
	if err := store.Publish(previous, artifacts); err != nil {
		t.Fatal(err)
	}
	next := previous
	next.ID = strings.Repeat("9", 64)
	writeIndex := store.writeIndex
	store.writeIndex = func(string, []byte) error { return errors.New("injected interruption before rename") }
	if err := store.Publish(next, artifacts); err == nil {
		t.Fatal("expected publication failure")
	}
	store.writeIndex = writeIndex
	got, err := store.Load(previous.Source, previous.Resources[0].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != previous.ID {
		t.Fatalf("read generation %s after failed publish, want %s", got.ID, previous.ID)
	}
}

func TestStoreIsIndependentFromCheckpointDirectories(t *testing.T) {
	work := t.TempDir()
	root := filepath.Join(work, "ledger")
	checkpoint := filepath.Join(work, "pull-checkpoints", "fingerprint")
	if err := os.MkdirAll(checkpoint, 0o700); err != nil {
		t.Fatal(err)
	}
	artifacts := t.TempDir()
	generation, _ := fixtureGeneration(t, artifacts)
	store, _ := OpenStore(root)
	if err := store.Publish(generation, artifacts); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Dir(checkpoint)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(generation.Source, generation.Resources[0].Descriptor); err != nil {
		t.Fatalf("checkpoint deletion affected ledger: %v", err)
	}
}

func TestStoreConcurrentReadersSeeCompleteGeneration(t *testing.T) {
	root, artifacts, previous, _ := storeFixture(t)
	store, _ := OpenStore(root)
	if err := store.Publish(previous, artifacts); err != nil {
		t.Fatal(err)
	}
	next := previous
	next.ID = strings.Repeat("9", 64)
	var wait sync.WaitGroup
	errorsSeen := make(chan error, 8)
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 100 {
				got, err := store.Load(previous.Source, previous.Resources[0].Descriptor)
				if err != nil {
					errorsSeen <- err
					return
				}
				if got.ID != previous.ID && got.ID != next.ID {
					errorsSeen <- errors.New("reader observed unknown generation")
					return
				}
			}
		}()
	}
	if err := store.Publish(next, artifacts); err != nil {
		t.Fatal(err)
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatal(err)
	}
}

func TestStoreRejectsUnsafeAndNonregularAdmissionSources(t *testing.T) {
	root, artifacts, generation, _ := storeFixture(t)
	store, _ := OpenStore(root)
	artifact := &generation.Resources[0].Units[0].Artifacts[0]
	source := filepath.Join(artifacts, filepath.FromSlash(artifact.Path))
	if err := os.Remove(source); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", source); err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(generation, artifacts); err == nil {
		t.Fatal("expected symlinked source rejection")
	}
}

func TestStoreRejectsSymlinkedAdmissionParent(t *testing.T) {
	root, artifacts, generation, _ := storeFixture(t)
	store, _ := OpenStore(root)
	artifact := generation.Resources[0].Units[0].Artifacts[0]
	parent := filepath.Dir(filepath.Join(artifacts, filepath.FromSlash(artifact.Path)))
	outside := t.TempDir()
	if err := os.RemoveAll(parent); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, parent); err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(generation, artifacts); err == nil {
		t.Fatal("expected symlinked parent rejection")
	}
}

func storeFixture(t *testing.T) (string, string, Generation, []byte) {
	t.Helper()
	artifacts := t.TempDir()
	generation, payload := fixtureGeneration(t, artifacts)
	return t.TempDir(), artifacts, generation, payload
}

func fixtureGeneration(t *testing.T, artifacts string) (Generation, []byte) {
	t.Helper()
	generation := validGeneration()
	payload := []byte("hello ledger")
	artifact := &generation.Resources[0].Units[0].Artifacts[0]
	artifact.SHA256 = digestBytes(payload)
	artifact.Size = int64(len(payload))
	filename := filepath.Join(artifacts, filepath.FromSlash(artifact.Path))
	if err := os.MkdirAll(filepath.Dir(filename), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return generation, payload
}
