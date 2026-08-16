package bundle

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nkootstra/floceed/internal/model"
)

func TestCanonicalJSONIsDeterministic(t *testing.T) {
	v := map[string]any{"z": 1, "a": map[string]any{"y": 2, "b": 3}}
	a, err := CanonicalJSON(v)
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalJSON(v)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("outputs differ: %q %q", a, b)
	}
	if string(a) != "{\n  \"a\": {\n    \"b\": 3,\n    \"y\": 2\n  },\n  \"z\": 1\n}\n" {
		t.Fatalf("unexpected output: %s", a)
	}
}

func TestValidateRelativePath(t *testing.T) {
	for _, path := range []string{"../secret", "/absolute", "a/../../b", `a\\b`} {
		if err := ValidateRelativePath(path); err == nil {
			t.Errorf("expected %q to fail", path)
		}
	}
	if err := ValidateRelativePath("bundle/data/item.json"); err != nil {
		t.Fatal(err)
	}
}

func TestWriteAtomicPreservesOldDirectoryOnFailure(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, ".floceed")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "old"), []byte("valid"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := WriteAtomic(target, func(stage string) error { return os.ErrPermission })
	if err == nil {
		t.Fatal("expected error")
	}
	b, readErr := os.ReadFile(filepath.Join(target, "old"))
	if readErr != nil || string(b) != "valid" {
		t.Fatalf("old bundle lost: %q, %v", b, readErr)
	}
}

func TestChecksumsDetectModification(t *testing.T) {
	root := t.TempDir()
	filename := filepath.Join(root, "manifest.json")
	if err := os.WriteFile(filename, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	sums, err := BuildChecksums(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyChecksums(root, sums); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyChecksums(root, sums); err == nil {
		t.Fatal("expected checksum mismatch")
	}
}

func TestInventoryIdentityIsIndependentOfEnumerationOrder(t *testing.T) {
	one := Checksums{SchemaVersion: 1, Files: []Checksum{{Path: "z", SHA256: "a", Size: 1}, {Path: "a", SHA256: "b", Size: 2}}}
	two := Checksums{SchemaVersion: 1, Files: []Checksum{{Path: "a", SHA256: "b", Size: 2}, {Path: "z", SHA256: "a", Size: 1}}}
	a, err := FixtureIdentity(one)
	if err != nil {
		t.Fatal(err)
	}
	b, err := FixtureIdentity(two)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("identities differ: %q != %q", a, b)
	}
}

func TestVerifyFixtureRejectsExtraAndDuplicateFiles(t *testing.T) {
	root := writeVerifiableFixture(t, nil)
	if err := os.WriteFile(filepath.Join(root, "unchecked"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyFixture(root); !hasVerificationCode(err, VerificationUnexpectedFile) {
		t.Fatalf("expected extra-file error, got %v", err)
	}

	root = writeVerifiableFixture(t, nil)
	b, err := os.ReadFile(filepath.Join(root, "checksums.json"))
	if err != nil {
		t.Fatal(err)
	}
	var sums Checksums
	if err := json.Unmarshal(b, &sums); err != nil {
		t.Fatal(err)
	}
	sums.Files = append(sums.Files, sums.Files[0])
	b, _ = CanonicalJSON(sums)
	if err := os.WriteFile(filepath.Join(root, "checksums.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyFixture(root); !hasVerificationCode(err, VerificationDuplicatePath) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestVerifyFixtureValidatesProvenanceAndArtifactReferences(t *testing.T) {
	manifest := model.Manifest{
		SchemaVersion: 1,
		Source:        model.SourceMetadata{AccountID: "123456789012", Region: "eu-west-1"},
		Capture:       model.CaptureMetadata{CapturedAt: time.Unix(100, 0).UTC()},
		Snapshots: []model.Snapshot{{
			Resource:         model.ResourceRef{Service: "s3", Type: "bucket", ID: "assets"},
			Service:          "s3",
			StructureVersion: 1,
			Structure:        json.RawMessage(`{"name":"assets","region":"eu-west-1"}`),
			Data:             []model.ArtifactRef{{Path: "bundle/data/assets.json", SHA256: "", Size: 3}},
		}},
	}
	root := writeVerifiableFixture(t, &manifest)
	artifact, err := SumFile(filepath.Join(root, "bundle/data/assets.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest.Snapshots[0].Data[0].SHA256 = artifact.SHA256
	manifest.Snapshots[0].Data[0].Size = artifact.Size
	writeManifestAndChecksums(t, root, manifest)
	provenance := model.Provenance{SchemaVersion: 1, AccountID: "123456789012", Region: "eu-west-1", CapturedAt: manifest.Capture.CapturedAt, ManifestSchema: 1}
	b, _ := CanonicalJSON(provenance)
	if err := os.WriteFile(filepath.Join(root, "provenance.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	writeManifestAndChecksums(t, root, manifest)
	result, err := VerifyFixture(root)
	if err != nil {
		t.Fatal(err)
	}
	if result.ProvenanceStatus != model.ProvenanceSelfAsserted || result.Provenance == nil || result.FileCount == 0 {
		t.Fatalf("unexpected verification result: %+v", result)
	}

	provenance.Region = "us-east-1"
	b, _ = CanonicalJSON(provenance)
	if err := os.WriteFile(filepath.Join(root, "provenance.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	writeManifestAndChecksums(t, root, manifest)
	if _, err := VerifyFixture(root); !hasVerificationCode(err, VerificationProvenanceMismatch) {
		t.Fatalf("expected provenance mismatch, got %v", err)
	}
}

func writeVerifiableFixture(t *testing.T, manifest *model.Manifest) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "bundle/data"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bundle/data/assets.json"), []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	if manifest == nil {
		manifest = &model.Manifest{SchemaVersion: 1}
	}
	b, err := CanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bundle/manifest.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	writeManifestAndChecksums(t, root, *manifest)
	return root
}

func writeManifestAndChecksums(t *testing.T, root string, _ model.Manifest) {
	t.Helper()
	sums, err := BuildChecksums(root, "checksums.json")
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalJSON(sums)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "checksums.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func hasVerificationCode(err error, code VerificationCode) bool {
	var verification *VerificationError
	return errors.As(err, &verification) && verification.Code == code
}
