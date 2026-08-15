package bundle

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
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
