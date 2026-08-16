package bundle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// ComposeFile is the generated Compose project installed into the bundle
// output directory. It is the entry floceed up gates on and floceed render
// produces, so it lives here rather than as a literal in each caller.
const ComposeFile = "compose.generated.yaml"

type Checksum struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}
type Checksums struct {
	SchemaVersion int        `json:"schema_version"`
	Files         []Checksum `json:"files"`
}

func CanonicalJSON(v any) ([]byte, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal deterministic JSON: %w", err)
	}
	return append(b, '\n'), nil
}

func ValidateRelativePath(name string) error {
	if name == "" || filepath.IsAbs(name) || strings.Contains(name, `\`) {
		return fmt.Errorf("unsafe bundle path %q", name)
	}
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || clean != name {
		return fmt.Errorf("unsafe bundle path %q", name)
	}
	return nil
}

func SumFile(filename string) (Checksum, error) {
	f, err := os.Open(filename)
	if err != nil {
		return Checksum{}, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return Checksum{}, err
	}
	return Checksum{SHA256: hex.EncodeToString(h.Sum(nil)), Size: n}, nil
}

func BuildChecksums(root string, exclude ...string) (Checksums, error) {
	skipped := map[string]bool{}
	for _, p := range exclude {
		skipped[filepath.ToSlash(p)] = true
	}
	var files []Checksum
	err := filepath.WalkDir(root, func(filename string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, filename)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if skipped[rel] {
			return nil
		}
		if err := ValidateRelativePath(rel); err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink is not allowed in bundle: %s", rel)
		}
		sum, err := SumFile(filename)
		if err != nil {
			return err
		}
		sum.Path = rel
		files = append(files, sum)
		return nil
	})
	if err != nil {
		return Checksums{}, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return Checksums{SchemaVersion: 1, Files: files}, nil
}

func VerifyChecksums(root string, sums Checksums) error {
	if sums.SchemaVersion != 1 {
		return fmt.Errorf("unsupported checksum schema %d", sums.SchemaVersion)
	}
	for _, expected := range sums.Files {
		if err := ValidateRelativePath(expected.Path); err != nil {
			return err
		}
		got, err := SumFile(filepath.Join(root, filepath.FromSlash(expected.Path)))
		if err != nil {
			return err
		}
		if got.SHA256 != expected.SHA256 || got.Size != expected.Size {
			return fmt.Errorf("checksum mismatch for %s", expected.Path)
		}
	}
	return nil
}

// WriteAtomic builds a complete directory beside target and replaces target only
// after build succeeds. When replacement fails, it restores the prior directory.
func WriteAtomic(target string, build func(stage string) error) error {
	return WriteAtomicGuarded(target, build, nil)
}

// WriteAtomicGuarded builds a complete replacement and invokes beforeInstall
// while the target lock is held, immediately before the target is replaced.
func WriteAtomicGuarded(target string, build func(stage string) error, beforeInstall func() error) error {
	parent := filepath.Dir(target)
	base := filepath.Base(target)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	lock := target + ".lock"
	lockFile, err := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("bundle render already in progress: %w", err)
		}
		return err
	}
	lockFile.Close()
	defer os.Remove(lock)
	stage, err := os.MkdirTemp(parent, "."+base+".stage-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	if err := os.Chmod(stage, 0o700); err != nil {
		return err
	}
	if err := build(stage); err != nil {
		return fmt.Errorf("build staged bundle: %w", err)
	}
	if beforeInstall != nil {
		if err := beforeInstall(); err != nil {
			return fmt.Errorf("validate bundle before install: %w", err)
		}
	}
	backup := target + ".backup"
	if _, err := os.Stat(backup); err == nil {
		return fmt.Errorf("recovery backup already exists at %s", backup)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	hadTarget := false
	if _, err := os.Stat(target); err == nil {
		hadTarget = true
		if err := os.Rename(target, backup); err != nil {
			return fmt.Errorf("back up prior bundle: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(stage, target); err != nil {
		if hadTarget {
			_ = os.Rename(backup, target)
		}
		return fmt.Errorf("install staged bundle: %w", err)
	}
	if hadTarget {
		if err := os.RemoveAll(backup); err != nil {
			return fmt.Errorf("new bundle installed but old backup remains at %s: %w", backup, err)
		}
	}
	return nil
}
