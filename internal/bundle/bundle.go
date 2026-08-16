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

	"github.com/nkootstra/floceed/internal/model"
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

type VerificationCode string

const (
	VerificationInvalidInput       VerificationCode = "invalid_input"
	VerificationUnsupportedSchema  VerificationCode = "unsupported_schema"
	VerificationUnsafePath         VerificationCode = "unsafe_path"
	VerificationDuplicatePath      VerificationCode = "duplicate_path"
	VerificationMissingFile        VerificationCode = "missing_file"
	VerificationUnexpectedFile     VerificationCode = "unexpected_file"
	VerificationNonRegularFile     VerificationCode = "non_regular_file"
	VerificationChecksumMismatch   VerificationCode = "checksum_mismatch"
	VerificationManifestMismatch   VerificationCode = "manifest_mismatch"
	VerificationProvenanceMismatch VerificationCode = "provenance_mismatch"
)

type VerificationError struct {
	Code VerificationCode
	Path string
	Err  error
}

func (e *VerificationError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("%s (%s): %v", e.Code, e.Path, e.Err)
	}
	return fmt.Sprintf("%s: %v", e.Code, e.Err)
}
func (e *VerificationError) Unwrap() error { return e.Err }

func verificationError(code VerificationCode, path string, err error) error {
	return &VerificationError{Code: code, Path: path, Err: err}
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

// FixtureIdentity derives identity from the canonical inventory, never from
// archive/container bytes. The inventory must already be complete and
// verified by VerifyFixture.
func FixtureIdentity(sums Checksums) (string, error) {
	files := append([]Checksum(nil), sums.Files...)
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	b, err := CanonicalJSON(files)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

// VerifyFixture validates a complete offline fixture directory. It performs
// all structural checks before returning a result, and never accesses cloud
// configuration or services.
func VerifyFixture(root string) (model.VerificationResult, error) {
	var result model.VerificationResult
	info, err := os.Stat(root)
	if err != nil {
		return result, verificationError(VerificationInvalidInput, root, err)
	}
	if !info.IsDir() {
		return result, verificationError(VerificationInvalidInput, root, errors.New("fixture root is not a directory"))
	}
	checksumPath := filepath.Join(root, "checksums.json")
	b, err := os.ReadFile(checksumPath)
	if err != nil {
		return result, verificationError(VerificationInvalidInput, "checksums.json", err)
	}
	var sums Checksums
	if err := json.Unmarshal(b, &sums); err != nil {
		return result, verificationError(VerificationInvalidInput, "checksums.json", err)
	}
	if sums.SchemaVersion != 1 {
		return result, verificationError(VerificationUnsupportedSchema, "checksums.json", fmt.Errorf("checksum schema %d", sums.SchemaVersion))
	}
	seen := make(map[string]struct{}, len(sums.Files))
	for _, entry := range sums.Files {
		if err := ValidateRelativePath(entry.Path); err != nil {
			return result, verificationError(VerificationUnsafePath, entry.Path, err)
		}
		if _, ok := seen[entry.Path]; ok {
			return result, verificationError(VerificationDuplicatePath, entry.Path, errors.New("duplicate checksum path"))
		}
		seen[entry.Path] = struct{}{}
		if entry.SHA256 == "" || entry.Size < 0 {
			return result, verificationError(VerificationInvalidInput, entry.Path, errors.New("invalid checksum record"))
		}
		filename := filepath.Join(root, filepath.FromSlash(entry.Path))
		st, statErr := os.Lstat(filename)
		if statErr != nil {
			return result, verificationError(VerificationMissingFile, entry.Path, statErr)
		}
		if !st.Mode().IsRegular() {
			return result, verificationError(VerificationNonRegularFile, entry.Path, errors.New("fixture entries must be regular files"))
		}
		got, sumErr := SumFile(filename)
		if sumErr != nil {
			return result, verificationError(VerificationChecksumMismatch, entry.Path, sumErr)
		}
		if got.Size != entry.Size || got.SHA256 != entry.SHA256 {
			return result, verificationError(VerificationChecksumMismatch, entry.Path, errors.New("size or sha256 differs"))
		}
	}
	var walked []string
	err = filepath.WalkDir(root, func(filename string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if filename == root {
			return nil
		}
		rel, relErr := filepath.Rel(root, filename)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			if entry.Type()&os.ModeSymlink != 0 {
				return verificationError(VerificationNonRegularFile, rel, errors.New("symlink directory is not allowed"))
			}
			return nil
		}
		if rel == "checksums.json" {
			return nil
		}
		if err := ValidateRelativePath(rel); err != nil {
			return verificationError(VerificationUnsafePath, rel, err)
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return verificationError(VerificationNonRegularFile, rel, errors.New("fixture entries must be regular files"))
		}
		walked = append(walked, rel)
		if _, ok := seen[rel]; !ok {
			return verificationError(VerificationUnexpectedFile, rel, errors.New("file is absent from checksum inventory"))
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if len(walked) != len(seen) {
		return result, verificationError(VerificationMissingFile, "", errors.New("checksum inventory is not exact"))
	}
	identity, err := FixtureIdentity(sums)
	if err != nil {
		return result, verificationError(VerificationInvalidInput, "", err)
	}
	manifestPath := filepath.Join(root, "manifest.json")
	if _, err := os.Stat(manifestPath); errors.Is(err, os.ErrNotExist) {
		manifestPath = filepath.Join(root, "bundle", "manifest.json")
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return result, verificationError(VerificationInvalidInput, "manifest.json", err)
	}
	var manifest model.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return result, verificationError(VerificationInvalidInput, "manifest.json", err)
	}
	if err := manifest.Validate(); err != nil {
		return result, verificationError(VerificationUnsupportedSchema, "manifest.json", err)
	}
	for _, snap := range manifest.Snapshots {
		for _, artifact := range append(append([]model.ArtifactRef(nil), snap.Data...), snapshotDatasetArtifacts(snap)...) {
			if err := ValidateRelativePath(artifact.Path); err != nil {
				return result, verificationError(VerificationUnsafePath, artifact.Path, err)
			}
			if _, ok := seen[artifact.Path]; !ok {
				return result, verificationError(VerificationManifestMismatch, artifact.Path, errors.New("manifest artifact is absent from checksum inventory"))
			}
		}
	}
	result.Identity, result.ManifestSchema = identity, manifest.SchemaVersion
	result.FileCount = len(seen)
	for _, entry := range sums.Files {
		result.TotalBytes += entry.Size
	}
	provPath := filepath.Join(root, "provenance.json")
	if pb, readErr := os.ReadFile(provPath); readErr == nil {
		var provenance model.Provenance
		if err := json.Unmarshal(pb, &provenance); err != nil {
			return result, verificationError(VerificationInvalidInput, "provenance.json", err)
		}
		if provenance.SchemaVersion != model.CurrentProvenanceSchemaVersion || provenance.AccountID != manifest.Source.AccountID || provenance.Region != manifest.Source.Region || provenance.ManifestSchema != manifest.SchemaVersion || !provenance.CapturedAt.Equal(manifest.Capture.CapturedAt) {
			return result, verificationError(VerificationProvenanceMismatch, "provenance.json", errors.New("provenance does not agree with manifest"))
		}
		result.Provenance, result.ProvenanceStatus = &provenance, model.ProvenanceSelfAsserted
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return result, verificationError(VerificationInvalidInput, "provenance.json", readErr)
	}
	return result, nil
}

func snapshotDatasetArtifacts(snapshot model.Snapshot) []model.ArtifactRef {
	if snapshot.Dataset == nil {
		return nil
	}
	out := make([]model.ArtifactRef, 0, len(snapshot.Dataset.Chunks)*2)
	for _, chunk := range snapshot.Dataset.Chunks {
		out = append(out, chunk.Data)
		if chunk.Index != nil {
			out = append(out, *chunk.Index)
		}
	}
	return out
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
