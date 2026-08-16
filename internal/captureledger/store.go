package captureledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/storage"
	"golang.org/x/sys/unix"
)

const maxGenerationBytes = 64 << 20

// InvalidationError classifies expected ledger misses and integrity failures
// without exposing filesystem details as part of the stable decision contract.
type InvalidationError struct {
	Reason Reason
	Err    error
}

func (e *InvalidationError) Error() string { return fmt.Sprintf("%s: %v", e.Reason, e.Err) }
func (e *InvalidationError) Unwrap() error { return e.Err }

func InvalidationReason(err error) (Reason, bool) {
	var invalid *InvalidationError
	if errors.As(err, &invalid) {
		return invalid.Reason, true
	}
	return "", false
}

type Store struct {
	root       string
	writeIndex func(string, []byte) error
}

func OpenStore(root string) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("ledger root is required")
	}
	if info, err := os.Lstat(root); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, fmt.Errorf("ledger root must be a regular directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	} else if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	return &Store{root: root, writeIndex: storage.WriteFileSync}, nil
}

// Publish admits every referenced artifact before atomically replacing each
// resource's current generation index. Existing blobs are never overwritten.
func (s *Store) Publish(generation Generation, artifactRoot string) error {
	payload, err := generation.CanonicalJSON()
	if err != nil {
		return err
	}
	for _, resource := range generation.Resources {
		for _, unit := range resource.Units {
			for _, artifact := range unit.Artifacts {
				if err := s.admit(artifactRoot, artifact); err != nil {
					return fmt.Errorf("admit artifact %s: %w", artifact.Path, err)
				}
			}
		}
	}
	for _, resource := range generation.Resources {
		index := s.indexPath(generation.Source, resource.Descriptor)
		if err := os.MkdirAll(filepath.Dir(index), 0o700); err != nil {
			return err
		}
		if err := s.writeIndex(index, payload); err != nil {
			return fmt.Errorf("publish ledger generation: %w", err)
		}
	}
	return nil
}

func (s *Store) Load(source SourceIdentity, resource ResourceDescriptor) (Generation, error) {
	filename := s.indexPath(source, resource)
	b, err := readRegular(filename, maxGenerationBytes)
	if err != nil {
		reason := ReasonFormatChanged
		if errors.Is(err, os.ErrNotExist) {
			reason = ReasonNoCandidate
		}
		return Generation{}, &InvalidationError{Reason: reason, Err: err}
	}
	var generation Generation
	if err := json.Unmarshal(b, &generation); err != nil {
		return Generation{}, &InvalidationError{Reason: ReasonFormatChanged, Err: err}
	}
	if err := generation.Validate(); err != nil {
		return Generation{}, &InvalidationError{Reason: ReasonFormatChanged, Err: err}
	}
	if generation.Source != source {
		return Generation{}, &InvalidationError{Reason: ReasonFormatChanged, Err: fmt.Errorf("generation source does not match partition")}
	}
	found := false
	for _, candidate := range generation.Resources {
		if candidate.Descriptor == resource {
			found = true
		}
		for _, unit := range candidate.Units {
			for _, artifact := range unit.Artifacts {
				f, err := s.OpenArtifact(artifact)
				if err != nil {
					return Generation{}, err
				}
				_ = f.Close()
			}
		}
	}
	if !found {
		return Generation{}, &InvalidationError{Reason: ReasonFormatChanged, Err: fmt.Errorf("generation resource does not match partition")}
	}
	return generation, nil
}

// OpenArtifact verifies the immutable blob again on every use and returns it
// positioned at the start.
func (s *Store) OpenArtifact(artifact Artifact) (*os.File, error) {
	if artifact.Size < 0 || !validSHA256(artifact.SHA256) {
		return nil, &InvalidationError{Reason: ReasonArtifactCorrupt, Err: fmt.Errorf("invalid artifact metadata")}
	}
	filename := s.blobPath(artifact)
	f, err := openNoFollow(filename)
	if err != nil {
		return nil, &InvalidationError{Reason: ReasonArtifactMissing, Err: err}
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = f.Close()
		if err == nil {
			err = fmt.Errorf("blob is not a regular file")
		}
		return nil, &InvalidationError{Reason: ReasonArtifactMissing, Err: err}
	}
	got, err := sumOpenFile(f)
	if err != nil {
		_ = f.Close()
		return nil, &InvalidationError{Reason: ReasonArtifactCorrupt, Err: err}
	}
	if got.SHA256 != artifact.SHA256 || got.Size != artifact.Size {
		_ = f.Close()
		return nil, &InvalidationError{Reason: ReasonArtifactCorrupt, Err: fmt.Errorf("blob size or checksum mismatch")}
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func (s *Store) admit(root string, artifact Artifact) error {
	if err := bundle.ValidateRelativePath(artifact.Path); err != nil {
		return &InvalidationError{Reason: ReasonArtifactCorrupt, Err: err}
	}
	if err := requireRegularSource(root, artifact.Path); err != nil {
		return &InvalidationError{Reason: ReasonArtifactMissing, Err: err}
	}
	source := filepath.Join(root, filepath.FromSlash(artifact.Path))
	f, err := openNoFollow(source)
	if err != nil {
		return &InvalidationError{Reason: ReasonArtifactMissing, Err: err}
	}
	info, statErr := f.Stat()
	if statErr != nil || !info.Mode().IsRegular() {
		_ = f.Close()
		if statErr == nil {
			statErr = fmt.Errorf("source is not a regular file")
		}
		return &InvalidationError{Reason: ReasonArtifactMissing, Err: statErr}
	}
	got, sumErr := sumOpenFile(f)
	_ = f.Close()
	if sumErr != nil || got.SHA256 != artifact.SHA256 || got.Size != artifact.Size {
		if sumErr == nil {
			sumErr = fmt.Errorf("source size or checksum mismatch")
		}
		return &InvalidationError{Reason: ReasonArtifactCorrupt, Err: sumErr}
	}
	destination := s.blobPath(artifact)
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	if err := os.Link(source, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return s.verifyExisting(artifact)
		}
		if err := copyExclusive(destination, source); err != nil {
			if errors.Is(err, os.ErrExist) {
				return s.verifyExisting(artifact)
			}
			return err
		}
	}
	if err := s.verifyExisting(artifact); err != nil {
		_ = os.Remove(destination)
		return err
	}
	return storage.SyncDir(filepath.Dir(destination))
}

func (s *Store) verifyExisting(artifact Artifact) error {
	f, err := s.OpenArtifact(artifact)
	if err != nil {
		return err
	}
	return f.Close()
}

func (s *Store) blobPath(artifact Artifact) string {
	return filepath.Join(s.root, "blobs", artifact.SHA256[:2], artifact.SHA256+"-"+strconv.FormatInt(artifact.Size, 10))
}

func (s *Store) indexPath(source SourceIdentity, resource ResourceDescriptor) string {
	return filepath.Join(s.root, "generations", source.AccountID, digestBytes([]byte(source.Region)), digestBytes([]byte(resource.Service)), digestBytes([]byte(descriptorKey(resource))), "current.json")
}

func digestBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func openNoFollow(filename string) (*os.File, error) {
	fd, err := unix.Open(filename, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), filename), nil
}

func sumOpenFile(f *os.File) (bundle.Checksum, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return bundle.Checksum{}, err
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return bundle.Checksum{}, err
	}
	return bundle.Checksum{SHA256: hex.EncodeToString(h.Sum(nil)), Size: n}, nil
}

func readRegular(filename string, limit int64) ([]byte, error) {
	f, err := openNoFollow(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, fmt.Errorf("metadata is not a bounded regular file")
	}
	return io.ReadAll(io.LimitReader(f, limit+1))
}

func requireRegularSource(root, relative string) error {
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return fmt.Errorf("artifact root is not a regular directory")
	}
	current := root
	parts := strings.Split(filepath.FromSlash(relative), string(filepath.Separator))
	for index, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact source contains a symlink")
		}
		if index < len(parts)-1 && !info.IsDir() {
			return fmt.Errorf("artifact source parent is not a directory")
		}
		if index == len(parts)-1 && !info.Mode().IsRegular() {
			return fmt.Errorf("artifact source is not a regular file")
		}
	}
	return nil
}

func copyExclusive(dst, src string) error {
	in, err := openNoFollow(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o400)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(dst)
		return errors.Join(copyErr, syncErr, closeErr)
	}
	return nil
}
