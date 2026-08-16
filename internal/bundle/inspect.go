package bundle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nkootstra/floceed/internal/model"
	"golang.org/x/sys/unix"
)

var (
	ErrGeneratedSchema      = errors.New("unsupported generated bundle schema")
	ErrGeneratedPath        = errors.New("unsafe generated bundle path")
	ErrGeneratedRootMissing = errors.New("generated bundle root is missing")
)

// Metadata limits keep inspection from allocating unbounded memory for files
// that must be decoded as a single JSON document. Bundle payloads remain
// streamed and are not subject to these limits.
const (
	maxChecksumsBytes = 16 << 20
	maxManifestBytes  = 64 << 20
	maxChecksumFiles  = 100_000
)

var requiredGeneratedFiles = [...]string{
	"bundle/manifest.json",
	ComposeFile,
	"runtime/replay.py",
	"init/ready.d/10-replay.py",
}

// Generated is a validated, read-only view of an installed bundle.
type Generated struct {
	Manifest  model.Manifest
	Checksums Checksums
}

// LoadGenerated validates the checksum index and manifest without changing the
// bundle or consulting any external service.
func LoadGenerated(ctx context.Context, root string) (Generated, error) {
	if err := ctx.Err(); err != nil {
		return Generated{}, err
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Generated{}, fmt.Errorf("%w: %w", ErrGeneratedRootMissing, err)
		}
		return Generated{}, err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return Generated{}, fmt.Errorf("%w: bundle root is not a regular directory", ErrGeneratedPath)
	}
	b, err := readMetadata(ctx, root, "checksums.json", maxChecksumsBytes, nil)
	if err != nil {
		return Generated{}, fmt.Errorf("read checksums: %w", err)
	}
	var sums Checksums
	if err := json.Unmarshal(b, &sums); err != nil {
		return Generated{}, fmt.Errorf("decode checksums: %w", err)
	}
	if sums.SchemaVersion != 1 {
		return Generated{}, fmt.Errorf("unsupported checksum schema %d: %w", sums.SchemaVersion, ErrGeneratedSchema)
	}
	if len(sums.Files) > maxChecksumFiles {
		return Generated{}, fmt.Errorf("checksums contain too many files: %d exceeds %d", len(sums.Files), maxChecksumFiles)
	}
	seen := make(map[string]struct{}, len(sums.Files))
	var manifestBytes []byte
	scratch := make([]byte, 128*1024)
	for _, expected := range sums.Files {
		if err := ctx.Err(); err != nil {
			return Generated{}, err
		}
		if err := ValidateRelativePath(expected.Path); err != nil {
			return Generated{}, fmt.Errorf("%w: %v", ErrGeneratedPath, err)
		}
		if _, exists := seen[expected.Path]; exists {
			return Generated{}, fmt.Errorf("duplicate checksum path %q", expected.Path)
		}
		seen[expected.Path] = struct{}{}
		if expected.Path == "bundle/manifest.json" {
			if expected.Size > maxManifestBytes {
				return Generated{}, fmt.Errorf("validate %s: manifest exceeds %d bytes", expected.Path, maxManifestBytes)
			}
			manifestBytes, err = readMetadata(ctx, root, expected.Path, maxManifestBytes, &expected.Size)
			if err == nil {
				err = verifyBytes(manifestBytes, expected)
			}
		} else {
			err = verifyChecksum(ctx, root, expected, scratch)
		}
		if err != nil {
			return Generated{}, fmt.Errorf("validate %s: %w", expected.Path, err)
		}
	}
	if err := validateChecksumCoverage(ctx, root, seen); err != nil {
		return Generated{}, err
	}
	for _, required := range requiredGeneratedFiles {
		if _, exists := seen[required]; !exists {
			return Generated{}, fmt.Errorf("checksums do not include required file %s", required)
		}
	}
	if err := ctx.Err(); err != nil {
		return Generated{}, err
	}
	var manifest model.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return Generated{}, fmt.Errorf("decode manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		if errors.Is(err, model.ErrSchema) {
			return Generated{}, fmt.Errorf("validate manifest: %w: %v", ErrGeneratedSchema, err)
		}
		return Generated{}, fmt.Errorf("validate manifest: %w", err)
	}
	if err := validateManifestArtifacts(manifest, sums); err != nil {
		return Generated{}, err
	}
	return Generated{Manifest: manifest, Checksums: sums}, nil
}

func validateManifestArtifacts(manifest model.Manifest, sums Checksums) error {
	indexed := make(map[string]Checksum, len(sums.Files))
	for _, sum := range sums.Files {
		indexed[sum.Path] = sum
	}
	var artifacts []model.ArtifactRef
	for _, snapshot := range manifest.Snapshots {
		artifacts = append(artifacts, snapshot.Data...)
		if snapshot.Dataset != nil {
			for _, chunk := range snapshot.Dataset.Chunks {
				artifacts = append(artifacts, chunk.Data)
				if chunk.Index != nil {
					artifacts = append(artifacts, *chunk.Index)
				}
			}
		}
	}
	for _, artifact := range artifacts {
		if err := ValidateRelativePath(artifact.Path); err != nil {
			return fmt.Errorf("%w: %v", ErrGeneratedPath, err)
		}
		sum, exists := indexed[artifact.Path]
		if !exists || sum.SHA256 != artifact.SHA256 || sum.Size != artifact.Size {
			return fmt.Errorf("manifest artifact checksum does not match index: %s", artifact.Path)
		}
	}
	return nil
}

func validateChecksumCoverage(ctx context.Context, root string, indexed map[string]struct{}) error {
	return filepath.WalkDir(root, func(filename string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if filename == root {
			if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() {
				return fmt.Errorf("%w: bundle root is not a regular directory", ErrGeneratedPath)
			}
			return nil
		}
		relative, err := filepath.Rel(root, filename)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if err := ValidateRelativePath(relative); err != nil {
			return fmt.Errorf("%w: %v", ErrGeneratedPath, err)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlink is not allowed: %s", ErrGeneratedPath, relative)
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: bundle entry is not a regular file: %s", ErrGeneratedPath, relative)
		}
		if relative == "checksums.json" {
			return nil
		}
		if _, exists := indexed[relative]; !exists {
			return fmt.Errorf("bundle file is missing from checksum index: %s", relative)
		}
		return nil
	})
}

func requireRegular(root, relative string) error {
	parts := strings.Split(filepath.FromSlash(relative), string(filepath.Separator))
	current := root
	for index, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink is not allowed: %s", relative)
		}
		if index < len(parts)-1 && !info.IsDir() {
			return fmt.Errorf("path component is not a directory: %s", relative)
		}
		if index == len(parts)-1 && !info.Mode().IsRegular() {
			return fmt.Errorf("bundle entry is not a regular file: %s", relative)
		}
	}
	return nil
}

func openRegular(root, relative string) (*os.File, os.FileInfo, error) {
	if err := requireRegular(root, relative); err != nil {
		return nil, nil, err
	}
	filename := filepath.Join(root, filepath.FromSlash(relative))
	fd, err := unix.Open(filename, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, err
	}
	f := os.NewFile(uintptr(fd), filename)
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, nil, fmt.Errorf("bundle entry is not a regular file: %s", relative)
	}
	return f, info, nil
}

func readMetadata(ctx context.Context, root, relative string, limit int64, expectedSize *int64) ([]byte, error) {
	f, info, err := openRegular(root, relative)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrGeneratedPath, err)
	}
	defer f.Close()
	if info.Size() > limit {
		return nil, fmt.Errorf("%s exceeds %d bytes", relative, limit)
	}
	if expectedSize != nil && (*expectedSize < 0 || info.Size() != *expectedSize) {
		return nil, fmt.Errorf("checksum mismatch")
	}
	data := make([]byte, 0, info.Size())
	buffer := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, readErr := f.Read(buffer)
		if n > 0 {
			if int64(len(data)+n) > limit {
				return nil, fmt.Errorf("%s exceeds %d bytes", relative, limit)
			}
			data = append(data, buffer[:n]...)
		}
		if readErr == io.EOF {
			return data, nil
		}
		if readErr != nil {
			return nil, readErr
		}
	}
}

func verifyChecksum(ctx context.Context, root string, expected Checksum, buffer []byte) error {
	f, info, err := openRegular(root, expected.Path)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrGeneratedPath, err)
	}
	defer f.Close()
	// Reject a declared-size mismatch before touching potentially large payload
	// contents. The streamed count below still guards files that change mid-read.
	if expected.Size < 0 || info.Size() != expected.Size {
		return fmt.Errorf("checksum mismatch")
	}
	h := sha256.New()
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := f.Read(buffer)
		if n != 0 {
			size += int64(n)
			_, _ = h.Write(buffer[:n])
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if size != expected.Size || hex.EncodeToString(h.Sum(nil)) != expected.SHA256 {
		return fmt.Errorf("checksum mismatch")
	}
	return nil
}

func verifyBytes(data []byte, expected Checksum) error {
	sum := sha256.Sum256(data)
	if int64(len(data)) != expected.Size || hex.EncodeToString(sum[:]) != expected.SHA256 {
		return fmt.Errorf("checksum mismatch")
	}
	return nil
}
