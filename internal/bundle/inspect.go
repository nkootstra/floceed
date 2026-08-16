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
)

var (
	ErrGeneratedSchema = errors.New("unsupported generated bundle schema")
	ErrGeneratedPath   = errors.New("unsafe generated bundle path")
)

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
	checksumsPath := filepath.Join(root, "checksums.json")
	if err := requireRegular(root, "checksums.json"); err != nil {
		return Generated{}, fmt.Errorf("read checksums: %w", err)
	}
	b, err := os.ReadFile(checksumsPath)
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
	seen := make(map[string]struct{}, len(sums.Files))
	manifestFound := false
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
		manifestFound = manifestFound || expected.Path == "bundle/manifest.json"
		if err := requireRegular(root, expected.Path); err != nil {
			return Generated{}, fmt.Errorf("validate %s: %w: %v", expected.Path, ErrGeneratedPath, err)
		}
		filename := filepath.Join(root, filepath.FromSlash(expected.Path))
		if expected.Path == "bundle/manifest.json" {
			manifestBytes, err = os.ReadFile(filename)
			if err == nil {
				err = verifyBytes(manifestBytes, expected)
			}
		} else {
			err = verifyChecksum(ctx, filename, expected, scratch)
		}
		if err != nil {
			return Generated{}, fmt.Errorf("validate %s: %w", expected.Path, err)
		}
	}
	if err := validateChecksumCoverage(ctx, root, seen); err != nil {
		return Generated{}, err
	}
	if !manifestFound {
		return Generated{}, fmt.Errorf("checksums do not include bundle/manifest.json")
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

func verifyChecksum(ctx context.Context, filename string, expected Checksum, buffer []byte) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
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
