package bundle

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	maxArchiveEntries = 100_000
	maxArchiveBytes   = 1 << 30
)

// PackFixture writes a deterministic gzip-compressed tar archive after
// validating the source fixture completely.
func PackFixture(ctx context.Context, source, destination string) error {
	if _, err := VerifyFixture(source); err != nil {
		return err
	}
	var files []string
	if err := filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == source || entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	}); err != nil {
		return err
	}
	sort.Strings(files)
	if len(files) > maxArchiveEntries {
		return fmt.Errorf("archive has too many entries")
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(destination), ".fixture-pack-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	gz := gzip.NewWriter(tmp)
	gz.Header.ModTime = time.Unix(0, 0).UTC()
	tw := tar.NewWriter(gz)
	for _, rel := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		filename := filepath.Join(source, filepath.FromSlash(rel))
		info, err := os.Stat(filename)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("archive source is not regular: %s", rel)
		}
		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		h := &tar.Header{Name: rel, Mode: 0o600, Size: info.Size(), ModTime: time.Unix(0, 0).UTC(), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(h); err != nil {
			f.Close()
			return err
		}
		if _, err := io.Copy(tw, f); err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, destination)
}

// UnpackFixture safely extracts a deterministic fixture archive and atomically
// replaces target only after the extracted fixture verifies completely.
func UnpackFixture(ctx context.Context, archive, target string) error {
	return WriteAtomic(target, func(stage string) error {
		f, err := os.Open(archive)
		if err != nil {
			return err
		}
		defer f.Close()
		gz, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gz.Close()
		tr := tar.NewReader(gz)
		var entries int
		var total int64
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			h, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			entries++
			if entries > maxArchiveEntries {
				return fmt.Errorf("archive has too many entries")
			}
			if h.Typeflag != tar.TypeReg || h.Size < 0 || h.Size > maxArchiveBytes-total {
				return fmt.Errorf("unsupported or oversized archive entry %q", h.Name)
			}
			if err := ValidateRelativePath(filepath.ToSlash(h.Name)); err != nil || strings.Contains(h.Name, `\`) {
				return fmt.Errorf("unsafe archive path %q", h.Name)
			}
			destination := filepath.Join(stage, filepath.FromSlash(h.Name))
			if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
				return err
			}
			out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			written, copyErr := io.CopyN(out, tr, h.Size)
			closeErr := out.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			if written != h.Size {
				return io.ErrUnexpectedEOF
			}
			total += written
		}
		if _, err := VerifyFixture(stage); err != nil {
			return err
		}
		return nil
	})
}
