package bundle

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"golang.org/x/sys/unix"
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
	checksumsBytes, err := os.ReadFile(filepath.Join(source, "checksums.json"))
	if err != nil {
		return err
	}
	var checksums Checksums
	if err := json.Unmarshal(checksumsBytes, &checksums); err != nil {
		return err
	}
	expected := make(map[string]Checksum, len(checksums.Files))
	for _, checksum := range checksums.Files {
		expected[checksum.Path] = checksum
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
		f, err := openRegularNoFollow(filename)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		checksum, hasExpected := expected[rel]
		if hasExpected && (checksum.Size != info.Size()) {
			f.Close()
			return fmt.Errorf("archive source changed: %s", rel)
		}
		h := &tar.Header{Name: rel, Mode: 0o600, Size: info.Size(), ModTime: time.Unix(0, 0).UTC(), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(h); err != nil {
			f.Close()
			return err
		}
		hash := sha256.New()
		written, err := io.CopyN(io.MultiWriter(tw, hash), f, info.Size())
		if err != nil || written != info.Size() {
			f.Close()
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		finalInfo, err := os.Stat(filename)
		if err != nil || finalInfo.Size() != info.Size() {
			if err == nil {
				err = fmt.Errorf("archive source changed: %s", rel)
			}
			return err
		}
		if hasExpected && hex.EncodeToString(hash.Sum(nil)) != checksum.SHA256 {
			return fmt.Errorf("archive source changed: %s", rel)
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

func openRegularNoFollow(filename string) (*os.File, error) {
	fd, err := unix.Open(filename, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), filename)
	if f == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open archive source: %s", filename)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("archive source is not regular: %s", filename)
	}
	return f, nil
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
