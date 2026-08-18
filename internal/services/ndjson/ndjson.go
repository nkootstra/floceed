// Package ndjson provides a bounded, hashed NDJSON artifact writer shared by
// the stream and message adapters (SQS, Kinesis). Capture output is written to
// a temporary sibling and renamed into place on Commit, so an interrupted
// capture cannot leave a partially written artifact behind.
package ndjson

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nkootstra/floceed/internal/model"
)

// Writer writes one NDJSON artifact under an artifact root while hashing the
// bytes and bounding the total size. It must be committed via Commit; Abort
// discards the temporary file.
type Writer struct {
	file        *os.File
	temp        string
	destination string
	buf         *bufio.Writer
	hash        hash.Hash
	path        string
	maxBytes    int64
	size        int64
}

// Create prepares a new artifact at name (a safe relative bundle path) under
// artifactRoot. maxBytes bounds the total encoded size including each newline;
// 0 or negative disables the bound.
func Create(artifactRoot, name string, maxBytes int64) (*Writer, error) {
	clean := filepath.Clean(filepath.FromSlash(name))
	if filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("unsafe artifact path %q: %w", name, model.ErrValidation)
	}
	destination := filepath.Join(artifactRoot, clean)
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(filepath.Dir(destination), ".ndjson-")
	if err != nil {
		return nil, err
	}
	_ = file.Chmod(0o600)
	hash := sha256.New()
	return &Writer{
		file:        file,
		temp:        file.Name(),
		destination: destination,
		buf:         bufio.NewWriter(io.MultiWriter(file, hash)),
		hash:        hash,
		path:        filepath.ToSlash(name),
		maxBytes:    maxBytes,
	}, nil
}

// Write appends value followed by a newline when it fits within the remaining
// byte budget. It returns false when the record would exceed maxBytes and
// nothing is written.
func (w *Writer) Write(value []byte) (bool, error) {
	next := w.size + int64(len(value)) + 1
	if w.maxBytes > 0 && next > w.maxBytes {
		return false, nil
	}
	if _, err := w.buf.Write(value); err != nil {
		return false, err
	}
	if err := w.buf.WriteByte('\n'); err != nil {
		return false, err
	}
	w.size = next
	return true, nil
}

// Size reports the number of bytes written so far, including newlines.
func (w *Writer) Size() int64 { return w.size }

// Commit flushes, syncs, and atomically renames the artifact into place,
// returning its reference. Commit must be called exactly once.
func (w *Writer) Commit() (model.ArtifactRef, error) {
	if err := w.buf.Flush(); err != nil {
		w.abort()
		return model.ArtifactRef{}, err
	}
	if err := w.file.Sync(); err != nil {
		w.abort()
		return model.ArtifactRef{}, err
	}
	if err := w.file.Close(); err != nil {
		w.abort()
		return model.ArtifactRef{}, err
	}
	if err := os.Rename(w.temp, w.destination); err != nil {
		_ = os.Remove(w.temp)
		return model.ArtifactRef{}, err
	}
	return model.ArtifactRef{Path: w.path, SHA256: hex.EncodeToString(w.hash.Sum(nil)), Size: w.size}, nil
}

// Abort closes and removes the temporary artifact, discarding all writes.
func (w *Writer) Abort() {
	w.abort()
}

func (w *Writer) abort() {
	_ = w.file.Close()
	_ = os.Remove(w.temp)
}
