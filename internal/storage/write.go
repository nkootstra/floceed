package storage

import (
	"os"
	"path/filepath"
)

// WriteFileSync writes data to path atomically (temp file + rename) and makes
// the content durable before the rename so a crash cannot leave a renamed file
// with torn contents. Checkpoints rely on this: a referenced file must either
// be absent or fully readable on resume.
func WriteFileSync(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".sync-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	_ = tmp.Chmod(0o600)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return SyncDir(dir)
}

// SyncDir makes a directory-entry update durable after a rename or create.
func SyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
