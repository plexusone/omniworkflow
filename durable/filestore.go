package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileStore is a zero-dependency StateStore that keeps one JSON file per run in
// a directory. Writes are atomic (temp file + rename) so a crash mid-write
// never corrupts a snapshot. It is the reference durable store; a production
// deployment may prefer a Postgres- or bbolt-backed StateStore.
type FileStore struct {
	dir string
}

// NewFileStore returns a FileStore rooted at dir, creating the directory if
// needed.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{dir: dir}, nil
}

const fileSuffix = ".run.json"

func (f *FileStore) path(runID string) string {
	// Run IDs are caller-controlled; keep the on-disk name safe and flat.
	safe := strings.NewReplacer("/", "_", "\\", "_", "..", "_").Replace(runID)
	return filepath.Join(f.dir, safe+fileSuffix)
}

func (f *FileStore) Save(_ context.Context, snap Snapshot) error {
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	final := f.path(snap.RunID)
	tmp, err := os.CreateTemp(f.dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, final)
}

func (f *FileStore) Load(_ context.Context, runID string) (Snapshot, bool, error) {
	data, err := os.ReadFile(f.path(runID))
	if os.IsNotExist(err) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return Snapshot{}, false, fmt.Errorf("durable: corrupt snapshot %s: %w", f.path(runID), err)
	}
	return snap, true, nil
}

func (f *FileStore) List(_ context.Context) ([]Snapshot, error) {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, err
	}
	var out []Snapshot
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), fileSuffix) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(f.dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var snap Snapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return nil, fmt.Errorf("durable: corrupt snapshot %s: %w", e.Name(), err)
		}
		out = append(out, snap)
	}
	return out, nil
}
