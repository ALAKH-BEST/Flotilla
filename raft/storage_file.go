package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage keeps State in a file. Each Save writes a new file, flushes it to disk and
// renames it over the old one, so after a crash the file holds either the previous state
// or the new one, never a mix.
//
// Every Save rewrites the whole state, so it is only suitable while the log stays short,
// which snapshots keep true.
type FileStorage struct {
	mu   sync.Mutex
	dir  string
	path string
}

const stateFileName = "raft-state.gob"

// NewFileStorage keeps state in dir, creating it if needed.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: create data dir: %w", err)
	}
	path := filepath.Join(dir, stateFileName)
	_ = os.Remove(path + ".tmp") // left over from a crash during a save; the real file is intact
	return &FileStorage{dir: dir, path: path}, nil
}

func (f *FileStorage) Save(state State) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		return fmt.Errorf("raft: encode state: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	tmp := f.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raft: save state: %w", err)
	}
	if _, err := file.Write(buf.Bytes()); err != nil {
		file.Close()
		return fmt.Errorf("raft: save state: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("raft: save state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("raft: save state: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("raft: save state: %w", err)
	}
	// Flush the directory entry so the rename itself survives a crash.
	if dir, err := os.Open(f.dir); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return nil
}

func (f *FileStorage) Load() (State, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, fmt.Errorf("raft: load state: %w", err)
	}
	var state State
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		return State{}, false, fmt.Errorf("raft: state file %s is corrupt: %w", f.path, err)
	}
	return state, true, nil
}
