// Package kv is the key-value state machine that runs on top of Raft.
package kv

import (
	"encoding/json"
	"sync"

	"github.com/ALAKH-BEST/flotilla/raft"
)

// Op is a kind of command.
type Op string

const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
)

// Command is what a client submits through Raft.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Encode turns a command into the bytes stored in the Raft log.
func Encode(c Command) []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // a Command of plain strings always marshals
	}
	return b
}

// Store applies committed commands in log order. It is deterministic, so every
// replica that applies the same log ends with the same data.
type Store struct {
	mu          sync.RWMutex
	data        map[string]string
	lastApplied int
}

func NewStore() *Store { return &Store{data: make(map[string]string)} }

// Apply applies one committed entry. No-op entries and malformed commands are skipped,
// but every entry still advances the applied index.
func (s *Store) Apply(msg raft.ApplyMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msg.SnapshotValid {
		s.restore(msg.Snapshot)
		s.lastApplied = msg.SnapshotIndex
		return
	}
	s.lastApplied = msg.Index
	if msg.NoOp {
		return
	}
	var c Command
	if err := json.Unmarshal(msg.Command, &c); err != nil {
		return
	}
	switch c.Op {
	case OpPut:
		s.data[c.Key] = c.Value
	case OpDelete:
		delete(s.data, c.Key)
	}
}

// Snapshot serializes the store's data. The output is deterministic, so replicas
// that applied the same log produce identical bytes.
func (s *Store) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := json.Marshal(s.data) // map keys are sorted
	if err != nil {
		panic(err) // a map of strings always marshals
	}
	return b
}

// restore replaces the data with a snapshot (the lock must be held).
func (s *Store) restore(snapshot []byte) {
	data := make(map[string]string)
	if len(snapshot) > 0 {
		if err := json.Unmarshal(snapshot, &data); err != nil {
			return // a snapshot written by Snapshot always parses; keep the old data otherwise
		}
	}
	s.data = data
}

// Data returns a copy of all key-value pairs, for comparing replicas.
func (s *Store) Data() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Get returns the value for a key from this replica's applied state. It is not a
// linearizable read: a replica can lag behind the leader.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// LastApplied is the index of the last entry applied.
func (s *Store) LastApplied() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastApplied
}
