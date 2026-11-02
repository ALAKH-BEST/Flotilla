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
