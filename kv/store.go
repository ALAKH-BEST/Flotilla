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
	// OpGet reads a key. When it is submitted through the log it reads the state at a
	// single point in the agreed order, which makes it linearizable; the server can also
	// serve it without a log entry (see Store.Get).
	OpGet Op = "get"
)

// Command is what a client submits through Raft. A command with a ClientID is applied
// at most once: the store remembers each client's latest Seq and its result, so a
// request that is retried after a timeout or a leader change does not run twice.
// A client must use increasing Seq values and have one request in flight at a time.
type Command struct {
	Op       Op     `json:"op"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ClientID int64  `json:"client,omitempty"`
	Seq      int64  `json:"seq,omitempty"`
}

// Result is what a command returns. For a get, Value and Found describe the key.
type Result struct {
	Value string `json:"value,omitempty"`
	Found bool   `json:"found,omitempty"`
}

type session struct {
	Seq    int64  `json:"seq"`
	Result Result `json:"result"`
}

type snapshotData struct {
	Data     map[string]string `json:"data"`
	Sessions map[int64]session `json:"sessions"`
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
	sessions    map[int64]session // latest request of each client, for exactly-once execution
	lastApplied int
}

func NewStore() *Store {
	return &Store{data: make(map[string]string), sessions: make(map[int64]session)}
}

// Apply applies one committed entry and returns its result. No-op entries and malformed
// commands are skipped, but every entry still advances the applied index. A snapshot
// replaces the whole state.
func (s *Store) Apply(msg raft.ApplyMsg) Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msg.SnapshotValid {
		s.restore(msg.Snapshot)
		s.lastApplied = msg.SnapshotIndex
		return Result{}
	}
	s.lastApplied = msg.Index
	if msg.NoOp {
		return Result{}
	}
	var c Command
	if err := json.Unmarshal(msg.Command, &c); err != nil {
		return Result{}
	}
	if c.ClientID != 0 {
		if prev, ok := s.sessions[c.ClientID]; ok && c.Seq <= prev.Seq {
			if c.Seq == prev.Seq {
				return prev.Result // a retry of the latest request: answer again, do not rerun it
			}
			return Result{} // an old request that was already superseded
		}
	}
	var res Result
	switch c.Op {
	case OpPut:
		s.data[c.Key] = c.Value
	case OpDelete:
		delete(s.data, c.Key)
	case OpGet:
		res.Value, res.Found = s.data[c.Key]
	}
	if c.ClientID != 0 {
		s.sessions[c.ClientID] = session{Seq: c.Seq, Result: res}
	}
	return res
}

// Snapshot serializes the store's data and client sessions. The output is deterministic,
// so replicas that applied the same log produce identical bytes.
func (s *Store) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := json.Marshal(snapshotData{Data: s.data, Sessions: s.sessions}) // map keys are sorted
	if err != nil {
		panic(err) // plain strings and numbers always marshal
	}
	return b
}

// restore replaces the data with a snapshot (the lock must be held).
func (s *Store) restore(snapshot []byte) {
	var snap snapshotData
	if len(snapshot) > 0 {
		if err := json.Unmarshal(snapshot, &snap); err != nil {
			return // a snapshot written by Snapshot always parses; keep the old state otherwise
		}
	}
	if snap.Data == nil {
		snap.Data = make(map[string]string)
	}
	if snap.Sessions == nil {
		snap.Sessions = make(map[int64]session)
	}
	s.data, s.sessions = snap.Data, snap.Sessions
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

// Get returns the value for a key from this replica's applied state, without going through
// the log. On its own it is not a linearizable read, because a replica can lag behind the
// leader; the server uses it only after the read index has confirmed the replica is current.
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
