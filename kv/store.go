// Package kv is the key-value state machine that runs on top of Raft.
package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/ALAKH-BEST/flotilla/raft"
)

// Op is a kind of command.
type Op string

const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
	// OpExpire removes a key whose time to live has run out. The leader proposes it; it only
	// takes effect if the key still has the expiry the command names, so a key that was written
	// again in the meantime is left alone.
	OpExpire Op = "expire"
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
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	// ExpireAt is when a put's key should expire, in Unix milliseconds, or 0 for never. For an
	// OpExpire it names the expiry being enforced.
	ExpireAt int64 `json:"expire_at,omitempty"`
	ClientID int64 `json:"client,omitempty"`
	Seq      int64 `json:"seq,omitempty"`
}

// Result is what a command returns. For a get, Value and Found describe the key.
type Result struct {
	Value    string `json:"value,omitempty"`
	Found    bool   `json:"found,omitempty"`
	ExpireAt int64  `json:"expire_at,omitempty"` // for a get: when the key expires, if it does
}

type session struct {
	Seq    int64  `json:"seq"`
	Result Result `json:"result"`
}

type snapshotData struct {
	Data     map[string]string `json:"data"`
	Sessions map[int64]session `json:"sessions"`
	Expires  map[string]int64  `json:"expires,omitempty"`
}

// Event is one change to the data, in the order the log applied it.
type Event struct {
	Index    int    `json:"index"` // the log index of the command that made the change
	Type     string `json:"type"`  // "put", "delete" or "expire" (a delete caused by a time to live)
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ExpireAt int64  `json:"expire_at,omitempty"`
}

// defaultMaxEvents is how many recent events a store keeps for watchers to resume from.
const defaultMaxEvents = 10000

// CompactedError means a watch asked for events the store no longer has. Oldest is the first
// index it can still replay from; a client that needs more must read the keys again and watch
// from the index it read at.
type CompactedError struct{ Oldest int }

func (e *CompactedError) Error() string {
	return fmt.Sprintf("kv: events before index %d are no longer available", e.Oldest)
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
	expires     map[string]int64  // when each key with a time to live expires (Unix ms)
	lastApplied int

	events    []Event // recent changes, oldest first
	maxEvents int
	floor     int // events at or before this index may be missing from events
	watchers  map[*Watcher]struct{}
}

func NewStore() *Store {
	return &Store{
		data:      make(map[string]string),
		sessions:  make(map[int64]session),
		expires:   make(map[string]int64),
		maxEvents: defaultMaxEvents,
		watchers:  make(map[*Watcher]struct{}),
	}
}

// SetMaxEvents sets how many recent changes the store keeps for watchers to resume from.
func (s *Store) SetMaxEvents(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxEvents = max(n, 1)
	s.trim()
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
		// What happened up to here is not known as events: watchers behind it must start again.
		s.events, s.floor = nil, msg.SnapshotIndex
		s.wake()
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
		if c.ExpireAt > 0 {
			s.expires[c.Key] = c.ExpireAt
		} else {
			delete(s.expires, c.Key)
		}
		s.emit(Event{Index: msg.Index, Type: "put", Key: c.Key, Value: c.Value, ExpireAt: c.ExpireAt})
	case OpDelete:
		if _, ok := s.data[c.Key]; ok {
			delete(s.data, c.Key)
			delete(s.expires, c.Key)
			s.emit(Event{Index: msg.Index, Type: "delete", Key: c.Key})
		}
	case OpExpire:
		if at, ok := s.expires[c.Key]; ok && at == c.ExpireAt {
			delete(s.data, c.Key)
			delete(s.expires, c.Key)
			s.emit(Event{Index: msg.Index, Type: "expire", Key: c.Key})
		}
	case OpGet:
		res.Value, res.Found = s.data[c.Key]
		res.ExpireAt = s.expires[c.Key]
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
	b, err := json.Marshal(snapshotData{Data: s.data, Sessions: s.sessions, Expires: s.expires}) // map keys are sorted
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
	if snap.Expires == nil {
		snap.Expires = make(map[string]int64)
	}
	s.data, s.sessions, s.expires = snap.Data, snap.Sessions, snap.Expires
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

// GetEntry is Get with the key's expiry (0 if it has none).
func (s *Store) GetEntry(key string) Result {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return Result{Value: v, Found: ok, ExpireAt: s.expires[key]}
}

// Expiry is a key whose time to live has run out.
type Expiry struct {
	Key      string
	ExpireAt int64
}

// Expired returns up to limit keys whose expiry is at or before now (Unix milliseconds), oldest
// first. Nothing is removed: the leader turns each into an OpExpire command, so every replica
// removes the key at the same point in the log, whatever its own clock says.
func (s *Store) Expired(now int64, limit int) []Expiry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Expiry
	for k, at := range s.expires {
		if at <= now {
			out = append(out, Expiry{k, at})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ExpireAt != out[j].ExpireAt {
			return out[i].ExpireAt < out[j].ExpireAt
		}
		return out[i].Key < out[j].Key
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// emit records a change and wakes the watchers (the lock must be held).
func (s *Store) emit(e Event) {
	s.events = append(s.events, e)
	s.trim()
	s.wake()
}

// trim drops the oldest events beyond the limit, remembering how far back the history reaches.
func (s *Store) trim() {
	if extra := len(s.events) - s.maxEvents; extra > 0 {
		s.floor = s.events[extra-1].Index
		s.events = append([]Event(nil), s.events[extra:]...)
	}
}

func (s *Store) wake() {
	for w := range s.watchers {
		select {
		case w.wake <- struct{}{}:
		default: // a wake-up is already waiting
		}
	}
}

// Watcher follows the changes to the keys a filter selects.
type Watcher struct {
	s     *Store
	match func(key string) bool
	next  int // the lowest index not yet delivered
	start int // the index it started at, after "from now" was resolved
	wake  chan struct{}
}

// Start is the log index the watcher began at: the first index whose change it will report.
func (w *Watcher) Start() int { return w.start }

// Watch starts following changes from log index from (0 means from now). It fails with a
// *CompactedError if events from that index are no longer kept.
func (s *Store) Watch(from int, match func(key string) bool) (*Watcher, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if from <= 0 {
		from = s.lastApplied + 1
	} else if from <= s.floor {
		return nil, &CompactedError{Oldest: s.floor + 1}
	}
	w := &Watcher{s: s, match: match, next: from, start: from, wake: make(chan struct{}, 1)}
	s.watchers[w] = struct{}{}
	return w, nil
}

// maxBatch is the most events Next returns at once.
const maxBatch = 256

// Next waits for the next changes the watcher selects and returns them in log order. It returns
// ctx's error when ctx ends, and a *CompactedError if the watcher fell so far behind that events
// it still needed were dropped.
func (w *Watcher) Next(ctx context.Context) ([]Event, error) {
	for {
		w.s.mu.RLock()
		if w.next <= w.s.floor {
			err := &CompactedError{Oldest: w.s.floor + 1}
			w.s.mu.RUnlock()
			return nil, err
		}
		i := sort.Search(len(w.s.events), func(i int) bool { return w.s.events[i].Index >= w.next })
		var out []Event
		for ; i < len(w.s.events) && len(out) < maxBatch; i++ {
			if w.match == nil || w.match(w.s.events[i].Key) {
				out = append(out, w.s.events[i])
			}
			w.next = w.s.events[i].Index + 1
		}
		w.s.mu.RUnlock()
		if len(out) > 0 {
			return out, nil
		}
		select {
		case <-w.wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Close stops the watcher.
func (w *Watcher) Close() {
	w.s.mu.Lock()
	delete(w.s.watchers, w)
	w.s.mu.Unlock()
}

// LastApplied is the index of the last entry applied.
func (s *Store) LastApplied() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastApplied
}
