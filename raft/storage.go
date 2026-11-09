package raft

import "sync"

// State is what a node must keep across restarts (Figure 2 of the Raft paper) plus
// its latest snapshot. Log[0] is a sentinel standing for the entry at SnapshotIndex
// (index 0 when there is no snapshot), so Log[i] is the entry at SnapshotIndex+i.
// Everything is saved together so a snapshot and the log that follows it never disagree.
type State struct {
	CurrentTerm   int
	VotedFor      int
	Log           []Entry
	SnapshotIndex int
	SnapshotTerm  int
	Snapshot      []byte
	// SnapshotMembers is the cluster membership as of SnapshotIndex.
	SnapshotMembers []Member
	// SnapshotGone are recently removed servers that may not know it yet, with their addresses,
	// as of SnapshotIndex. A leader elected later keeps telling them after the removal entry itself
	// has been compacted away.
	SnapshotGone []Member
}

func (s State) clone() State {
	s.Log = append([]Entry(nil), s.Log...)
	s.Snapshot = append([]byte(nil), s.Snapshot...)
	s.SnapshotMembers = append([]Member(nil), s.SnapshotMembers...)
	s.SnapshotGone = append([]Member(nil), s.SnapshotGone...)
	return s
}

// Storage persists State. Save must not return until the state is durable.
type Storage interface {
	Save(State) error
	// Load returns the saved state, or ok=false if nothing has been saved yet.
	Load() (state State, ok bool, err error)
}

// MemoryStorage keeps State in memory. It survives a simulated crash because the
// test harness hands the same MemoryStorage to the restarted node.
type MemoryStorage struct {
	mu    sync.Mutex
	state State
	saved bool
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Save(state State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state.clone()
	m.saved = true
	return nil
}

func (m *MemoryStorage) Load() (State, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.saved {
		return State{}, false, nil
	}
	return m.state.clone(), true, nil
}
