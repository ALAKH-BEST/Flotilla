package raft

import "sync"

// State is what a node must keep across restarts (Figure 2 of the Raft paper).
// Log includes the sentinel entry at index 0.
type State struct {
	CurrentTerm int
	VotedFor    int
	Log         []Entry
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
	m.state = State{
		CurrentTerm: state.CurrentTerm,
		VotedFor:    state.VotedFor,
		Log:         append([]Entry(nil), state.Log...),
	}
	m.saved = true
	return nil
}

func (m *MemoryStorage) Load() (State, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.saved {
		return State{}, false, nil
	}
	return State{
		CurrentTerm: m.state.CurrentTerm,
		VotedFor:    m.state.VotedFor,
		Log:         append([]Entry(nil), m.state.Log...),
	}, true, nil
}
