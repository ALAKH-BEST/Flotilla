package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
)

type noTransport struct{}

func (noTransport) RequestVote(int, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, errors.New("no peers")
}

func (noTransport) PreVote(int, *raft.PreVoteArgs) (*raft.PreVoteReply, error) {
	return nil, errors.New("no peers")
}

func (noTransport) AppendEntries(int, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, errors.New("no peers")
}

func (noTransport) InstallSnapshot(int, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return nil, errors.New("no peers")
}

// A read must wait until the state machine has applied everything up to the read index. With
// the apply loop not yet running, the leader has committed entries the store has not seen,
// so answering now would be a stale read.
func TestReadWaitsForTheStateMachine(t *testing.T) {
	s, err := New(Config{
		Raft: raft.Config{
			ID: 0, Peers: []int{0},
			Transport: noTransport{},
			Storage:   raft.NewMemoryStorage(),
		},
		RequestTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	s.node.Start() // the node only; the apply loop is started later

	limit := time.Now().Add(3 * time.Second)
	for st := s.node.Status(); st.Role != raft.Leader || st.CommitIndex < 1; st = s.node.Status() {
		if time.Now().After(limit) {
			t.Fatal("the single node never committed its first entry")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if _, err := s.readLocal(context.Background(), "k"); !errors.Is(err, ErrTimeout) {
		t.Fatalf("read before the state machine caught up returned %v, want ErrTimeout", err)
	}

	s.wg.Add(1)
	go s.applyLoop()
	if _, err := s.readLocal(context.Background(), "k"); err != nil {
		t.Fatalf("read after the state machine caught up failed: %v", err)
	}
}
