package raft_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
	"github.com/ALAKH-BEST/flotilla/simnet"
)

// noAppend lets a node win elections but never get an AppendEntries through, so the entry it
// appends on taking office can never commit.
type noAppend struct{ raft.Transport }

func (noAppend) AppendEntries(int, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, simnet.ErrUnreachable
}

func (noAppend) InstallSnapshot(int, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return nil, simnet.ErrUnreachable
}

// A new leader's commit index can be behind entries that earlier leaders committed, until it
// commits an entry of its own term. Until then it must not hand out a read index.
func TestNewLeaderRefusesReadIndexUntilItCommitsInItsTerm(t *testing.T) {
	net := simnet.New(3)
	var nodes []*raft.Node
	for i := 0; i < 3; i++ {
		cfg := raft.Config{
			ID: i, Peers: []int{0, 1, 2},
			Transport: net.Transport(i),
			Storage:   raft.NewMemoryStorage(),
			ApplyCh:   make(chan raft.ApplyMsg, 64),
			Seed:      int64(i + 1),
		}
		if i == 0 {
			cfg.Transport = noAppend{net.Transport(i)}
			cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax = 100*time.Millisecond, 150*time.Millisecond
		} else {
			cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax = 3*time.Second, 4*time.Second
		}
		node, err := raft.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		net.Register(i, node)
		nodes = append(nodes, node)
	}
	for _, n := range nodes {
		n.Start()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	limit := time.Now().Add(2 * time.Second)
	for nodes[0].Status().Role != raft.Leader {
		if time.Now().After(limit) {
			t.Fatal("node 0 never became leader")
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if _, err := nodes[0].ReadIndex(ctx); !errors.Is(err, raft.ErrNotReady) {
		t.Fatalf("ReadIndex on a leader that has committed nothing in its term returned %v, want ErrNotReady", err)
	}
	if _, err := nodes[1].ReadIndex(ctx); !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("ReadIndex on a follower returned %v, want ErrNotLeader", err)
	}
}
