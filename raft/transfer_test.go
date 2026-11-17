package raft_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
)

func other(leader, n int) int { return (leader + 1) % n }

// Handing over makes exactly the chosen node the leader, quickly (no election timeout is waited out),
// with a higher term, and the cluster keeps working.
func TestTransferMakesTheTargetLeader(t *testing.T) {
	c := newCluster(t, 3)
	c.put("a", "1", 3)
	for round := 0; round < 3; round++ {
		leader := c.checkOneLeader()
		target := other(leader, 3)
		before := c.nodes[leader].Status().Term

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := c.nodes[leader].TransferLeadership(ctx, target)
		cancel()
		if err != nil {
			t.Fatalf("round %d: transfer from %d to %d: %v", round, leader, target, err)
		}
		if took := time.Since(start); took > time.Second {
			t.Fatalf("round %d: the transfer took %v", round, took)
		}
		if got := c.checkOneLeader(); got != target {
			t.Fatalf("round %d: node %d leads, want %d", round, got, target)
		}
		if after := c.nodes[target].Status().Term; after <= before {
			t.Fatalf("round %d: term %d did not go up from %d", round, after, before)
		}
		c.put(fmt.Sprintf("k%d", round), "v", 3)
	}
}

// A target that is behind is caught up first: once it leads it has every committed entry.
func TestTransferWaitsForALaggingTarget(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()
	target := other(leader, 3)
	c.isolate(target)
	var last int
	for i := 0; i < 30; i++ {
		last = c.put(fmt.Sprintf("k%d", i), "v", 2)
	}
	c.reconnect(target)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.nodes[leader].TransferLeadership(ctx, target); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if got := c.checkOneLeader(); got != target {
		t.Fatalf("node %d leads, want %d", got, target)
	}
	c.waitApplied(target, last)
	c.waitStoresEqual()
}

// If the target never takes over, the leader gives up after the timeout and carries on. While the
// transfer is pending it takes no new commands, and a second transfer is refused.
func TestTransferToUnreachableTargetIsCancelled(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()
	target := other(leader, 3)
	c.isolate(target)
	term := c.nodes[leader].Status().Term

	result := make(chan error, 1)
	go func() { result <- c.nodes[leader].TransferLeadership(context.Background(), target) }()

	// While it waits: no new commands, and no second transfer.
	deadline := time.Now().Add(time.Second)
	for {
		if _, _, ok := c.nodes[leader].Propose([]byte("x")); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the leader still took commands during a transfer")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.nodes[leader].TransferLeadership(context.Background(), target); !errors.Is(err, raft.ErrTransferInProgress) {
		t.Fatalf("second transfer: %v, want ErrTransferInProgress", err)
	}

	select {
	case err := <-result:
		if !errors.Is(err, raft.ErrTransferTimeout) {
			t.Fatalf("transfer: %v, want ErrTransferTimeout", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("the transfer never gave up")
	}
	if st := c.nodes[leader].Status(); st.Role != raft.Leader || st.Term != term {
		t.Fatalf("after the failed transfer the node is %v in term %d, want leader in term %d", st.Role, st.Term, term)
	}
	c.put("after", "1", 2) // it takes commands again
}

// Cancelling the caller's context cancels the transfer too.
func TestTransferStopsWhenTheCallerGivesUp(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()
	target := other(leader, 3)
	c.isolate(target)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.nodes[leader].TransferLeadership(ctx, target); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("transfer: %v, want the context's deadline error", err)
	}
	c.put("after", "1", 2)
}

func TestTransferRefusesBadRequests(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()
	follower := other(leader, 3)
	ctx := context.Background()

	if err := c.nodes[follower].TransferLeadership(ctx, leader); !errors.Is(err, raft.ErrNotLeader) {
		t.Errorf("from a follower: %v, want ErrNotLeader", err)
	}
	if err := c.nodes[leader].TransferLeadership(ctx, 9); !errors.Is(err, raft.ErrBadTransferTarget) {
		t.Errorf("to a stranger: %v, want ErrBadTransferTarget", err)
	}
	if err := c.nodes[leader].TransferLeadership(ctx, leader); err != nil {
		t.Errorf("to itself: %v, want nil", err)
	}
	if got := c.checkOneLeader(); got != leader {
		t.Errorf("node %d leads after the no-op transfer, want %d", got, leader)
	}
}

// A learner cannot be handed leadership: it does not vote or campaign.
func TestTransferRefusesALearner(t *testing.T) {
	c := newMemberCluster(t, 3, 4, 0)
	leader := c.checkOneLeader()
	c.joining[3] = true
	c.start(3)
	if _, _, err := c.nodes[leader].AddLearner(raft.Member{ID: 3}); err != nil {
		t.Fatal(err)
	}
	if err := c.nodes[leader].TransferLeadership(context.Background(), 3); !errors.Is(err, raft.ErrBadTransferTarget) {
		t.Fatalf("to a learner: %v, want ErrBadTransferTarget", err)
	}
}

// A timeout-now only counts from the current leader in the current term. Anything else must not
// start an election, or a stale or forged message could unseat a healthy leader.
func TestTimeoutNowIsIgnoredUnlessFromTheCurrentLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()
	follower := other(leader, 3)
	third := other(follower, 3)
	if third == leader {
		third = other(third, 3)
	}
	st := c.nodes[follower].Status()

	c.nodes[follower].HandleTimeoutNow(&raft.TimeoutNowArgs{Term: st.Term, LeaderID: third})      // not the leader
	c.nodes[follower].HandleTimeoutNow(&raft.TimeoutNowArgs{Term: st.Term - 1, LeaderID: leader}) // old term
	c.nodes[follower].HandleTimeoutNow(&raft.TimeoutNowArgs{Term: st.Term + 1, LeaderID: leader}) // future term
	time.Sleep(100 * time.Millisecond)
	if now := c.nodes[follower].Status(); now.Term != st.Term || now.Role != raft.Follower {
		t.Fatalf("the follower is %v in term %d after bogus messages, want follower in term %d", now.Role, now.Term, st.Term)
	}
	if got := c.checkOneLeader(); got != leader {
		t.Fatalf("node %d leads, want %d", got, leader)
	}
}
