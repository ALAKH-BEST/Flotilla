package raft_test

import (
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
)

// A node that loses its connection cannot win a pre-vote, so it never raises its term, and its return
// does not disturb a leader that everyone else could reach all along.
func TestIsolatedFollowerDoesNotInflateItsTerm(t *testing.T) {
	c := newMemberCluster(t, 3, 3, 0)
	leader := c.checkOneLeader()
	c.put("k", "v", 3)
	term := c.nodes[leader].Status().Term
	f := (leader + 1) % 3

	c.isolate(f)
	time.Sleep(2 * time.Second) // many election timeouts
	if got := c.nodes[f].Status().Term; got != term {
		t.Fatalf("an isolated follower's term went from %d to %d", term, got)
	}

	c.reconnect(f)
	time.Sleep(time.Second)
	if st := c.nodes[leader].Status(); st.Role != raft.Leader || st.Term != term {
		t.Fatalf("the follower's return disturbed the leader: %+v (term was %d)", st, term)
	}
	c.put("k2", "v2", 3)
}

// A leader that was cut off steps down, and on returning must follow the leader that replaced it
// rather than force a new election.
func TestReturningLeaderDoesNotForceAnElection(t *testing.T) {
	c := newMemberCluster(t, 3, 3, 0)
	old := c.checkOneLeader()
	c.put("k", "v", 3)

	c.isolate(old)
	c.put("k2", "v2", 2) // the others elect a new leader
	var next int
	for i := 0; i < 3; i++ {
		if i != old && c.nodes[i].Status().Role == raft.Leader {
			next = i
		}
	}
	term := c.nodes[next].Status().Term
	time.Sleep(time.Second) // the old leader has stepped down and is trying to campaign
	c.reconnect(old)
	time.Sleep(time.Second)
	if st := c.nodes[next].Status(); st.Role != raft.Leader || st.Term != term {
		t.Fatalf("the returning node forced an election: %+v (term was %d)", st, term)
	}
	c.waitStoresEqual()
}

// What a node answers to a pre-vote.
func TestPreVoteRules(t *testing.T) {
	c := newMemberCluster(t, 3, 3, 0)
	leader := c.checkOneLeader()
	c.put("k", "v", 3)
	f := (leader + 1) % 3
	other := (leader + 2) % 3
	st := c.nodes[f].Status()
	fresh := &raft.PreVoteArgs{Term: st.Term + 1, CandidateID: other, LastLogIndex: st.LastLogIndex + 5, LastLogTerm: st.Term}

	// Still hearing from a live leader: no, even to a candidate that is entirely up to date.
	if r := c.nodes[f].HandlePreVote(fresh); r.VoteGranted {
		t.Fatal("a node that is hearing from a leader granted a pre-vote")
	}

	// With the leader and the other node gone, nothing is heard any more.
	c.crash(leader)
	c.crash(other)
	time.Sleep(500 * time.Millisecond)

	stale := &raft.PreVoteArgs{Term: st.Term + 1, CandidateID: other, LastLogIndex: 0, LastLogTerm: 0}
	if r := c.nodes[f].HandlePreVote(stale); r.VoteGranted {
		t.Fatal("granted a pre-vote to a candidate whose log is behind")
	}
	now := c.nodes[f].Status()
	notAhead := &raft.PreVoteArgs{Term: now.Term, CandidateID: other, LastLogIndex: now.LastLogIndex + 5, LastLogTerm: now.Term}
	if r := c.nodes[f].HandlePreVote(notAhead); r.VoteGranted {
		t.Fatal("granted a pre-vote for a term that is not ahead of ours")
	}
	fresh = &raft.PreVoteArgs{Term: now.Term + 1, CandidateID: other, LastLogIndex: now.LastLogIndex + 5, LastLogTerm: now.Term}
	if r := c.nodes[f].HandlePreVote(fresh); !r.VoteGranted {
		t.Fatal("refused a pre-vote to an up-to-date candidate when no leader is heard from")
	}
	if after := c.nodes[f].Status(); after.Term != now.Term {
		t.Fatalf("answering a pre-vote changed this node's term: %d -> %d", now.Term, after.Term)
	}
}
