package raft_test

import (
	"sync"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()
	term := c.nodes[leader].Status().Term

	// With a healthy network the leader and term must stay put.
	time.Sleep(time.Second)
	if again := c.checkOneLeader(); again != leader {
		t.Fatalf("leader changed from %d to %d with no failures", leader, again)
	}
	if now := c.nodes[leader].Status().Term; now != term {
		t.Fatalf("term moved from %d to %d with no failures", term, now)
	}
}

func TestReElectionAfterLeaderFailure(t *testing.T) {
	c := newCluster(t, 3)
	first := c.checkOneLeader()

	c.isolate(first)
	second := c.checkOneLeader()
	if second == first {
		t.Fatal("isolated leader was not replaced")
	}

	// The old leader rejoins, learns the new term and steps down.
	c.reconnect(first)
	time.Sleep(500 * time.Millisecond)
	if got := c.checkOneLeader(); got != second {
		t.Fatalf("leader is %d after rejoin, want %d", got, second)
	}
	if st := c.nodes[first].Status(); st.Role != raft.Follower {
		t.Fatalf("old leader is %s after rejoin, want follower", st.Role)
	}
}

func TestNoLeaderWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()
	for i := 0; i < 3; i++ {
		if i != leader {
			c.isolate(i)
		}
	}
	// The remaining leader can still call itself leader until it hears of a newer term,
	// but with the others gone it must not be able to commit anything.
	index, _, ok := c.nodes[leader].Propose(cmd("lonely"))
	if !ok {
		t.Fatal("expected the old leader to accept the proposal")
	}
	time.Sleep(time.Second)
	if c.appliedCount(index, cmd("lonely")) != 0 {
		t.Fatal("an entry committed without a majority")
	}
}

func TestAgreement(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader()
	for i := 0; i < 10; i++ {
		c.submit(cmd("cmd-%d", i), 3)
	}
}

func TestFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()
	follower := (leader + 1) % 3

	c.submit(cmd("before"), 3)
	c.isolate(follower)
	last := 0
	for i := 0; i < 5; i++ {
		last = c.submit(cmd("while-away-%d", i), 2)
	}
	c.reconnect(follower)
	c.waitApplied(follower, last)
}

// A leader cut off from the majority keeps accepting proposals, but they can never commit,
// and they are discarded when the partition heals.
func TestMinorityLeaderEntriesAreDiscarded(t *testing.T) {
	c := newCluster(t, 5)
	old := c.checkOneLeader()
	c.submit(cmd("committed-before"), 5)

	// Put the leader and one follower on the minority side.
	minority := []int{old, (old + 1) % 5}
	majority := []int{(old + 2) % 5, (old + 3) % 5, (old + 4) % 5}
	c.net.Partition(minority, majority)
	for _, i := range minority {
		c.cut[i] = true
	}

	staleIndex, _, ok := c.nodes[old].Propose(cmd("stale"))
	if !ok {
		t.Fatal("the old leader should still accept proposals")
	}

	// The majority side elects its own leader and keeps working.
	for _, i := range majority {
		c.cut[i] = false
	}
	for _, i := range minority {
		c.cut[i] = true
	}
	newIndex := c.submit(cmd("on-majority"), 3)

	if c.appliedCount(staleIndex, cmd("stale")) != 0 {
		t.Fatal("an entry committed on the minority side")
	}

	c.net.Heal()
	for i := range c.cut {
		c.cut[i] = false
	}
	for i := 0; i < 5; i++ {
		c.waitApplied(i, newIndex)
	}
	if n := c.appliedCount(staleIndex, cmd("stale")); n != 0 {
		t.Fatalf("the stale entry was applied on %d nodes after healing", n)
	}
	c.submit(cmd("after-heal"), 5)
}

func TestRestartKeepsCommittedEntries(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader()
	last := 0
	for i := 0; i < 5; i++ {
		last = c.submit(cmd("persist-%d", i), 3)
	}

	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.restart(i)
	}

	c.checkOneLeader()
	// New work still commits, and the old entries are replayed to every node.
	idx := c.submit(cmd("after-restart"), 3)
	if idx <= last {
		t.Fatalf("new entry got index %d, expected after %d", idx, last)
	}
	for i := 0; i < 3; i++ {
		c.waitApplied(i, idx)
	}
}

func TestLeaderCrashAndRestart(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()
	c.submit(cmd("one"), 3)

	c.crash(leader)
	c.checkOneLeader()
	idx := c.submit(cmd("two"), 2)

	c.restart(leader)
	c.waitApplied(leader, idx)
}

func TestUnreliableNetwork(t *testing.T) {
	c := newCluster(t, 5)
	c.checkOneLeader()
	c.net.SetDropRate(0.15)
	c.net.SetDelay(0, 15*time.Millisecond)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				c.submit(cmd("w%d-%d", w, i), 3)
			}
		}(w)
	}
	wg.Wait()

	// Once the network recovers every node must reach the same log.
	c.net.SetDropRate(0)
	c.net.SetDelay(0, 0)
	final := c.submit(cmd("final"), 5)
	for i := 0; i < 5; i++ {
		c.waitApplied(i, final)
	}
}
