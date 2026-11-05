package raft_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
)

// join starts node i with no members; it takes part only once the leader adds it.
func (c *cluster) join(i int) {
	c.joining[i] = true
	c.start(i)
}

// changeMembers runs a membership change on whoever is leader, retrying while a new leader settles,
// and waits until the leader has committed the change. It returns the change's log index.
func (c *cluster) changeMembers(op func(*raft.Node) (int, int, error)) int {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			if !c.reachable(i) {
				continue
			}
			index, term, err := op(c.nodes[i])
			switch {
			case errors.Is(err, raft.ErrNotLeader):
				continue
			case errors.Is(err, raft.ErrNotReady), errors.Is(err, raft.ErrChangeInProgress):
			case err != nil:
				c.t.Fatalf("membership change on node %d: %v", i, err)
			default:
				until := time.Now().Add(3 * time.Second)
				for time.Now().Before(until) {
					st := c.nodes[i].Status()
					if st.Term == term && st.CommitIndex >= index {
						return index
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatal("the membership change did not commit in time")
	return -1
}

func (c *cluster) add(i int) int {
	c.t.Helper()
	c.join(i)
	return c.changeMembers(func(n *raft.Node) (int, int, error) { return n.AddServer(raft.Member{ID: i}) })
}

func (c *cluster) remove(i int) int {
	c.t.Helper()
	return c.changeMembers(func(n *raft.Node) (int, int, error) { return n.RemoveServer(i) })
}

// waitMembers waits until node i reports exactly these members.
func (c *cluster) waitMembers(i int, want ...int) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := c.nodes[i].Status().Members
		if reflect.DeepEqual(got, want) {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("node %d has members %v, want %v", i, got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestGrowAndShrinkTheCluster(t *testing.T) {
	c := newMemberCluster(t, 3, 5, 0)
	c.checkOneLeader()
	c.put("a", "1", 3)

	c.waitApplied(0, c.add(3))
	c.waitApplied(3, c.put("b", "2", 4)) // the new node gets the whole history and takes part
	c.add(4)
	c.put("c", "3", 5)
	for i := 0; i < 5; i++ {
		c.waitMembers(i, 0, 1, 2, 3, 4)
	}
	data := c.waitStoresEqual()
	if data["a"] != "1" || data["b"] != "2" || data["c"] != "3" {
		t.Fatalf("data after growing: %v", data)
	}

	// Shrink back to three. The removed nodes keep running and never hear about it, which is the
	// case that can disrupt a cluster: they time out and campaign with ever higher terms.
	leader := c.checkOneLeader()
	var removed []int
	for i := 4; i >= 0 && len(removed) < 2; i-- {
		if i != leader {
			removed = append(removed, i)
		}
	}
	for _, i := range removed {
		c.remove(i)
	}
	term := c.nodes[leader].Status().Term
	time.Sleep(2 * time.Second) // many election timeouts
	if st := c.nodes[leader].Status(); st.Role != raft.Leader || st.Term != term {
		t.Fatalf("a removed node disturbed the cluster: leader now %+v, term was %d", st, term)
	}
	c.put("d", "4", 3)
}

func TestLeaderCanRemoveItself(t *testing.T) {
	c := newMemberCluster(t, 3, 3, 0)
	old := c.checkOneLeader()
	c.put("x", "1", 3)
	c.remove(old)

	// The remaining two elect a leader among themselves and keep serving.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if l := c.nodes[old].Status(); l.Role != raft.Leader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the removed leader never stepped down")
		}
		time.Sleep(20 * time.Millisecond)
	}
	var next int = -1
	for time.Now().Before(deadline) && next < 0 {
		for i := 0; i < 3; i++ {
			if i != old && c.nodes[i].Status().Role == raft.Leader {
				next = i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if next < 0 {
		t.Fatal("the remaining nodes elected no leader")
	}
	c.put("y", "2", 2)
	termAfter := c.nodes[old].Status().Term
	time.Sleep(time.Second)
	if st := c.nodes[old].Status(); st.Role == raft.Leader || st.Term != termAfter {
		t.Fatalf("the removed node kept campaigning: %+v (term was %d)", st, termAfter)
	}
	for i := 0; i < 3; i++ {
		if i != old {
			c.waitMembers(i, otherTwo(old)...)
		}
	}
}

func otherTwo(x int) []int {
	var out []int
	for i := 0; i < 3; i++ {
		if i != x {
			out = append(out, i)
		}
	}
	return out
}

func TestMembershipChangeRules(t *testing.T) {
	c := newMemberCluster(t, 3, 5, 0)
	leader := c.checkOneLeader()
	c.put("k", "v", 3)

	follower := (leader + 1) % 3
	if _, _, err := c.nodes[follower].AddServer(raft.Member{ID: 3}); !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("AddServer on a follower: %v, want ErrNotLeader", err)
	}
	if _, _, err := c.nodes[leader].AddServer(raft.Member{ID: 1}); !errors.Is(err, raft.ErrAlreadyMember) {
		t.Fatalf("adding an existing member: %v, want ErrAlreadyMember", err)
	}
	if _, _, err := c.nodes[leader].RemoveServer(9); !errors.Is(err, raft.ErrNotMember) {
		t.Fatalf("removing a stranger: %v, want ErrNotMember", err)
	}

	// With both followers unreachable the change cannot commit, so a second one must be refused.
	// Two changes in flight at once could let two disjoint majorities form.
	c.isolate(follower)
	c.isolate((leader + 2) % 3)
	if _, _, err := c.nodes[leader].AddServer(raft.Member{ID: 3}); err != nil {
		t.Fatalf("first change: %v", err)
	}
	if _, _, err := c.nodes[leader].AddServer(raft.Member{ID: 4}); !errors.Is(err, raft.ErrChangeInProgress) {
		t.Fatalf("second change while the first is uncommitted: %v, want ErrChangeInProgress", err)
	}
	if _, _, err := c.nodes[leader].RemoveServer(follower); !errors.Is(err, raft.ErrChangeInProgress) {
		t.Fatalf("removal while a change is uncommitted: %v, want ErrChangeInProgress", err)
	}
}

func TestCannotRemoveTheLastMember(t *testing.T) {
	c := newMemberCluster(t, 1, 1, 0)
	c.checkOneLeader()
	if _, _, err := c.nodes[0].RemoveServer(0); !errors.Is(err, raft.ErrLastMember) {
		t.Fatalf("removing the only member: %v, want ErrLastMember", err)
	}
}

// A membership entry that was never committed disappears with the leader that wrote it, and its
// effect must disappear too.
func TestUncommittedMembershipChangeIsUndone(t *testing.T) {
	c := newMemberCluster(t, 3, 4, 0)
	old := c.checkOneLeader()
	c.put("k", "v", 3)

	c.isolate(old)
	if _, _, err := c.nodes[old].AddServer(raft.Member{ID: 3}); err != nil {
		t.Fatal(err)
	}
	if got := c.nodes[old].Status().Members; len(got) != 4 {
		t.Fatalf("the leader should use the new membership at once, has %v", got)
	}
	c.put("k2", "v2", 2) // the others elect a new leader and commit something else
	c.reconnect(old)

	c.waitMembers(old, 0, 1, 2)
	c.waitStoresEqual()
	c.checkOneLeader()
}

// Membership is part of what a snapshot remembers, so a node restarted after the log has been
// compacted, and a node that joins after it, both end up with the right cluster.
func TestMembershipSurvivesSnapshotsAndRestarts(t *testing.T) {
	c := newMemberCluster(t, 3, 5, 10)
	c.checkOneLeader()
	c.add(3)
	for i := 0; i < 40; i++ { // enough entries to compact the log past the membership entry
		c.put(fmt.Sprintf("k%d", i), "v", 4)
	}
	if st := c.nodes[c.checkOneLeader()].Status(); st.SnapshotIndex == 0 {
		t.Fatal("the log was never compacted, so this test proves nothing")
	}

	c.crash(1)
	c.restart(1)
	c.crash(3)
	c.restart(3)
	for _, i := range []int{0, 1, 2, 3} {
		c.waitMembers(i, 0, 1, 2, 3)
	}

	// Node 4 is added while it cannot be reached, and the log is compacted past that membership
	// entry before it reconnects. The membership can then only reach it inside the snapshot.
	c.join(4)
	c.isolate(4)
	addIndex := c.changeMembers(func(n *raft.Node) (int, int, error) { return n.AddServer(raft.Member{ID: 4}) })
	for i := 0; i < 40; i++ {
		c.put(fmt.Sprintf("m%d", i), "v", 4)
	}
	if st := c.nodes[c.checkOneLeader()].Status(); st.SnapshotIndex < addIndex {
		t.Fatalf("the log is only compacted to %d, not past the membership entry at %d", st.SnapshotIndex, addIndex)
	}
	c.reconnect(4)
	for i := 0; i < 5; i++ {
		c.waitMembers(i, 0, 1, 2, 3, 4)
	}
	c.put("last", "x", 5)
	c.mu.Lock()
	got := c.installed[4]
	c.mu.Unlock()
	if got == 0 {
		t.Fatal("node 4 should have been brought up to date with a snapshot")
	}
	c.waitStoresEqual()
}

// After the cluster grows to five, two nodes are not a majority any more.
func TestCommitNeedsAMajorityOfTheCurrentMembership(t *testing.T) {
	c := newMemberCluster(t, 3, 5, 0)
	c.checkOneLeader()
	c.add(3)
	c.add(4)
	leader := c.checkOneLeader()

	cut := 0
	for i := 0; i < 5 && cut < 3; i++ {
		if i != leader {
			c.isolate(i)
			cut++
		}
	}
	index, _, ok := c.nodes[leader].Propose(cmd("two of five"))
	if !ok {
		t.Fatal("the leader refused the proposal")
	}
	time.Sleep(time.Second)
	if st := c.nodes[leader].Status(); st.CommitIndex >= index {
		t.Fatalf("the leader committed index %d with only 2 of 5 nodes reachable", index)
	}

	c.heal()
	c.put("after", "heal", 5) // with a majority again, everything commits
}
