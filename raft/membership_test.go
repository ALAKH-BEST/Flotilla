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

// addLearner starts node i and adds it as a non-voting learner.
func (c *cluster) addLearner(i int) int {
	c.t.Helper()
	c.join(i)
	return c.changeMembers(func(n *raft.Node) (int, int, error) { return n.AddLearner(raft.Member{ID: i}) })
}

// waitCaughtUp waits until the leader reports node i has replicated nearly the whole log.
func (c *cluster) waitCaughtUp(i int) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for j := 0; j < c.n; j++ {
			if !c.reachable(j) {
				continue
			}
			if match, commit, err := c.nodes[j].CatchUp(i); err == nil && match > 0 && commit-match <= 2 {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("node %d never caught up", i)
}

func (c *cluster) promote(i int) int {
	c.t.Helper()
	return c.changeMembers(func(n *raft.Node) (int, int, error) { return n.PromoteLearner(i) })
}

// add brings a new node in the way the server does: as a learner first, then as a voter.
func (c *cluster) add(i int) int {
	c.t.Helper()
	c.addLearner(i)
	c.waitCaughtUp(i)
	return c.promote(i)
}

func (c *cluster) waitLearners(i int, want ...int) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := c.nodes[i].Status().Learners
		if reflect.DeepEqual(got, want) || (len(got) == 0 && len(want) == 0) {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("node %d has learners %v, want %v", i, got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
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

	// Shrink back to three. The removed nodes keep running (this test never stops them), which is the
	// case that can disrupt a cluster: they could time out and campaign with ever higher terms.
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
	if _, _, err := c.nodes[follower].AddLearner(raft.Member{ID: 3}); !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("AddLearner on a follower: %v, want ErrNotLeader", err)
	}
	if _, _, err := c.nodes[leader].AddLearner(raft.Member{ID: 1}); !errors.Is(err, raft.ErrAlreadyMember) {
		t.Fatalf("adding an existing member: %v, want ErrAlreadyMember", err)
	}
	if _, _, err := c.nodes[leader].RemoveServer(9); !errors.Is(err, raft.ErrNotMember) {
		t.Fatalf("removing a stranger: %v, want ErrNotMember", err)
	}

	// With both followers unreachable the change cannot commit, so a second one must be refused.
	// Two changes in flight at once could let two disjoint majorities form.
	c.isolate(follower)
	c.isolate((leader + 2) % 3)
	if _, _, err := c.nodes[leader].AddLearner(raft.Member{ID: 3}); err != nil {
		t.Fatalf("first change: %v", err)
	}
	if _, _, err := c.nodes[leader].AddLearner(raft.Member{ID: 4}); !errors.Is(err, raft.ErrChangeInProgress) {
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
	if _, _, err := c.nodes[old].AddLearner(raft.Member{ID: 3}); err != nil {
		t.Fatal(err)
	}
	if got := c.nodes[old].Status().Learners; len(got) != 1 {
		t.Fatalf("the leader should use the new membership at once, has learners %v", got)
	}
	c.put("k2", "v2", 2) // the others elect a new leader and commit something else
	c.reconnect(old)

	c.waitMembers(old, 0, 1, 2)
	c.waitLearners(old) // the uncommitted learner is gone
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
	addIndex := c.changeMembers(func(n *raft.Node) (int, int, error) { return n.AddLearner(raft.Member{ID: 4}) })
	for i := 0; i < 40; i++ {
		c.put(fmt.Sprintf("m%d", i), "v", 4)
	}
	if st := c.nodes[c.checkOneLeader()].Status(); st.SnapshotIndex < addIndex {
		t.Fatalf("the log is only compacted to %d, not past the membership entry at %d", st.SnapshotIndex, addIndex)
	}
	c.reconnect(4)
	c.waitMembers(4, 0, 1, 2, 3) // it learns the cluster, and that it is a learner, from the snapshot
	c.waitLearners(4, 4)
	c.waitCaughtUp(4)
	c.promote(4)
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

// A learner gets the log but is not a voter: it does not count towards a majority.
func TestLearnerDoesNotCountTowardsMajority(t *testing.T) {
	c := newMemberCluster(t, 3, 4, 0)
	leader := c.checkOneLeader()
	c.put("a", "1", 3)

	c.join(3)
	c.isolate(3) // a learner that cannot be reached
	c.changeMembers(func(n *raft.Node) (int, int, error) { return n.AddLearner(raft.Member{ID: 3}) })

	// Three voters and one unreachable learner: one voter down must not stop commits. If the
	// learner counted, a majority would be three of four, and the leader plus one voter would
	// not be enough.
	c.isolate((leader + 1) % 3)
	c.put("b", "2", 2)
}

// A learner never starts an election, however long it hears nothing.
func TestLearnerNeverCampaigns(t *testing.T) {
	c := newMemberCluster(t, 3, 4, 0)
	c.checkOneLeader()
	c.addLearner(3)
	c.waitCaughtUp(3)

	c.isolate(3)
	term := c.nodes[3].Status().Term
	time.Sleep(2 * time.Second) // many election timeouts
	if st := c.nodes[3].Status(); st.Role != raft.Follower || st.Term != term {
		t.Fatalf("an isolated learner started an election: %+v (term was %d)", st, term)
	}
}

func TestPromoteAndRemoveLearner(t *testing.T) {
	c := newMemberCluster(t, 3, 4, 0)
	leader := c.checkOneLeader()
	c.put("a", "1", 3)

	if _, _, err := c.nodes[leader].PromoteLearner(0); !errors.Is(err, raft.ErrNotLearner) {
		t.Fatalf("promoting a voter: %v, want ErrNotLearner", err)
	}
	if _, _, err := c.nodes[leader].PromoteLearner(3); !errors.Is(err, raft.ErrNotMember) {
		t.Fatalf("promoting a stranger: %v, want ErrNotMember", err)
	}

	c.addLearner(3)
	c.waitCaughtUp(3)
	c.waitMembers(leader, 0, 1, 2)
	c.waitLearners(leader, 3)
	c.waitStoresEqual() // the learner has the data
	c.promote(3)
	c.waitMembers(leader, 0, 1, 2, 3)
	c.waitLearners(leader)

	// A learner can also be dropped without ever voting.
	c2 := newMemberCluster(t, 3, 4, 0)
	c2.checkOneLeader()
	c2.addLearner(3)
	c2.remove(3)
	c2.waitMembers(c2.checkOneLeader(), 0, 1, 2)
}

// The last voter cannot be removed even if a learner is still there.
func TestLastVoterCannotBeRemovedEvenWithALearner(t *testing.T) {
	c := newMemberCluster(t, 1, 2, 0)
	c.checkOneLeader()
	c.addLearner(1)
	if _, _, err := c.nodes[0].RemoveServer(0); !errors.Is(err, raft.ErrLastMember) {
		t.Fatalf("removing the only voter: %v, want ErrLastMember", err)
	}
}

// A leader that cannot reach a majority stops claiming to be leader, so clients are not left
// waiting on a leader that can neither commit nor confirm a read.
func TestIsolatedLeaderStepsDown(t *testing.T) {
	c := newMemberCluster(t, 3, 3, 0)
	old := c.checkOneLeader()
	c.put("k", "v", 3)

	c.isolate(old)
	deadline := time.Now().Add(3 * time.Second)
	for c.nodes[old].Status().Role == raft.Leader {
		if time.Now().After(deadline) {
			t.Fatal("a leader cut off from every other node never stepped down")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := c.nodes[old].Status().LeaderID; got == old {
		t.Fatalf("the node that stepped down still names itself as leader")
	}
	c.put("k2", "v2", 2) // the other two elect a new leader and carry on
	c.reconnect(old)
	c.checkOneLeader()
	c.waitStoresEqual()
}

// Losing one follower out of three leaves a majority, so the leader must keep leading, in the same term.
func TestLeaderKeepsLeadingWhileAMajorityAnswers(t *testing.T) {
	c := newMemberCluster(t, 3, 3, 0)
	leader := c.checkOneLeader()
	c.put("k", "v", 3)
	term := c.nodes[leader].Status().Term

	c.isolate((leader + 1) % 3)
	for i := 0; i < 10; i++ {
		time.Sleep(200 * time.Millisecond)
		if st := c.nodes[leader].Status(); st.Role != raft.Leader || st.Term != term {
			t.Fatalf("the leader gave up with a majority still answering: %+v (term was %d)", st, term)
		}
	}
	c.put("k2", "v2", 2)
}

// Round trips slower than the tick interval are normal, so a freshly elected leader must be given
// time to hear back from a majority before it is judged.
func TestNewLeaderIsGivenTimeToHearFromAMajority(t *testing.T) {
	c := newMemberCluster(t, 3, 3, 0)
	c.net.SetDelay(25*time.Millisecond, 40*time.Millisecond)
	leader := c.checkOneLeader()
	term := c.nodes[leader].Status().Term
	for i := 0; i < 8; i++ {
		time.Sleep(200 * time.Millisecond)
		if st := c.nodes[leader].Status(); st.Role != raft.Leader || st.Term != term {
			t.Fatalf("the leader was unseated on a slow but healthy network: %+v (term was %d)", st, term)
		}
	}
}

// ---------------------------------------------------------------------------
// A removed server finds out that it was removed
// ---------------------------------------------------------------------------

func (c *cluster) removedCalls(i int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.removed[i]
}

func (c *cluster) waitRemoved(i int) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !c.nodes[i].Status().Removed {
		if time.Now().After(deadline) {
			c.t.Fatalf("node %d never learned that it was removed", i)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *cluster) sentAppends(i int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.appends[i]
}

func (c *cluster) wantNotRemoved(ids ...int) {
	c.t.Helper()
	for _, i := range ids {
		if c.nodes[i].Status().Removed || c.removedCalls(i) != 0 {
			c.t.Fatalf("node %d believes it was removed but is a member", i)
		}
	}
}

// A server that is cut off when it is removed learns of it as soon as it can be reached again.
func TestUnreachableServerLearnsItWasRemoved(t *testing.T) {
	c := newMemberCluster(t, 5, 5, 0)
	leader := c.checkOneLeader()
	victim := (leader + 1) % 5
	c.put("a", "1", 5)

	c.isolate(victim)
	c.remove(victim)
	c.put("b", "2", 4)
	time.Sleep(300 * time.Millisecond)
	if c.nodes[victim].Status().Removed {
		t.Fatal("it cannot know before it is reachable")
	}

	c.reconnect(victim)
	c.waitRemoved(victim)
	c.mu.Lock()
	calls := c.removed[victim]
	c.mu.Unlock()
	if calls != 1 {
		t.Fatalf("OnRemoved called %d times, want 1", calls)
	}
	for i := 0; i < 5; i++ {
		if i != victim {
			c.wantNotRemoved(i)
		}
	}
	c.put("c", "3", 4)
}

// A server that was reachable all along is told too, which used to need stopping by hand.
func TestReachableServerLearnsItWasRemoved(t *testing.T) {
	c := newMemberCluster(t, 4, 4, 0)
	leader := c.checkOneLeader()
	victim := (leader + 2) % 4
	c.remove(victim)
	c.waitRemoved(victim)
	c.wantNotRemoved(leader)
}

// The leader that removes itself is told as well, once the change commits.
func TestRemovedLeaderLearnsItWasRemoved(t *testing.T) {
	c := newMemberCluster(t, 3, 3, 0)
	old := c.checkOneLeader()
	c.remove(old)
	c.waitRemoved(old)
}

// The others compact their logs well past the removal while the server is away, so it can only find
// out through the snapshot that replaces what it missed.
func TestUnreachableServerLearnsItWasRemovedFromASnapshot(t *testing.T) {
	c := newMemberCluster(t, 3, 3, 5)
	leader := c.checkOneLeader()
	victim := (leader + 1) % 3
	c.isolate(victim)
	c.remove(victim)
	removalAt := c.nodes[leader].Status().CommitIndex
	for i := 0; i < 40; i++ {
		c.put(fmt.Sprintf("k%d", i), "v", 2)
	}
	if got := c.nodes[leader].Status().SnapshotIndex; got <= removalAt {
		t.Fatalf("the leader has compacted only to %d, the removal is at %d", got, removalAt)
	}
	c.reconnect(victim)
	c.waitRemoved(victim)
}

// If the leader changes while the server is away, the new leader still tells it.
func TestNewLeaderStillTellsARemovedServer(t *testing.T) {
	c := newMemberCluster(t, 5, 5, 0)
	leader := c.checkOneLeader()
	victim := (leader + 1) % 5
	c.isolate(victim)
	c.remove(victim)
	c.put("a", "1", 4)

	c.crash(leader)
	next := c.checkOneLeader()
	if next == leader {
		t.Fatal("the crashed node cannot be the leader")
	}
	c.put("b", "2", 3)
	c.reconnect(victim)
	c.waitRemoved(victim)
}

// A server being added, as a learner and then a voter, is never mistaken for a removed one.
func TestAddedServerIsNeverMistakenForRemoved(t *testing.T) {
	c := newMemberCluster(t, 3, 5, 5)
	c.checkOneLeader()
	for i := 0; i < 30; i++ {
		c.put(fmt.Sprintf("k%d", i), "v", 3)
	}
	c.addLearner(3)
	c.addLearner(4)
	for _, id := range []int{3, 4} {
		c.changeMembers(func(n *raft.Node) (int, int, error) { return n.PromoteLearner(id) })
	}
	c.put("after", "v", 5)
	time.Sleep(500 * time.Millisecond)
	c.wantNotRemoved(0, 1, 2, 3, 4)
}

// Once a removed server has confirmed, the leader stops contacting it.
func TestLeaderStopsContactingAServerThatKnows(t *testing.T) {
	c := newMemberCluster(t, 4, 4, 0)
	leader := c.checkOneLeader()
	victim := (leader + 1) % 4
	c.remove(victim)
	c.waitRemoved(victim)
	time.Sleep(300 * time.Millisecond)
	before := c.sentAppends(victim)
	time.Sleep(time.Second)
	if after := c.sentAppends(victim); after-before > 1 {
		t.Fatalf("%d more messages went to a server that already knows it was removed", after-before)
	}
}

// A removal that never commits is undone, and a server that held it must not act on it: it saw the
// entry in its log, but the cluster never agreed to it.
func TestUncommittedRemovalDoesNotRemove(t *testing.T) {
	c := newMemberCluster(t, 5, 5, 0)
	leader := c.checkOneLeader()
	victim := (leader + 1) % 5
	var others []int
	for i := 0; i < 5; i++ {
		if i != leader && i != victim {
			others = append(others, i)
		}
	}
	c.put("a", "1", 5)

	c.net.Partition([]int{leader, victim}, others)
	if _, _, err := c.nodes[leader].RemoveServer(victim); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(c.nodes[victim].Status().Members) != 4 { // the victim holds the entry that removes it
		if time.Now().After(deadline) {
			t.Fatal("the removal entry never reached the victim")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	c.wantNotRemoved(victim)

	c.net.Heal()
	c.checkOneLeader()
	c.put("b", "2", 5)
	time.Sleep(500 * time.Millisecond)
	c.wantNotRemoved(0, 1, 2, 3, 4)
	c.waitMembers(victim, 0, 1, 2, 3, 4)
}

// Only a committed removal counts, even while the commit index moves on past entries before it.
func TestOnlyACommittedRemovalMarksANodeRemoved(t *testing.T) {
	n, _ := newBare(t) // node 0 of {0, 1, 2}
	entries := []raft.Entry{
		{Term: 1, NoOp: true},
		{Term: 1, Members: []raft.Member{{ID: 1}, {ID: 2}}}, // removes node 0
	}
	r := n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: entries, LeaderCommit: 1})
	if !r.Success {
		t.Fatalf("append refused: %+v", r)
	}
	if st := n.Status(); st.Removed || st.CommitIndex != 1 || len(st.Members) != 2 {
		t.Fatalf("the removal is in the log but not committed; got %+v", st)
	}
	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 2})
	if st := n.Status(); !st.Removed {
		t.Fatalf("the removal committed but the node does not know: %+v", st)
	}
}
