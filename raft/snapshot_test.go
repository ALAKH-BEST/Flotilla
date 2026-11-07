package raft_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
)

func wantKeys(t *testing.T, data map[string]string, keys []string) {
	t.Helper()
	for _, k := range keys {
		if data[k] != "v-"+k {
			t.Fatalf("key %q = %q, want %q", k, data[k], "v-"+k)
		}
	}
	if len(data) != len(keys) {
		t.Fatalf("store has %d keys, want %d", len(data), len(keys))
	}
}

func TestSnapshotCompactsLog(t *testing.T) {
	c := newSnapshotCluster(t, 3, 10)
	c.checkOneLeader()
	var keys []string
	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("k%d", i)
		keys = append(keys, k)
		c.put(k, "v-"+k, 3)
	}
	wantKeys(t, c.waitStoresEqual(), keys)

	for i := 0; i < 3; i++ {
		st := c.nodes[i].Status()
		if st.SnapshotIndex < 40 {
			t.Errorf("node %d: snapshot only covers index %d", i, st.SnapshotIndex)
		}
		if st.LogEntries > 25 {
			t.Errorf("node %d still holds %d log entries; compaction should keep it short", i, st.LogEntries)
		}
	}
	if err := c.nodes[0].Snapshot(1_000_000, nil); err == nil {
		t.Error("snapshotting an uncommitted index should fail")
	}
}

// A follower that missed more than the leader still has in its log can only catch up
// from the snapshot.
func TestLaggingFollowerInstallsSnapshot(t *testing.T) {
	c := newSnapshotCluster(t, 3, 10)
	leader := c.checkOneLeader()
	lagging := (leader + 1) % 3
	c.put("k0", "v-k0", 3)
	keys := []string{"k0"}

	c.isolate(lagging)
	last := 0
	for i := 1; i < 50; i++ {
		k := fmt.Sprintf("k%d", i)
		keys = append(keys, k)
		last = c.put(k, "v-"+k, 2)
	}
	behind := c.nodes[lagging].Status().LastLogIndex
	covered := 0
	for i := 0; i < 3; i++ {
		if i != lagging {
			covered = max(covered, c.nodes[i].Status().SnapshotIndex)
		}
	}
	if covered <= behind {
		t.Fatalf("the others have only compacted to %d but the follower stopped at %d, so it would not need a snapshot", covered, behind)
	}

	c.reconnect(lagging)
	c.waitApplied(lagging, last)
	wantKeys(t, c.waitStoresEqual(), keys)

	c.mu.Lock()
	installed := c.installed[lagging]
	c.mu.Unlock()
	if installed == 0 {
		t.Fatal("the follower caught up without receiving a snapshot")
	}
}

func TestRestartFromSnapshot(t *testing.T) {
	c := newSnapshotCluster(t, 3, 10)
	c.checkOneLeader()
	var keys []string
	for i := 0; i < 45; i++ {
		k := fmt.Sprintf("k%d", i)
		keys = append(keys, k)
		c.put(k, "v-"+k, 3)
	}
	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.restart(i)
	}
	c.checkOneLeader()

	keys = append(keys, "after")
	c.put("after", "v-after", 3)
	wantKeys(t, c.waitStoresEqual(), keys)

	for i := 0; i < 3; i++ {
		c.mu.Lock()
		installed := c.installed[i]
		c.mu.Unlock()
		if installed == 0 {
			t.Errorf("node %d restarted without restoring its snapshot", i)
		}
	}
}

func TestSnapshotsOnUnreliableNetwork(t *testing.T) {
	c := newSnapshotCluster(t, 5, 5)
	c.checkOneLeader()
	c.net.SetDropRate(0.15)
	c.net.SetDelay(0, 15*time.Millisecond)

	var mu sync.Mutex
	var keys []string
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				k := fmt.Sprintf("w%d-%d", w, i)
				c.put(k, "v-"+k, 3)
				mu.Lock()
				keys = append(keys, k)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	c.net.SetDropRate(0)
	c.net.SetDelay(0, 0)
	c.put("final", "v-final", 5)
	keys = append(keys, "final")
	wantKeys(t, c.waitStoresEqual(), keys)
}

// ---------------------------------------------------------------------------
// Chunked transfer
// ---------------------------------------------------------------------------

// With a tiny chunk size the snapshot must be streamed in many pieces, none bigger than the limit,
// and the follower still ends up with exactly the leader's data.
func TestSnapshotIsSentInChunks(t *testing.T) {
	c := newSnapshotCluster(t, 3, 10)
	c.chunk = 16 // applies to nodes started from here on
	for i := 0; i < 3; i++ {
		c.crash(i)
		c.restart(i)
	}
	leader := c.checkOneLeader()
	lagging := (leader + 1) % 3
	c.put("k0", "v-k0", 3)
	keys := []string{"k0"}

	c.isolate(lagging)
	last := 0
	for i := 1; i < 50; i++ {
		k := fmt.Sprintf("k%d", i)
		keys = append(keys, k)
		last = c.put(k, "v-"+k, 2)
	}
	c.mu.Lock()
	c.chunks = snapshotChunks{}
	c.mu.Unlock()

	c.reconnect(lagging)
	c.waitApplied(lagging, last)
	wantKeys(t, c.waitStoresEqual(), keys)

	c.mu.Lock()
	got := c.chunks
	c.mu.Unlock()
	if got.sent < 10 {
		t.Errorf("only %d chunk messages sent; the snapshot should have been split into many", got.sent)
	}
	if got.largest > 16 {
		t.Errorf("a chunk carried %d bytes, over the 16 byte limit", got.largest)
	}
	if got.restarts > 2 {
		t.Errorf("the transfer started from the beginning %d times on a healthy network; heartbeats must not start competing transfers", got.restarts)
	}
}

// Chunks are lost and delayed; the transfer must resume and still install a whole, uncorrupted snapshot.
func TestChunkedSnapshotsOnUnreliableNetwork(t *testing.T) {
	c := newSnapshotCluster(t, 5, 5)
	c.chunk = 8
	for i := 0; i < 5; i++ {
		c.crash(i)
		c.restart(i)
	}
	c.checkOneLeader()
	c.net.SetDropRate(0.15)
	c.net.SetDelay(0, 10*time.Millisecond)
	lagging := 0
	c.isolate(lagging)

	var keys []string
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("k%d", i)
		keys = append(keys, k)
		c.put(k, "v-"+k, 3)
	}
	c.reconnect(lagging)
	c.net.SetDropRate(0.15)
	c.put("final", "v-final", 5)
	keys = append(keys, "final")
	c.net.SetDropRate(0)
	c.net.SetDelay(0, 0)
	wantKeys(t, c.waitStoresEqual(), keys)
}

func newBare(t *testing.T) (*raft.Node, <-chan raft.ApplyMsg) {
	t.Helper()
	ch := make(chan raft.ApplyMsg, 16)
	n, err := raft.New(raft.Config{ID: 0, Peers: []int{0, 1, 2}, Transport: nil2{}, Storage: raft.NewMemoryStorage(), ApplyCh: ch})
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	t.Cleanup(n.Stop)
	return n, ch
}

type nil2 struct{}

func (nil2) RequestVote(int, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, fmt.Errorf("down")
}
func (nil2) PreVote(int, *raft.PreVoteArgs) (*raft.PreVoteReply, error) {
	return nil, fmt.Errorf("down")
}
func (nil2) AppendEntries(int, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, fmt.Errorf("down")
}
func (nil2) InstallSnapshot(int, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return nil, fmt.Errorf("down")
}

func chunk(term, offset int, data string, done bool) *raft.InstallSnapshotArgs {
	return &raft.InstallSnapshotArgs{Term: term, LeaderID: 1, LastIncludedIndex: 7, LastIncludedTerm: 2,
		Offset: offset, Data: []byte(data), Done: done}
}

// The receiver's reply to each chunk, including the odd ones: duplicates, gaps, a new leader, a restart.
func TestReceiverHandlesChunksOutOfOrder(t *testing.T) {
	n, ch := newBare(t)

	r := n.HandleInstallSnapshot(chunk(3, 0, "abc", false))
	if r.NextOffset != 3 || r.Installed {
		t.Fatalf("first chunk: %+v", r)
	}
	if r = n.HandleInstallSnapshot(chunk(3, 0+3, "def", false)); r.NextOffset != 6 {
		t.Fatalf("second chunk: %+v", r)
	}
	// A duplicate of an earlier chunk must not be appended again.
	if r = n.HandleInstallSnapshot(chunk(3, 3, "def", false)); r.NextOffset != 6 || r.Installed {
		t.Fatalf("duplicate chunk: %+v", r)
	}
	// A chunk beyond what we hold (one was lost) is refused with where to resume.
	if r = n.HandleInstallSnapshot(chunk(3, 9, "jkl", true)); r.NextOffset != 6 || r.Installed {
		t.Fatalf("gap: %+v", r)
	}
	if n.Status().SnapshotIndex != 0 {
		t.Fatal("a partial snapshot must not be installed")
	}
	select {
	case m := <-ch:
		t.Fatalf("nothing should reach the state machine before the last chunk, got %+v", m)
	case <-time.After(50 * time.Millisecond):
	}
	// A new leader's transfer discards the old one: mid-transfer chunks from another term are not mixed in.
	if r = n.HandleInstallSnapshot(chunk(4, 6, "ghi", false)); r.NextOffset != 0 || r.Installed {
		t.Fatalf("chunk from another term: %+v", r)
	}
	// Offset 0 restarts cleanly, and the full, correct data is what gets installed.
	n.HandleInstallSnapshot(chunk(4, 0, "AB", false))
	if r = n.HandleInstallSnapshot(chunk(4, 2, "CD", true)); !r.Installed || r.NextOffset != 4 {
		t.Fatalf("last chunk: %+v", r)
	}
	if st := n.Status(); st.SnapshotIndex != 7 {
		t.Fatalf("snapshot not installed: %+v", st)
	}
	select {
	case m := <-ch:
		if !m.SnapshotValid || string(m.Snapshot) != "ABCD" || m.SnapshotIndex != 7 {
			t.Fatalf("installed %q (index %d), want ABCD at 7", m.Snapshot, m.SnapshotIndex)
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot never delivered")
	}
	// Anything already covered is acknowledged without being stored again.
	if r = n.HandleInstallSnapshot(chunk(4, 0, "zzz", false)); !r.Installed {
		t.Fatalf("already covered: %+v", r)
	}
}

// When one chunk takes longer than a heartbeat to be answered, the heartbeats must not start more
// transfers to the same follower: one is streaming already.
func TestOnlyOneSnapshotTransferPerFollower(t *testing.T) {
	c := newSnapshotCluster(t, 3, 10)
	c.chunk = 16
	for i := 0; i < 3; i++ {
		c.crash(i)
		c.restart(i)
	}
	leader := c.checkOneLeader()
	lagging := (leader + 1) % 3
	c.isolate(lagging)
	last := 0
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("k%d", i)
		last = c.put(k, "v-"+k, 2)
	}
	c.mu.Lock()
	c.chunks = snapshotChunks{}
	c.mu.Unlock()

	c.net.SetDelay(35*time.Millisecond, 45*time.Millisecond) // a round trip is about two heartbeats
	c.reconnect(lagging)
	c.waitApplied(lagging, last)
	c.net.SetDelay(0, 0)

	c.mu.Lock()
	most := c.chunks.mostInflight[[2]int{leader, lagging}]
	c.mu.Unlock()
	if most != 1 {
		t.Fatalf("up to %d snapshot messages to one follower were in flight at once, want 1", most)
	}
}

// Chunks of two different snapshots from the same leader are never spliced together.
func TestReceiverRejectsChunkOfADifferentSnapshot(t *testing.T) {
	n, ch := newBare(t)
	n.HandleInstallSnapshot(chunk(3, 0, "abc", false))
	other := chunk(3, 3, "XYZ", true)
	other.LastIncludedIndex = 9 // the leader has since taken a newer snapshot
	if r := n.HandleInstallSnapshot(other); r.Installed || r.NextOffset != 0 {
		t.Fatalf("spliced two snapshots: %+v", r)
	}
	select {
	case m := <-ch:
		t.Fatalf("installed %q from mixed chunks", m.Snapshot)
	case <-time.After(50 * time.Millisecond):
	}
}
