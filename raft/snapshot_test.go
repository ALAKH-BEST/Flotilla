package raft_test

import (
	"fmt"
	"sync"
	"testing"
	"time"
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
