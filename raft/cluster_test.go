package raft_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
	"github.com/ALAKH-BEST/flotilla/simnet"
)

// cluster runs several nodes over a simulated network and records what each one applies.
type cluster struct {
	t        *testing.T
	net      *simnet.Network
	n        int
	storages []*raft.MemoryStorage
	nodes    []*raft.Node
	running  []bool
	cut      []bool // isolated from the network

	mu        sync.Mutex
	applied   []map[int]raft.ApplyMsg // per node: index -> entry
	committed map[int]raft.ApplyMsg   // first entry seen at each index, to compare replicas
	lastIndex []int
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{
		t:         t,
		net:       simnet.New(1),
		n:         n,
		storages:  make([]*raft.MemoryStorage, n),
		nodes:     make([]*raft.Node, n),
		running:   make([]bool, n),
		cut:       make([]bool, n),
		applied:   make([]map[int]raft.ApplyMsg, n),
		committed: make(map[int]raft.ApplyMsg),
		lastIndex: make([]int, n),
	}
	for i := 0; i < n; i++ {
		c.storages[i] = raft.NewMemoryStorage()
		c.start(i)
	}
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) peers() []int {
	ids := make([]int, c.n)
	for i := range ids {
		ids[i] = i
	}
	return ids
}

// start launches (or relaunches) node i from whatever its storage holds.
func (c *cluster) start(i int) {
	applyCh := make(chan raft.ApplyMsg, 256)
	node, err := raft.New(raft.Config{
		ID:        i,
		Peers:     c.peers(),
		Transport: c.net.Transport(i),
		Storage:   c.storages[i],
		ApplyCh:   applyCh,
		Seed:      int64(i + 1),
	})
	if err != nil {
		c.t.Fatalf("node %d: %v", i, err)
	}
	c.mu.Lock()
	c.applied[i] = make(map[int]raft.ApplyMsg) // a restarted node replays its log from index 1
	c.lastIndex[i] = 0
	c.mu.Unlock()
	c.nodes[i] = node
	c.running[i] = true
	c.net.Register(i, node)
	go c.consume(i, applyCh, node)
	node.Start()
}

// consume records applied entries and checks them against what other nodes applied.
func (c *cluster) consume(i int, ch <-chan raft.ApplyMsg, node *raft.Node) {
	for msg := range ch {
		c.mu.Lock()
		if c.nodes[i] == node { // ignore stragglers from a stopped incarnation
			if msg.Index != c.lastIndex[i]+1 {
				c.t.Errorf("node %d applied index %d after %d: entries must arrive in order", i, msg.Index, c.lastIndex[i])
			}
			c.lastIndex[i] = msg.Index
			c.applied[i][msg.Index] = msg
			if prev, ok := c.committed[msg.Index]; ok {
				if prev.Term != msg.Term || !bytes.Equal(prev.Command, msg.Command) || prev.NoOp != msg.NoOp {
					c.t.Errorf("replicas disagree at index %d: %+v vs %+v", msg.Index, prev, msg)
				}
			} else {
				c.committed[msg.Index] = msg
			}
		}
		c.mu.Unlock()
	}
}

// crash stops node i, keeping its storage.
func (c *cluster) crash(i int) {
	c.net.Unregister(i)
	c.nodes[i].Stop()
	c.running[i] = false
}

func (c *cluster) restart(i int) { c.start(i) }

func (c *cluster) isolate(i int) {
	c.net.Isolate(i)
	c.cut[i] = true
}

func (c *cluster) reconnect(i int) {
	c.net.Reconnect(i)
	c.cut[i] = false
}

func (c *cluster) heal() {
	c.net.Heal()
	for i := range c.cut {
		c.cut[i] = false
	}
}

func (c *cluster) shutdown() {
	for i := range c.nodes {
		if c.running[i] {
			c.net.Unregister(i)
			c.nodes[i].Stop()
		}
	}
}

func (c *cluster) reachable(i int) bool { return c.running[i] && !c.cut[i] }

// checkOneLeader waits for exactly one leader among the reachable nodes and returns its ID.
// It fails the test if two nodes ever lead in the same term.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leaders := make(map[int][]int) // term -> leader IDs
		for i := 0; i < c.n; i++ {
			if !c.reachable(i) {
				continue
			}
			if st := c.nodes[i].Status(); st.Role == raft.Leader {
				leaders[st.Term] = append(leaders[st.Term], i)
			}
		}
		latest := -1
		for term, ids := range leaders {
			if len(ids) > 1 {
				c.t.Fatalf("term %d has more than one leader: %v", term, ids)
			}
			if term > latest {
				latest = term
			}
		}
		if latest >= 0 {
			return leaders[latest][0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no leader was elected")
	return -1
}

// checkNoLeader fails if any reachable node is a leader.
func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i := 0; i < c.n; i++ {
		if c.reachable(i) && c.nodes[i].Status().Role == raft.Leader {
			c.t.Fatalf("node %d is leader but should not be able to be", i)
		}
	}
}

// appliedCount reports how many nodes applied the given command at the given index.
func (c *cluster) appliedCount(index int, cmd []byte) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for i := 0; i < c.n; i++ {
		if m, ok := c.applied[i][index]; ok && bytes.Equal(m.Command, cmd) {
			count++
		}
	}
	return count
}

// submit proposes cmd to the leader and waits until at least `want` nodes applied it.
// It retries on a new leader if the first one loses its role. It returns the log index.
func (c *cluster) submit(cmd []byte, want int) int {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			if !c.reachable(i) {
				continue
			}
			index, _, isLeader := c.nodes[i].Propose(cmd)
			if !isLeader {
				continue
			}
			until := time.Now().Add(2 * time.Second)
			for time.Now().Before(until) {
				if c.appliedCount(index, cmd) >= want {
					return index
				}
				time.Sleep(10 * time.Millisecond)
			}
			break // this leader did not commit it in time: look for the current one
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("command %q was not applied by %d nodes in time", cmd, want)
	return -1
}

// waitApplied waits until node i has applied up to at least the given index.
func (c *cluster) waitApplied(i, index int) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := c.lastIndex[i]
		c.mu.Unlock()
		if got >= index {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("node %d did not apply index %d in time", i, index)
}

func cmd(format string, args ...any) []byte { return []byte(fmt.Sprintf(format, args...)) }
