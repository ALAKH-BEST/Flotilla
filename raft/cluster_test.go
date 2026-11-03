package raft_test

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/kv"
	"github.com/ALAKH-BEST/flotilla/raft"
	"github.com/ALAKH-BEST/flotilla/simnet"
)

// cluster runs several nodes over a simulated network. Each node feeds a key-value
// store, and the cluster records what every node applies so replicas can be compared.
type cluster struct {
	t         *testing.T
	net       *simnet.Network
	n         int
	snapEvery int // take a snapshot every this many applied entries (0 = never)
	storages  []*raft.MemoryStorage
	nodes     []*raft.Node
	stores    []*kv.Store
	running   []bool
	cut       []bool // isolated from the network

	mu        sync.Mutex
	committed map[int]raft.ApplyMsg // first entry seen at each index, to compare replicas
	lastIndex []int                 // per node: highest index applied (or covered by an installed snapshot)
	installed []int                 // per node: snapshots handed to its state machine
}

func newCluster(t *testing.T, n int) *cluster { return newSnapshotCluster(t, n, 0) }

func newSnapshotCluster(t *testing.T, n, snapEvery int) *cluster {
	t.Helper()
	c := &cluster{
		snapEvery: snapEvery,
		stores:    make([]*kv.Store, n),
		installed: make([]int, n),
		t:         t,
		net:       simnet.New(1),
		n:         n,
		storages:  make([]*raft.MemoryStorage, n),
		nodes:     make([]*raft.Node, n),
		running:   make([]bool, n),
		cut:       make([]bool, n),
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
	c.stores[i] = kv.NewStore() // the state machine is lost in a crash and rebuilt from the snapshot and log
	c.lastIndex[i] = 0
	c.installed[i] = 0
	c.mu.Unlock()
	c.nodes[i] = node
	c.running[i] = true
	c.net.Register(i, node)
	go c.consume(i, applyCh, node)
	node.Start()
}

// consume feeds a node's state machine and checks what it applies against the other nodes.
func (c *cluster) consume(i int, ch <-chan raft.ApplyMsg, node *raft.Node) {
	for msg := range ch {
		c.mu.Lock()
		if c.nodes[i] != node { // a straggler from a stopped incarnation
			c.mu.Unlock()
			continue
		}
		store := c.stores[i]
		snapshotAt := 0
		if msg.SnapshotValid {
			c.lastIndex[i] = msg.SnapshotIndex
			c.installed[i]++
			store.Apply(msg)
		} else {
			if msg.Index != c.lastIndex[i]+1 {
				c.t.Errorf("node %d applied index %d after %d: entries must arrive in order", i, msg.Index, c.lastIndex[i])
			}
			c.lastIndex[i] = msg.Index
			if prev, ok := c.committed[msg.Index]; ok {
				if prev.Term != msg.Term || !bytes.Equal(prev.Command, msg.Command) || prev.NoOp != msg.NoOp {
					c.t.Errorf("replicas disagree at index %d: %+v vs %+v", msg.Index, prev, msg)
				}
			} else {
				c.committed[msg.Index] = msg
			}
			store.Apply(msg)
			if c.snapEvery > 0 && msg.Index%c.snapEvery == 0 {
				snapshotAt = msg.Index
			}
		}
		var data []byte
		if snapshotAt > 0 {
			data = store.Snapshot()
		}
		c.mu.Unlock()
		if snapshotAt > 0 {
			if err := node.Snapshot(snapshotAt, data); err != nil {
				c.t.Errorf("node %d: snapshot at %d: %v", i, snapshotAt, err)
			}
		}
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

// appliedCount reports how many nodes have applied the given command at the given index.
// A node that got there through a snapshot counts too.
func (c *cluster) appliedCount(index int, cmd []byte) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m, ok := c.committed[index]; !ok || !bytes.Equal(m.Command, cmd) {
		return 0
	}
	count := 0
	for i := 0; i < c.n; i++ {
		if c.lastIndex[i] >= index {
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

// put submits a key-value write and waits until `want` nodes applied it.
func (c *cluster) put(key, value string, want int) int {
	c.t.Helper()
	return c.submit(kv.Encode(kv.Command{Op: kv.OpPut, Key: key, Value: value}), want)
}

// waitStoresEqual waits until every running node holds the same data, and returns it.
func (c *cluster) waitStoresEqual() map[string]string {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		c.mu.Lock()
		var first map[string]string
		equal := true
		for i := 0; i < c.n; i++ {
			if !c.running[i] {
				continue
			}
			data := c.stores[i].Data()
			if first == nil {
				first = data
			} else if !reflect.DeepEqual(first, data) {
				equal = false
			}
		}
		c.mu.Unlock()
		if equal {
			return first
		}
		if time.Now().After(deadline) {
			c.t.Fatal("replicas did not converge to the same data")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
