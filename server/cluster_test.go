package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/kv"
	"github.com/ALAKH-BEST/flotilla/raft"
	"github.com/ALAKH-BEST/flotilla/server"
	"github.com/ALAKH-BEST/flotilla/simnet"
)

// kvCluster runs several servers over a simulated network.
type kvCluster struct {
	t         *testing.T
	net       *simnet.Network
	n         int
	initial   int    // the first initial nodes form the starting cluster
	joining   []bool // nodes started with Join, to be added later
	snapEvery int
	storages  []*raft.MemoryStorage

	mu      sync.Mutex
	servers []*server.Server // nil while a node is crashed
}

func newKVCluster(t *testing.T, n, snapEvery int) *kvCluster {
	return newKVMemberCluster(t, n, n, snapEvery)
}

// newKVMemberCluster has room for total nodes and starts the first initial; the others can be
// started later with joinStart and added with AddServer.
func newKVMemberCluster(t *testing.T, initial, total, snapEvery int) *kvCluster {
	t.Helper()
	n := total
	c := &kvCluster{
		t:         t,
		net:       simnet.New(7),
		n:         n,
		initial:   initial,
		joining:   make([]bool, n),
		snapEvery: snapEvery,
		storages:  make([]*raft.MemoryStorage, n),
		servers:   make([]*server.Server, n),
	}
	for i := 0; i < n; i++ {
		c.storages[i] = raft.NewMemoryStorage()
		if i < initial {
			c.start(i)
		}
	}
	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			c.crash(i)
		}
	})
	return c
}

func (c *kvCluster) start(i int) {
	var peers []int
	if !c.joining[i] {
		for p := 0; p < c.initial; p++ {
			peers = append(peers, p)
		}
	}
	srv, err := server.New(server.Config{
		Raft: raft.Config{
			ID:        i,
			Peers:     peers,
			Join:      c.joining[i],
			Transport: c.net.Transport(i),
			Storage:   c.storages[i],
			Seed:      int64(i + 1),
		},
		SnapshotEvery:  c.snapEvery,
		RequestTimeout: time.Second,
	})
	if err != nil {
		c.t.Fatalf("server %d: %v", i, err)
	}
	c.mu.Lock()
	c.servers[i] = srv
	c.mu.Unlock()
	c.net.Register(i, srv)
	srv.Start()
}

// crash stops server i, keeping its storage. Crashing a stopped server does nothing.
func (c *kvCluster) crash(i int) {
	c.mu.Lock()
	srv := c.servers[i]
	c.servers[i] = nil
	c.mu.Unlock()
	if srv != nil {
		c.net.Unregister(i)
		srv.Stop()
	}
}

func (c *kvCluster) restart(i int) {
	c.mu.Lock()
	running := c.servers[i] != nil
	c.mu.Unlock()
	if !running {
		c.start(i)
	}
}

// liveDoer reaches whichever server is currently running at a slot, so clients keep
// working across crashes and restarts.
type liveDoer struct {
	c *kvCluster
	i int
}

func (d liveDoer) Do(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	d.c.mu.Lock()
	srv := d.c.servers[d.i]
	d.c.mu.Unlock()
	if srv == nil {
		return kv.Result{}, server.ErrStopped
	}
	return srv.Do(ctx, cmd)
}

func (c *kvCluster) doers() []server.Doer {
	out := make([]server.Doer, c.n)
	for i := range out {
		out[i] = liveDoer{c: c, i: i}
	}
	return out
}

func (c *kvCluster) clerk(id int64) *server.Clerk { return server.NewClerk(id, c.doers()) }

// waitConverged waits until every running server holds the same data.
func (c *kvCluster) waitConverged() map[string]string {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		c.mu.Lock()
		var first map[string]string
		equal := true
		for _, srv := range c.servers {
			if srv == nil {
				continue
			}
			data := srv.Store().Data()
			if first == nil {
				first = data
			} else if !sameData(first, data) {
				equal = false
			}
		}
		c.mu.Unlock()
		if equal {
			return first
		}
		if time.Now().After(deadline) {
			c.t.Fatal("replicas did not converge")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func sameData(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
