package server_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/kv"
	"github.com/ALAKH-BEST/flotilla/linearize"
	"github.com/ALAKH-BEST/flotilla/raft"
	"github.com/ALAKH-BEST/flotilla/server"
)

func ctx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return c
}

func TestPutGetDelete(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	cl := c.clerk(1)
	ctx := ctx(t, 20*time.Second)

	if _, found, err := cl.Get(ctx, "x"); err != nil || found {
		t.Fatalf("get of a missing key = found %v, err %v", found, err)
	}
	if err := cl.Put(ctx, "x", "1"); err != nil {
		t.Fatal(err)
	}
	if v, found, err := cl.Get(ctx, "x"); err != nil || !found || v != "1" {
		t.Fatalf("get = %q, %v, %v; want 1", v, found, err)
	}
	if err := cl.Delete(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := cl.Get(ctx, "x"); found {
		t.Fatal("key still present after delete")
	}
}

// A request that reaches the leader twice (a client retry after a timeout) must run once,
// even if the client has moved on to a newer request in between.
func TestRetriedRequestRunsOnce(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	ctx := ctx(t, 20*time.Second)
	cl := c.clerk(1)
	if err := cl.Put(ctx, "warm", "up"); err != nil { // makes sure a leader exists
		t.Fatal(err)
	}

	do := func(cmd kv.Command) kv.Result {
		for i := 0; i < 3; i++ {
			for _, d := range c.doers() {
				if res, err := d.Do(ctx, cmd); err == nil {
					return res
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("no server accepted the command")
		return kv.Result{}
	}
	do(kv.Command{Op: kv.OpPut, Key: "k", Value: "old", ClientID: 99, Seq: 1})
	do(kv.Command{Op: kv.OpPut, Key: "k", Value: "new", ClientID: 99, Seq: 2})
	do(kv.Command{Op: kv.OpPut, Key: "k", Value: "old", ClientID: 99, Seq: 1}) // a late duplicate of the first

	if v, _, _ := cl.Get(ctx, "k"); v != "new" {
		t.Fatalf("k = %q, want new: a duplicate request ran a second time", v)
	}
}

func TestFollowerRejectsRequests(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	cl := c.clerk(1)
	if err := cl.Put(ctx(t, 20*time.Second), "x", "1"); err != nil {
		t.Fatal(err)
	}
	rejected := 0
	for _, d := range c.doers() {
		_, err := d.Do(context.Background(), kv.Command{Op: kv.OpGet, Key: "x", ClientID: 5, Seq: 1})
		if err != nil {
			rejected++
		}
	}
	if rejected != 2 {
		t.Fatalf("%d of 3 servers rejected a request, want exactly 2 (the followers)", rejected)
	}
}

// Clients hammer the cluster while a nemesis isolates, partitions and crashes nodes.
// Every operation is recorded and the whole history must be linearizable.
func TestLinearizableUnderFailures(t *testing.T) {
	const (
		servers   = 5
		clients   = 5
		runFor    = 6 * time.Second
		snapEvery = 10
	)
	c := newKVCluster(t, servers, snapEvery)
	keys := []string{"a", "b", "c"}

	stop := make(chan struct{})
	var nemesis sync.WaitGroup
	var events [3]int
	nemesis.Add(1)
	go func() {
		defer nemesis.Done()
		rng := rand.New(rand.NewSource(3))
		for {
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
			}
			victim := rng.Intn(servers)
			kind := rng.Intn(3)
			events[kind]++
			switch kind {
			case 0: // cut one node off
				c.net.Isolate(victim)
				time.Sleep(400 * time.Millisecond)
				c.net.Reconnect(victim)
			case 1: // crash one node and bring it back from disk
				c.crash(victim)
				time.Sleep(400 * time.Millisecond)
				c.restart(victim)
			default: // split the cluster 2 / 3
				other := (victim + 1) % servers
				var rest []int
				for i := 0; i < servers; i++ {
					if i != victim && i != other {
						rest = append(rest, i)
					}
				}
				c.net.Partition([]int{victim, other}, rest)
				time.Sleep(400 * time.Millisecond)
				c.net.Heal()
			}
		}
	}()

	deadline := ctx(t, 90*time.Second)
	history := hammer(t, c, clients, keys, runFor, deadline)
	close(stop)
	nemesis.Wait()
	if t.Failed() {
		return
	}

	if ok, key := linearize.Check(history); !ok {
		t.Fatalf("history is not linearizable; operations on key %q:\n%s", key, linearize.Explain(history, key))
	}
	t.Logf("%d operations were linearizable; nemesis: %d isolations, %d crashes, %d partitions", len(history), events[0], events[1], events[2])
	if events[0]+events[1]+events[2] < 4 {
		t.Fatalf("only %d failures were injected, so this run proved little", events[0]+events[1]+events[2])
	}

	// With the network healed and everyone back, all replicas must agree.
	c.net.Heal()
	for i := 0; i < servers; i++ {
		c.restart(i)
	}
	if err := c.clerk(100).Put(deadline, "final", "x"); err != nil {
		t.Fatal(err)
	}
	c.waitConverged()
}

// A leader that is cut off from the majority does not know it has been replaced. It must
// not answer a read from its own copy of the data: the majority may have moved on.
func TestIsolatedLeaderCannotServeStaleRead(t *testing.T) {
	const servers = 5
	c := newKVCluster(t, servers, 0)
	deadline := ctx(t, 60*time.Second)

	if err := c.clerk(1).Put(deadline, "x", "old"); err != nil {
		t.Fatal(err)
	}
	old := -1
	for i := 0; i < servers && old < 0; i++ {
		c.mu.Lock()
		role := c.servers[i].Node().Status().Role
		c.mu.Unlock()
		if role == raft.Leader {
			old = i
		}
	}
	if old < 0 {
		t.Fatal("no leader")
	}

	c.net.Isolate(old)
	var others []server.Doer
	for i, d := range c.doers() {
		if i != old {
			others = append(others, d)
		}
	}
	if err := server.NewClerk(2, others).Put(deadline, "x", "new"); err != nil {
		t.Fatal(err)
	}

	// The isolated old leader still believes it leads. Asking it must fail, not return "old".
	readCtx, cancel := context.WithTimeout(deadline, 1500*time.Millisecond)
	defer cancel()
	res, err := liveDoer{c: c, i: old}.Do(readCtx, kv.Command{Op: kv.OpGet, Key: "x", ClientID: 3, Seq: 1})
	if err == nil {
		t.Fatalf("the isolated old leader answered a read with %+v; the majority already holds %q", res, "new")
	}

	c.net.Reconnect(old)
	if v, _, err := c.clerk(4).Get(deadline, "x"); err != nil || v != "new" {
		t.Fatalf("after healing, get = %q, %v; want new", v, err)
	}
}

func (c *kvCluster) leader(t *testing.T) int {
	t.Helper()
	limit := time.Now().Add(10 * time.Second)
	for time.Now().Before(limit) {
		for i := range c.servers {
			c.mu.Lock()
			srv := c.servers[i]
			c.mu.Unlock()
			if srv != nil && srv.Node().Status().Role == raft.Leader {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader")
	return -1
}

// Reads are served from the leader's state machine and add nothing to the log, so they cost
// no disk write. Writes still do.
func TestReadsDoNotGrowTheLog(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	deadline := ctx(t, 30*time.Second)
	cl := c.clerk(1)
	if err := cl.Put(deadline, "k", "v"); err != nil {
		t.Fatal(err)
	}
	node := c.servers[c.leader(t)].Node()
	before := node.Status().LastLogIndex
	for i := 0; i < 50; i++ {
		v, found, err := cl.Get(deadline, "k")
		if err != nil || !found || v != "v" {
			t.Fatalf("get = %q, %v, %v", v, found, err)
		}
	}
	if after := node.Status().LastLogIndex; after != before {
		t.Fatalf("50 reads grew the log from %d to %d entries", before, after)
	}
	if err := cl.Put(deadline, "k", "w"); err != nil {
		t.Fatal(err)
	}
	if after := node.Status().LastLogIndex; after != before+1 {
		t.Fatalf("a write should add one entry: %d -> %d", before, after)
	}
}

// A read always sees the writes that finished before it started, including right after a
// leader change when the new leader has to catch its state machine up first.
func TestReadSeesCompletedWritesAcrossLeaderChanges(t *testing.T) {
	const servers = 5
	c := newKVCluster(t, servers, 0)
	deadline := ctx(t, 60*time.Second)
	cl := c.clerk(1)
	for round := 0; round < 4; round++ {
		want := fmt.Sprintf("v%d", round)
		if err := cl.Put(deadline, "x", want); err != nil {
			t.Fatal(err)
		}
		if got, _, err := cl.Get(deadline, "x"); err != nil || got != want {
			t.Fatalf("round %d: get = %q, %v; want %q", round, got, err, want)
		}
		c.net.Isolate(c.leader(t)) // force an election, then read from the new leader
		if got, _, err := c.clerk(int64(10+round)).Get(deadline, "x"); err != nil || got != want {
			t.Fatalf("round %d after the leader change: get = %q, %v; want %q", round, got, err, want)
		}
		c.net.Heal()
	}
}

// Only the leader answers a read.
func TestFollowerRefusesRead(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	deadline := ctx(t, 30*time.Second)
	if err := c.clerk(1).Put(deadline, "k", "v"); err != nil {
		t.Fatal(err)
	}
	leader := c.leader(t)
	follower := (leader + 1) % 3
	_, err := liveDoer{c: c, i: follower}.Do(deadline, kv.Command{Op: kv.OpGet, Key: "k"})
	if err != server.ErrNotLeader {
		t.Fatalf("a follower's answer to a read was %v, want ErrNotLeader", err)
	}
}

// hammer runs clients against the cluster for runFor, recording every operation with the times it
// started and finished, for the linearizability checker.
func hammer(t *testing.T, c *kvCluster, clients int, keys []string, runFor time.Duration, deadline context.Context) []linearize.Op {
	t.Helper()
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	var mu sync.Mutex
	var history []linearize.Op
	record := func(op linearize.Op) {
		mu.Lock()
		history = append(history, op)
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for id := 1; id <= clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cl := c.clerk(int64(id))
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; time.Since(start) < runFor; i++ {
				time.Sleep(time.Duration(30+rng.Intn(80)) * time.Millisecond)
				key := keys[rng.Intn(len(keys))]
				op := linearize.Op{Client: id, Key: key}
				op.Call = now()
				switch r := rng.Intn(10); {
				case r < 4:
					op.Kind, op.Value = linearize.Put, fmt.Sprintf("c%d-%d", id, i)
					if err := cl.Put(deadline, key, op.Value); err != nil {
						t.Errorf("client %d: put: %v", id, err)
						return
					}
				case r < 5:
					op.Kind = linearize.Delete
					if err := cl.Delete(deadline, key); err != nil {
						t.Errorf("client %d: delete: %v", id, err)
						return
					}
				default:
					op.Kind = linearize.Get
					v, found, err := cl.Get(deadline, key)
					if err != nil {
						t.Errorf("client %d: get: %v", id, err)
						return
					}
					op.Value, op.Found = v, found
				}
				op.Return = now()
				record(op)
			}
		}(id)
	}
	wg.Wait()
	return history
}
