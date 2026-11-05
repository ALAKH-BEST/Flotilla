package server_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/linearize"
	"github.com/ALAKH-BEST/flotilla/raft"
	"github.com/ALAKH-BEST/flotilla/server"
)

// members returns the membership as the current leader sees it.
func (c *kvCluster) members() ([]int, bool) {
	for i := 0; i < c.n; i++ {
		c.mu.Lock()
		srv := c.servers[i]
		c.mu.Unlock()
		if srv == nil {
			continue
		}
		if st := srv.Node().Status(); st.Role == raft.Leader {
			return st.Members, true
		}
	}
	return nil, false
}

func (c *kvCluster) running(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i] != nil
}

// change tries a membership change on each running server in turn until the leader accepts it.
func (c *kvCluster) change(timeout time.Duration, do func(context.Context, *server.Server) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var last error = server.ErrNotLeader
	for ctx.Err() == nil {
		for i := 0; i < c.n; i++ {
			c.mu.Lock()
			srv := c.servers[i]
			c.mu.Unlock()
			if srv == nil {
				continue
			}
			err := do(ctx, srv)
			if err == nil {
				return nil
			}
			last = err
		}
		time.Sleep(30 * time.Millisecond)
	}
	return last
}

// Clients hammer the cluster while servers are added and removed (the leader included) at random,
// in between isolations and crashes. Removed servers keep running and never hear of their removal.
// Every operation is recorded and the whole history must be linearizable.
func TestLinearizableUnderMembershipChanges(t *testing.T) {
	const (
		slots   = 8 // IDs 0..7; the cluster starts with 0..2 and may grow to 5 voters
		clients = 5
		runFor  = 9 * time.Second
	)
	c := newKVMemberCluster(t, 3, slots, 10)
	keys := []string{"a", "b", "c"}

	stop := make(chan struct{})
	done := make(chan struct{})
	var adds, removes, isolations, crashes int
	go func() {
		defer close(done)
		rng := rand.New(rand.NewSource(11))
		next := 3 // the next slot never used
		for {
			select {
			case <-stop:
				return
			case <-time.After(250 * time.Millisecond):
			}
			members, ok := c.members()
			if !ok {
				continue
			}
			switch kind := rng.Intn(5); kind {
			case 0: // cut a running node off for a moment
				victim := rng.Intn(slots)
				if c.running(victim) {
					isolations++
					c.net.Isolate(victim)
					time.Sleep(400 * time.Millisecond)
					c.net.Reconnect(victim)
				}
			case 1: // crash a running node and bring it back from disk
				victim := rng.Intn(slots)
				if c.running(victim) {
					crashes++
					c.crash(victim)
					time.Sleep(400 * time.Millisecond)
					c.restart(victim)
				}
			case 2, 3: // add a brand-new node
				if len(members) >= 5 || next >= slots {
					continue
				}
				id := next
				next++
				c.joining[id] = true
				c.start(id)
				err := c.change(10*time.Second, func(ctx context.Context, s *server.Server) error {
					return s.AddServer(ctx, id, "")
				})
				if err == nil {
					adds++
				}
			default: // remove a member, maybe the leader
				if len(members) <= 3 {
					continue
				}
				id := members[rng.Intn(len(members))]
				err := c.change(4*time.Second, func(ctx context.Context, s *server.Server) error {
					return s.RemoveServer(ctx, id)
				})
				if err == nil {
					removes++
					if rng.Intn(2) == 0 {
						c.crash(id) // otherwise it keeps running, unaware that it was removed
					}
				}
			}
		}
	}()

	deadline := ctx(t, 120*time.Second)
	history := hammer(t, c, clients, keys, runFor, deadline)
	close(stop)
	<-done
	if t.Failed() {
		return
	}
	if ok, key := linearize.Check(history); !ok {
		t.Fatalf("history is not linearizable; operations on key %q:\n%s", key, linearize.Explain(history, key))
	}
	t.Logf("%d operations were linearizable; %d adds, %d removes, %d isolations, %d crashes", len(history), adds, removes, isolations, crashes)
	if adds+removes < 3 {
		t.Fatalf("only %d membership changes succeeded, so this run proved little", adds+removes)
	}

	// With everything back, the members must agree on the data.
	c.net.Heal()
	members, ok := c.members()
	for tries := 0; !ok && tries < 100; tries++ {
		time.Sleep(50 * time.Millisecond)
		members, ok = c.members()
	}
	if !ok {
		t.Fatal("no leader at the end")
	}
	for _, id := range members {
		c.restart(id) // does nothing for a member that is already running
	}
	if err := c.clerk(100).Put(deadline, "final", "x"); err != nil {
		t.Fatal(err)
	}
	waitMembersConverged(t, c, members)
}

// waitMembersConverged waits until every listed server holds the same data.
func waitMembersConverged(t *testing.T, c *kvCluster, ids []int) {
	t.Helper()
	limit := time.Now().Add(20 * time.Second)
	for {
		var first map[string]string
		same := true
		for _, id := range ids {
			c.mu.Lock()
			srv := c.servers[id]
			c.mu.Unlock()
			if srv == nil {
				same = false
				continue
			}
			data := srv.Store().Data()
			if first == nil {
				first = data
			} else if !sameData(first, data) {
				same = false
			}
		}
		if same {
			return
		}
		if time.Now().After(limit) {
			t.Fatalf("the members %v did not converge", ids)
		}
		time.Sleep(30 * time.Millisecond)
	}
}

// AddServer returns only once the new node is a voter that has caught up: it first joins as a
// learner and is promoted after it has received (here, mostly by snapshot) the data.
func TestAddServerWaitsForTheNewNodeToCatchUp(t *testing.T) {
	c := newKVMemberCluster(t, 3, 4, 10)
	deadline := ctx(t, 60*time.Second)
	cl := c.clerk(1)
	const keys = 200
	for i := 0; i < keys; i++ {
		if err := cl.Put(deadline, fmt.Sprintf("k%03d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	c.net.SetDelay(10*time.Millisecond, 20*time.Millisecond) // catching up now takes visible time
	c.joining[3] = true
	c.start(3)
	if err := c.change(30*time.Second, func(ctx context.Context, s *server.Server) error {
		return s.AddServer(ctx, 3, "")
	}); err != nil {
		t.Fatal(err)
	}

	// Promotion waited for the node to trail by at most a few entries, so it already has nearly
	// everything now, without any further waiting.
	if got := len(c.servers[3].Store().Data()); got < keys-10 {
		t.Fatalf("the new node was made a voter with only %d of %d keys", got, keys)
	}
	st := c.servers[3].Node().Status()
	if len(st.Members) != 4 || len(st.Learners) != 0 {
		t.Fatalf("the new node sees voters %v and learners %v, want 4 voters", st.Members, st.Learners)
	}
}
