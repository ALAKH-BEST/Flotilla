package raft_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
	"github.com/ALAKH-BEST/flotilla/simnet"
)

// gateStorage wraps a storage so a test can slow every save down, hold saves back, and see what
// was saved and in what order.
type gateStorage struct {
	raft.Storage
	delay time.Duration

	mu      sync.Mutex
	gate    chan struct{} // while non-nil, a save waits for it to be closed
	saves   int
	started chan struct{} // gets a value each time a save begins
	logLens []int
}

func newGateStorage(delay time.Duration) *gateStorage {
	return &gateStorage{Storage: raft.NewMemoryStorage(), delay: delay, started: make(chan struct{}, 1024)}
}

func (g *gateStorage) Save(state raft.State) error {
	g.mu.Lock()
	gate := g.gate
	g.mu.Unlock()
	g.started <- struct{}{}
	if gate != nil {
		<-gate
	}
	if g.delay > 0 {
		time.Sleep(g.delay) // stands for the cost of a disk sync
	}
	if err := g.Storage.Save(state); err != nil {
		return err
	}
	g.mu.Lock()
	g.saves++
	g.logLens = append(g.logLens, len(state.Log))
	g.mu.Unlock()
	return nil
}

func (g *gateStorage) hold() chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gate = make(chan struct{})
	return g.gate
}

func (g *gateStorage) stats() (saves int, logLens []int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.saves, append([]int(nil), g.logLens...)
}

// aloneNode starts a one-node cluster on the given storage and waits for it to lead.
func aloneNode(t *testing.T, storage raft.Storage) (*raft.Node, chan raft.ApplyMsg) {
	t.Helper()
	net := simnet.New(1)
	applyCh := make(chan raft.ApplyMsg, 4096)
	node, err := raft.New(raft.Config{
		ID: 0, Peers: []int{0}, Transport: net.Transport(0), Storage: storage, ApplyCh: applyCh, Seed: 1,
		ElectionTimeoutMin: 50 * time.Millisecond, ElectionTimeoutMax: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	net.Register(0, node)
	node.Start()
	t.Cleanup(node.Stop)
	deadline := time.Now().Add(5 * time.Second)
	for node.Status().Role != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatal("the node never became leader")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Let the entry a new leader appends on taking office settle, so the counts below start clean.
	for node.Status().CommitIndex < node.Status().LastLogIndex {
		time.Sleep(5 * time.Millisecond)
	}
	return node, applyCh
}

// Commands proposed at the same time share syncs: far fewer saves than commands, and every
// command still commits, in the order it was appended.
func TestConcurrentProposalsShareSyncs(t *testing.T) {
	storage := newGateStorage(5 * time.Millisecond)
	node, applyCh := aloneNode(t, storage)
	before, _ := storage.stats()

	const clients, each = 32, 10
	var wg sync.WaitGroup
	indexes := make(chan int, clients*each)
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				index, _, ok := node.Propose([]byte(fmt.Sprintf("c%d-%d", c, i)))
				if !ok {
					t.Errorf("the leader refused a proposal")
					return
				}
				indexes <- index
			}
		}(c)
	}
	wg.Wait()
	close(indexes)

	seen := map[int]bool{}
	last := 0
	for index := range indexes {
		if seen[index] {
			t.Fatalf("index %d was handed out twice", index)
		}
		seen[index] = true
		last = max(last, index)
	}
	if len(seen) != clients*each {
		t.Fatalf("%d distinct indexes for %d proposals", len(seen), clients*each)
	}

	deadline := time.Now().Add(5 * time.Second)
	for node.Status().CommitIndex < last {
		if time.Now().After(deadline) {
			t.Fatalf("commit index stuck at %d, want %d", node.Status().CommitIndex, last)
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := 0
	deadline = time.Now().Add(5 * time.Second)
	for got < clients*each && time.Now().Before(deadline) {
		select {
		case msg := <-applyCh:
			if !msg.NoOp && !msg.SnapshotValid && len(msg.Command) > 0 {
				got++
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if got < clients*each {
		t.Fatalf("applied %d commands, want %d", got, clients*each)
	}

	after, _ := storage.stats()
	saves := after - before
	t.Logf("%d proposals took %d saves", clients*each, saves)
	if saves > clients*each/4 {
		t.Fatalf("%d proposals took %d saves; they should share syncs", clients*each, saves)
	}
}

// A leader must not count a command as its own, and so must not commit it, before the command is
// on disk. Even a one-node cluster, which needs nobody else's vote, waits for the save.
func TestCommandIsNotCommittedBeforeItIsDurable(t *testing.T) {
	storage := newGateStorage(0)
	node, _ := aloneNode(t, storage)

	release := storage.hold()
	type result struct{ index int }
	done := make(chan result, 1)
	go func() {
		index, _, _ := node.Propose([]byte("held"))
		done <- result{index}
	}()
	<-storage.started // the save has begun and is held

	select {
	case r := <-done:
		t.Fatalf("Propose returned (index %d) before the command was durable", r.index)
	case <-time.After(300 * time.Millisecond):
	}
	if st := node.Status(); st.CommitIndex >= st.LastLogIndex {
		t.Fatalf("committed %d of %d while the save was held back", st.CommitIndex, st.LastLogIndex)
	}

	close(release)
	r := <-done
	deadline := time.Now().Add(5 * time.Second)
	for node.Status().CommitIndex < r.index {
		if time.Now().After(deadline) {
			t.Fatal("the command never committed after the save finished")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The node keeps answering while a save is in progress: group commit must not hold the node's
// lock across the sync.
func TestNodeStaysResponsiveDuringASync(t *testing.T) {
	storage := newGateStorage(0)
	node, _ := aloneNode(t, storage)

	release := storage.hold()
	go node.Propose([]byte("held"))
	<-storage.started

	statusDone := make(chan struct{})
	go func() {
		node.Status()
		close(statusDone)
	}()
	select {
	case <-statusDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Status blocked behind a save in progress")
	}
	close(release)
}

// Saves reach storage in the order the states were captured, so the log on disk never shrinks
// behind the node's back while one leader appends.
func TestSavesNeverGoBackwards(t *testing.T) {
	storage := newGateStorage(time.Millisecond)
	node, _ := aloneNode(t, storage)

	var wg sync.WaitGroup
	for c := 0; c < 16; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				node.Propose([]byte("x"))
			}
		}()
	}
	wg.Wait()

	_, lens := storage.stats()
	for i := 1; i < len(lens); i++ {
		if lens[i] < lens[i-1] {
			t.Fatalf("save %d wrote a log of %d entries after one of %d: an older state overtook a newer one", i, lens[i], lens[i-1])
		}
	}
	if node.Status().LastLogIndex < 16*20 {
		t.Fatalf("log has %d entries, want at least %d", node.Status().LastLogIndex, 16*20)
	}
}

// A proposal waiting for its sync is released, refused, when the node stops.
func TestStopReleasesAProposalWaitingForItsSync(t *testing.T) {
	storage := newGateStorage(0)
	net := simnet.New(1)
	node, err := raft.New(raft.Config{
		ID: 0, Peers: []int{0}, Transport: net.Transport(0), Storage: storage, ApplyCh: make(chan raft.ApplyMsg, 64), Seed: 1,
		ElectionTimeoutMin: 50 * time.Millisecond, ElectionTimeoutMax: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	net.Register(0, node)
	node.Start()
	for node.Status().Role != raft.Leader {
		time.Sleep(5 * time.Millisecond)
	}
	release := storage.hold()
	result := make(chan bool, 1)
	go func() {
		_, _, ok := node.Propose([]byte("held"))
		result <- ok
	}()
	<-storage.started

	stopped := make(chan struct{})
	go func() {
		node.Stop()
		close(stopped)
	}()
	select {
	case ok := <-result:
		if ok {
			t.Fatal("a proposal that never became durable was reported as accepted after Stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a waiting proposal was not released by Stop")
	}
	close(release) // Stop waits for the save in flight
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop never returned")
	}
}
