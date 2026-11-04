// Package server turns a Raft node and a key-value store into a service that clients can
// call. Writes go through the replicated log, so a request that returns took effect at one
// point between its start and its end (linearizable), and a retried write is not run twice.
// Reads use Raft's read index: the leader confirms it is still the leader with a round of
// heartbeats, waits for its state machine to catch up to its commit index, and answers from
// local state. That is also linearizable, and writes nothing to disk.
package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ALAKH-BEST/flotilla/kv"
	"github.com/ALAKH-BEST/flotilla/raft"
)

var (
	// ErrNotLeader means this server is not the leader, or lost leadership before the
	// request committed. The request may or may not have taken effect; retrying it with
	// the same client ID and sequence number is safe.
	ErrNotLeader = errors.New("server: not the leader")
	// ErrTimeout means the request did not commit in time (for example the leader lost
	// its majority). Retry it the same way.
	ErrTimeout = errors.New("server: request timed out")
	// ErrStopped means the server was shut down.
	ErrStopped = errors.New("server: stopped")
)

// Config configures a Server.
type Config struct {
	// Raft configures the node. The server supplies ApplyCh itself.
	Raft raft.Config
	// SnapshotEvery compacts the log after this many applied entries (0 = never).
	SnapshotEvery int
	// RequestTimeout bounds how long Do waits for a commit. Zero means 2 seconds.
	RequestTimeout time.Duration
	// LogReads sends gets through the replicated log like writes instead of using the read
	// index. It is slower and exists for comparison and testing.
	LogReads bool
}

type outcome struct {
	result kv.Result
	err    error
}

type reader struct {
	index int
	ch    chan struct{} // closed when the state machine reaches index
}

type waiter struct {
	term int
	ch   chan outcome // buffered, receives at most one outcome
}

// Server is one replica of the service.
type Server struct {
	node          *raft.Node
	store         *kv.Store
	applyCh       chan raft.ApplyMsg
	snapshotEvery int
	timeout       time.Duration
	logReads      bool

	mu      sync.Mutex
	waiters map[int]*waiter // requests waiting for their log index to commit
	applied int             // the newest index the state machine has applied
	readers []*reader       // reads waiting for the state machine to reach an index

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

var _ raft.Handler = (*Server)(nil)

// New creates a server. Call Start to run it.
func New(cfg Config) (*Server, error) {
	applyCh := make(chan raft.ApplyMsg, 256)
	cfg.Raft.ApplyCh = applyCh
	node, err := raft.New(cfg.Raft)
	if err != nil {
		return nil, err
	}
	timeout := cfg.RequestTimeout
	if timeout == 0 {
		timeout = 2 * time.Second
	}
	return &Server{
		node:          node,
		store:         kv.NewStore(),
		applyCh:       applyCh,
		snapshotEvery: cfg.SnapshotEvery,
		timeout:       timeout,
		logReads:      cfg.LogReads,
		waiters:       make(map[int]*waiter),
		stopCh:        make(chan struct{}),
	}, nil
}

// Start runs the server.
func (s *Server) Start() {
	s.wg.Add(1)
	go s.applyLoop()
	s.node.Start()
}

// Stop shuts the server down.
func (s *Server) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.node.Stop()
	s.wg.Wait()
}

// Node exposes the underlying Raft node, for status and tests.
func (s *Server) Node() *raft.Node { return s.node }

// Store exposes the state machine, for tests that compare replicas.
func (s *Server) Store() *kv.Store { return s.store }

// HandleRequestVote, HandleAppendEntries and HandleInstallSnapshot pass Raft RPCs to the
// node, so a Server can be registered on the network in place of its node.
func (s *Server) HandleRequestVote(args *raft.RequestVoteArgs) *raft.RequestVoteReply {
	return s.node.HandleRequestVote(args)
}

func (s *Server) HandleAppendEntries(args *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	return s.node.HandleAppendEntries(args)
}

func (s *Server) HandleInstallSnapshot(args *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	return s.node.HandleInstallSnapshot(args)
}

// Do runs a command through the replicated log and returns its result.
func (s *Server) Do(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	if cmd.Op == kv.OpGet && !s.logReads {
		res, err := s.readLocal(ctx, cmd.Key)
		if !errors.Is(err, raft.ErrNotReady) {
			return res, err
		}
		// A leader that was just elected has not yet committed an entry of its own term, so
		// its commit index may be stale. Read through the log until it has.
	}
	// The lock is held from Propose until the waiter is registered, so the apply loop
	// cannot deliver the result before anyone is waiting for it.
	s.mu.Lock()
	index, term, isLeader := s.node.Propose(kv.Encode(cmd))
	if !isLeader {
		s.mu.Unlock()
		return kv.Result{}, ErrNotLeader
	}
	w := &waiter{term: term, ch: make(chan outcome, 1)}
	if old := s.waiters[index]; old != nil {
		// A newer leader's entry took this index, so the earlier request there was overwritten.
		old.ch <- outcome{err: ErrNotLeader}
	}
	s.waiters[index] = w
	s.mu.Unlock()

	forget := func() {
		s.mu.Lock()
		if s.waiters[index] == w {
			delete(s.waiters, index)
		}
		s.mu.Unlock()
	}
	timer := time.NewTimer(s.timeout)
	defer timer.Stop()
	select {
	case o := <-w.ch:
		return o.result, o.err
	case <-ctx.Done():
		forget()
		return kv.Result{}, ctx.Err()
	case <-timer.C:
		forget()
		return kv.Result{}, ErrTimeout
	case <-s.stopCh:
		return kv.Result{}, ErrStopped
	}
}

// readLocal serves a get with the read index: the leader proves it is still the leader, the
// state machine catches up to the commit index the leader had at that point, and the key is read locally.
func (s *Server) readLocal(ctx context.Context, key string) (kv.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	fail := func(err error) (kv.Result, error) {
		switch {
		case errors.Is(err, raft.ErrNotLeader):
			return kv.Result{}, ErrNotLeader
		case errors.Is(err, raft.ErrStopped):
			return kv.Result{}, ErrStopped
		case errors.Is(err, context.DeadlineExceeded):
			return kv.Result{}, ErrTimeout
		}
		return kv.Result{}, err
	}
	index, err := s.node.ReadIndex(ctx)
	if err != nil {
		return fail(err)
	}
	if err := s.waitApplied(ctx, index); err != nil {
		return fail(err)
	}
	v, ok := s.store.Get(key)
	return kv.Result{Value: v, Found: ok}, nil
}

// waitApplied blocks until the state machine has applied up to index.
func (s *Server) waitApplied(ctx context.Context, index int) error {
	s.mu.Lock()
	if s.applied >= index {
		s.mu.Unlock()
		return nil
	}
	r := &reader{index: index, ch: make(chan struct{})}
	s.readers = append(s.readers, r)
	s.mu.Unlock()
	select {
	case <-r.ch:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		for i, x := range s.readers {
			if x == r {
				s.readers = append(s.readers[:i], s.readers[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
		return ctx.Err()
	case <-s.stopCh:
		return raft.ErrStopped
	}
}

func (s *Server) applyLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stopCh:
			return
		case msg := <-s.applyCh:
			s.handle(msg)
		}
	}
}

func (s *Server) handle(msg raft.ApplyMsg) {
	result := s.store.Apply(msg)

	s.mu.Lock()
	if msg.SnapshotValid {
		s.applied = msg.SnapshotIndex
	} else {
		s.applied = msg.Index
	}
	kept := s.readers[:0]
	for _, r := range s.readers {
		if r.index <= s.applied {
			close(r.ch)
		} else {
			kept = append(kept, r)
		}
	}
	s.readers = kept
	if msg.SnapshotValid {
		// The state jumped forward. We cannot tell whether a waiting request ran, so
		// tell it to retry; the client's session makes the retry safe.
		for index, w := range s.waiters {
			if index <= msg.SnapshotIndex {
				w.ch <- outcome{err: ErrNotLeader}
				delete(s.waiters, index)
			}
		}
	} else if w := s.waiters[msg.Index]; w != nil {
		delete(s.waiters, msg.Index)
		if w.term == msg.Term {
			w.ch <- outcome{result: result}
		} else {
			w.ch <- outcome{err: ErrNotLeader} // another leader's entry committed at this index
		}
	}
	s.mu.Unlock()

	if !msg.SnapshotValid && s.snapshotEvery > 0 && msg.Index%s.snapshotEvery == 0 {
		// The store has applied exactly up to msg.Index, so its snapshot matches that index.
		_ = s.node.Snapshot(msg.Index, s.store.Snapshot())
	}
}
