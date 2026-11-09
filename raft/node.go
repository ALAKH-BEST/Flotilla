// Package raft is an implementation of the Raft consensus algorithm
// (Ongaro and Ousterhout, "In Search of an Understandable Consensus Algorithm").
//
// A Node runs leader election and log replication. Committed entries are
// delivered in order on the ApplyCh given in Config. Networking and storage are
// behind the Transport and Storage interfaces so the protocol can be tested
// against a simulated network.
package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Config configures a Node.
type Config struct {
	// ID is this node's ID. Peers (or Members, when the nodes have addresses) lists every
	// node of a new cluster, including this one. They are only the starting membership:
	// once a node has saved state, the membership recorded in its log and snapshot wins.
	ID      int
	Peers   []int
	Members []Member
	// Join starts a node that is not yet part of any cluster. It has no members, so it does
	// not campaign, and learns the membership from the leader once it is added with AddLearner.
	Join bool
	// OnMembership is called with the voting members whenever this node's view of them
	// changes (and once at Start). It runs with the node's lock held, so it must be quick
	// and must not call back into the node.
	OnMembership func([]Member)
	// OnRemoved is called once, from its own goroutine, when this node learns that a committed
	// membership change removed it from the cluster. The node then has nothing left to do: the
	// caller should stop it.
	OnRemoved func()
	// OnDeparted is called, like OnMembership with the lock held, when this node becomes leader
	// and starts telling removed servers, with those servers and their addresses. A removed
	// server is in no membership any more, so this is how the caller learns where to reach it.
	OnDeparted func([]Member)

	Transport Transport
	Storage   Storage
	// ApplyCh receives committed entries in log order. The caller must keep reading it.
	ApplyCh chan<- ApplyMsg

	// Timings; zero values use the defaults below.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	TickInterval       time.Duration
	// SnapshotChunkSize is the most snapshot bytes sent in one message (default 64 KiB).
	SnapshotChunkSize int

	// Seed for the election timeout jitter. Zero picks one from the clock and ID.
	Seed int64
}

const (
	defaultElectionTimeoutMin = 150 * time.Millisecond
	defaultElectionTimeoutMax = 300 * time.Millisecond
	defaultHeartbeatInterval  = 40 * time.Millisecond
	defaultTickInterval       = 10 * time.Millisecond
	defaultSnapshotChunkSize  = 64 << 10
)

// Node is one member of a Raft cluster.
type Node struct {
	cfg Config

	mu        sync.Mutex
	applyCond *sync.Cond
	rng       *rand.Rand

	// Persistent state (saved to Storage before replying to any RPC that changes it).
	currentTerm int
	votedFor    int // -1 when the node has not voted this term
	// log[0] is a sentinel for the entry at snapshotIndex; log[i] is the entry at
	// snapshotIndex+i. Entries up to snapshotIndex live only in the snapshot.
	log           []Entry
	snapshotIndex int
	snapshotTerm  int
	snapshot      []byte // never modified once set, so it can be shared with RPCs
	// snapshotMembers is the membership as of snapshotIndex; members is the one in use now.
	snapshotMembers []Member
	// snapshotGone are recently removed servers, as of snapshotIndex. Never modified once set.
	snapshotGone []Member
	members      []Member

	// Volatile state.
	commitIndex int
	lastApplied int
	role        Role
	leaderID    int
	// pendingSnapshot is a snapshot waiting to be handed to the state machine, which
	// must happen before any entry that follows it.
	pendingSnapshot *ApplyMsg
	// incoming holds a snapshot being received in chunks. Nothing in it is used until it is complete.
	incoming *incomingSnapshot

	electionDeadline time.Time
	nextHeartbeat    time.Time

	// Leader state, reset on every election win.
	nextIndex  map[int]int
	matchIndex map[int]int
	// sendSeq numbers the messages the leader sends; ackSeq is the newest one each peer answered
	// in this term. Together they prove, for ReadIndex, that a majority still follows us.
	sendSeq int
	ackSeq  map[int]int
	reads   []*readReq
	// lastContact is when each peer last answered the leader in its term. A leader that cannot
	// reach a majority steps down instead of staying leader in name only.
	lastContact map[int]time.Time
	// sendingSnap marks the peers a snapshot is being streamed to, so that heartbeats do not
	// start a second, competing transfer. The value identifies the transfer that owns the mark.
	sendingSnap map[int]int
	// snapOffset remembers how far a transfer got, so one interrupted by a lost message resumes
	// instead of starting over. The follower corrects it if it is wrong.
	snapOffset map[int]snapProgress
	// departing are servers that were removed and may not know it yet: the leader keeps offering
	// them the log, now and then, until one confirms it has the removal committed.
	departing map[int]*departure
	// removed is set once a committed change has removed this node.
	removed bool
	// lastLeaderContact is when this node last heard from a leader that was valid for its term.
	// epoch counts resets of the election timer, so a pre-vote round can tell it went stale.
	lastLeaderContact time.Time
	epoch             int

	started  bool
	stopped  bool
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

var _ Handler = (*Node)(nil)

// New creates a node, restoring its persistent state from cfg.Storage if there is any.
// Call Start to run it.
func New(cfg Config) (*Node, error) {
	if cfg.Transport == nil || cfg.Storage == nil || cfg.ApplyCh == nil {
		return nil, errors.New("raft: Transport, Storage and ApplyCh are required")
	}
	var bootstrap []Member
	if !cfg.Join {
		bootstrap = cfg.Members
		if len(bootstrap) == 0 {
			for _, id := range cfg.Peers {
				bootstrap = append(bootstrap, Member{ID: id})
			}
		}
		self := false
		for _, m := range bootstrap {
			if m.ID == cfg.ID {
				self = true
			}
		}
		if !self {
			return nil, fmt.Errorf("raft: the starting members must include the node's own ID %d", cfg.ID)
		}
		bootstrap = sortedMembers(bootstrap)
	}
	if cfg.ElectionTimeoutMin == 0 {
		cfg.ElectionTimeoutMin = defaultElectionTimeoutMin
	}
	if cfg.ElectionTimeoutMax == 0 {
		cfg.ElectionTimeoutMax = defaultElectionTimeoutMax
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		return nil, errors.New("raft: ElectionTimeoutMax must be greater than ElectionTimeoutMin")
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}
	if cfg.TickInterval == 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if cfg.SnapshotChunkSize <= 0 {
		cfg.SnapshotChunkSize = defaultSnapshotChunkSize
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano() + int64(cfg.ID)*7919
	}

	n := &Node{
		cfg:        cfg,
		rng:        rand.New(rand.NewSource(seed)),
		votedFor:   -1,
		log:        []Entry{{}}, // index 0 is a sentinel so real entries start at 1
		leaderID:   -1,
		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),
		ackSeq:     make(map[int]int),

		lastContact: make(map[int]time.Time),
		sendingSnap: make(map[int]int),
		snapOffset:  make(map[int]snapProgress),
		departing:   make(map[int]*departure),

		snapshotMembers: bootstrap,
		stopCh:          make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)

	state, ok, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}
	if ok {
		n.currentTerm = state.CurrentTerm
		n.votedFor = state.VotedFor
		n.log = state.Log
		n.snapshotIndex = state.SnapshotIndex
		n.snapshotTerm = state.SnapshotTerm
		n.snapshot = state.Snapshot
		if len(state.SnapshotMembers) > 0 {
			n.snapshotMembers = state.SnapshotMembers
		}
		n.snapshotGone = state.SnapshotGone
		if len(n.log) == 0 {
			n.log = []Entry{{Term: n.snapshotTerm}}
		}
		// Everything up to the snapshot is already committed. The state machine starts
		// empty, so the snapshot is handed to it first.
		n.commitIndex = n.snapshotIndex
		n.lastApplied = n.snapshotIndex
		if n.snapshotIndex > 0 {
			n.pendingSnapshot = n.snapshotMsg()
		}
	}
	n.members = n.membersAt(n.lastLogIndex())
	return n, nil
}

// Start runs the node's background work. It must be called once.
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started || n.stopped {
		return
	}
	n.started = true
	n.resetElectionDeadline()
	if n.cfg.OnMembership != nil {
		n.cfg.OnMembership(append([]Member(nil), n.members...))
	}
	n.wg.Add(2)
	go n.run()
	go n.applier()
}

// Stop shuts the node down and waits for its background goroutines. It is safe to call more than once.
func (n *Node) Stop() {
	n.mu.Lock()
	n.stopped = true
	n.failReads(ErrStopped)
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.stopOnce.Do(func() { close(n.stopCh) })
	n.wg.Wait()
}

// Propose appends a command to the log if this node is the leader. It returns the
// index and term the command will have if it is committed. A true isLeader does
// not guarantee the command commits: the leader can lose its role first.
func (n *Node) Propose(command []byte) (index, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.role != Leader {
		return 0, n.currentTerm, false
	}
	n.log = append(n.log, Entry{Term: n.currentTerm, Command: command})
	n.persist()
	index = n.lastLogIndex()
	n.matchIndex[n.cfg.ID] = index
	n.broadcastAppendEntries()
	n.advanceCommit() // a single-node cluster commits immediately
	return index, n.currentTerm, true
}

// Status reports the node's current state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:           n.cfg.ID,
		Term:         n.currentTerm,
		Role:         n.role,
		LeaderID:     n.leaderID,
		CommitIndex:  n.commitIndex,
		LastLogIndex: n.lastLogIndex(),

		SnapshotIndex: n.snapshotIndex,
		LogEntries:    len(n.log) - 1,
		Members:       n.voterIDs(),
		Learners:      n.learnerIDs(),
		Removed:       n.removed,
	}
}

// Snapshot tells the node that the state machine has applied everything up to index
// and can summarize it as data. The node drops the log up to index and keeps data
// to bring slow followers and restarted nodes up to date. Calls for an index that is
// already covered are ignored; an index that is not yet committed is an error.
func (n *Node) Snapshot(index int, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || index <= n.snapshotIndex {
		return nil
	}
	if index > n.commitIndex {
		return fmt.Errorf("raft: cannot snapshot index %d: only %d is committed", index, n.commitIndex)
	}
	term := n.termAt(index)
	members := n.membersAt(index) // before the log below index is dropped
	gone := n.goneAfter(index)
	n.log = append([]Entry{{Term: term}}, n.log[index-n.snapshotIndex+1:]...)
	n.snapshotMembers = members
	n.snapshotGone = gone
	n.snapshotIndex = index
	n.snapshotTerm = term
	n.snapshot = append([]byte(nil), data...)
	n.persist()
	return nil
}

// ---------------------------------------------------------------------------
// Background loops

func (n *Node) run() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case now := <-ticker.C:
			n.tick(now)
		}
	}
}

func (n *Node) tick(now time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	if n.role == Leader {
		if !n.hasQuorumContact(now) {
			// Cut off from a majority: this node cannot commit anything and may already have been
			// replaced. Stepping down lets clients move on to a leader that can answer.
			n.becomeFollower(n.currentTerm)
			n.resetElectionDeadline()
			return
		}
		if !now.Before(n.nextHeartbeat) {
			n.nextHeartbeat = now.Add(n.cfg.HeartbeatInterval)
			n.broadcastAppendEntries()
		}
		return
	}
	if !now.Before(n.electionDeadline) {
		if !n.isVoter(n.cfg.ID) {
			n.resetElectionDeadline() // not (or no longer) a voter: never campaign
			return
		}
		n.startPreVote()
	}
}

// applier delivers snapshots and committed entries to ApplyCh, in order, outside the lock.
func (n *Node) applier() {
	defer n.wg.Done()
	n.mu.Lock()
	for {
		for !n.stopped && n.pendingSnapshot == nil && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		if snap := n.pendingSnapshot; snap != nil {
			n.pendingSnapshot = nil
			n.mu.Unlock()
			select {
			case n.cfg.ApplyCh <- *snap:
			case <-n.stopCh:
				return
			}
			n.mu.Lock()
			continue
		}
		first, last := n.lastApplied+1, n.commitIndex
		if first <= n.snapshotIndex {
			first = n.snapshotIndex + 1 // the state machine already has these through its own snapshot
		}
		msgs := make([]ApplyMsg, 0, max(0, last-first+1))
		for i := first; i <= last; i++ {
			e := n.log[i-n.snapshotIndex]
			// membership entries carry no client command, so the state machine sees them as no-ops
			msgs = append(msgs, ApplyMsg{Index: i, Term: e.Term, Command: e.Command, NoOp: e.NoOp || len(e.Members) > 0})
		}
		n.mu.Unlock()
		for _, m := range msgs {
			select {
			case n.cfg.ApplyCh <- m:
			case <-n.stopCh:
				return
			}
		}
		n.mu.Lock()
		if n.lastApplied < last { // a snapshot installed meanwhile may have moved it further
			n.lastApplied = last
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers (the lock must be held)

func (n *Node) lastLogIndex() int { return n.snapshotIndex + len(n.log) - 1 }
func (n *Node) lastLogTerm() int  { return n.log[len(n.log)-1].Term }

// termAt returns the term of the entry at index, which must lie between
// snapshotIndex and lastLogIndex.
func (n *Node) termAt(index int) int { return n.log[index-n.snapshotIndex].Term }

// entriesFrom returns a copy of the entries from index to the end of the log.
func (n *Node) entriesFrom(index int) []Entry {
	return append([]Entry(nil), n.log[index-n.snapshotIndex:]...)
}

func (n *Node) snapshotMsg() *ApplyMsg {
	return &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      n.snapshot,
		SnapshotIndex: n.snapshotIndex,
		SnapshotTerm:  n.snapshotTerm,
	}
}

func (n *Node) resetElectionDeadline() {
	n.epoch++
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.electionDeadline = time.Now().Add(n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int63n(int64(span))))
}

// persist saves the persistent state. If storage fails the node cannot keep its
// promises (a vote or an acknowledged entry could be forgotten after a restart),
// so it stops rather than carry on.
func (n *Node) persist() {
	err := n.cfg.Storage.Save(State{
		CurrentTerm:   n.currentTerm,
		VotedFor:      n.votedFor,
		Log:           append([]Entry(nil), n.log...),
		SnapshotIndex: n.snapshotIndex,
		SnapshotTerm:  n.snapshotTerm,
		Snapshot:      n.snapshot,

		SnapshotMembers: n.snapshotMembers,
		SnapshotGone:    n.snapshotGone,
	})
	if err != nil {
		panic(fmt.Sprintf("raft: node %d cannot persist state: %v", n.cfg.ID, err))
	}
}

// becomeFollower moves to a follower of the given term. A newer term clears the vote.
func (n *Node) becomeFollower(term int) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = -1
		n.persist()
	}
	n.role = Follower
	if n.leaderID == n.cfg.ID {
		n.leaderID = -1 // a node that stopped leading must not keep naming itself leader
	}
	n.failReads(ErrNotLeader)
}
