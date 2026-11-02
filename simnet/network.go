// Package simnet is an in-memory network for testing a Raft cluster under failures:
// lost messages, delay, isolated nodes and partitions.
package simnet

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
)

// ErrUnreachable is returned when a message or its reply does not get through.
var ErrUnreachable = errors.New("simnet: unreachable")

// Network connects Raft nodes in one process.
type Network struct {
	mu       sync.Mutex
	rng      *rand.Rand
	handlers map[int]raft.Handler
	isolated map[int]bool
	group    map[int]int // partition group of each node; nodes in different groups cannot talk
	dropRate float64
	minDelay time.Duration
	maxDelay time.Duration
}

// New creates a network. The seed makes drops and delays reproducible in a single-threaded test.
func New(seed int64) *Network {
	return &Network{
		rng:      rand.New(rand.NewSource(seed)),
		handlers: make(map[int]raft.Handler),
		isolated: make(map[int]bool),
		group:    make(map[int]int),
	}
}

// Register attaches a node so it can receive messages. Registering again replaces the
// handler, which is how a restarted node comes back.
func (n *Network) Register(id int, h raft.Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Unregister detaches a node, as if its process had crashed.
func (n *Network) Unregister(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

// Isolate cuts a node off from everyone, in both directions.
func (n *Network) Isolate(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isolated[id] = true
}

// Reconnect undoes Isolate.
func (n *Network) Reconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.isolated, id)
}

// Partition splits the listed nodes into groups that cannot reach each other.
// Nodes not listed stay in group 0 together.
func (n *Network) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = make(map[int]int)
	for i, g := range groups {
		for _, id := range g {
			n.group[id] = i + 1
		}
	}
}

// Heal removes all partitions and isolation.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = make(map[int]int)
	n.isolated = make(map[int]bool)
}

// SetDropRate makes each one-way message fail with probability p (0 to 1).
func (n *Network) SetDropRate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = p
}

// SetDelay gives each one-way message a random delay between min and max.
func (n *Network) SetDelay(min, max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.minDelay, n.maxDelay = min, max
}

// Transport returns the transport a node uses to send messages.
func (n *Network) Transport(from int) raft.Transport { return &endpoint{net: n, from: from} }

type endpoint struct {
	net  *Network
	from int
}

func (e *endpoint) RequestVote(to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply *raft.RequestVoteReply
	err := e.net.deliver(e.from, to, func(h raft.Handler) { reply = h.HandleRequestVote(args) })
	return reply, err
}

func (e *endpoint) AppendEntries(to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply *raft.AppendEntriesReply
	err := e.net.deliver(e.from, to, func(h raft.Handler) { reply = h.HandleAppendEntries(args) })
	return reply, err
}

// leg decides whether one message from a to b gets through and how long it takes.
func (n *Network) leg(from, to int) (h raft.Handler, delay time.Duration, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	h = n.handlers[to]
	if h == nil || n.isolated[from] || n.isolated[to] || n.group[from] != n.group[to] {
		return nil, 0, false
	}
	if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		return nil, 0, false
	}
	if n.maxDelay > n.minDelay {
		delay = n.minDelay + time.Duration(n.rng.Int63n(int64(n.maxDelay-n.minDelay)))
	} else {
		delay = n.minDelay
	}
	return h, delay, true
}

// deliver sends a request and returns once the reply is back. The reply travels as a
// separate message, so it can be lost after the receiver has already acted on the request.
func (n *Network) deliver(from, to int, call func(raft.Handler)) error {
	h, delay, ok := n.leg(from, to)
	if !ok {
		return ErrUnreachable
	}
	time.Sleep(delay)
	call(h)
	if _, delay, ok = n.leg(to, from); !ok {
		return ErrUnreachable
	}
	time.Sleep(delay)
	return nil
}
