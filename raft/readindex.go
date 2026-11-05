package raft

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotLeader means the node is not the leader, or stopped being the leader before it could confirm the read.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrNotReady means a new leader has not yet committed an entry of its own term, so its commit
	// index may still be behind. The caller can retry shortly or read through the log instead.
	ErrNotReady = errors.New("raft: leader has not committed an entry in its term yet")
	// ErrStopped means the node was shut down.
	ErrStopped = errors.New("raft: stopped")
)

// readReq is a read waiting for proof that this node was still the leader.
type readReq struct {
	seq   int // only AppendEntries sent after the read arrived count as proof
	index int // the commit index when the read arrived
	done  chan error
}

// ReadIndex returns an index such that once the state machine has applied up to it, a read of
// the state machine is linearizable. It needs no log entry and no disk write: the leader
// notes its commit index, then confirms with one round of heartbeats that a majority still
// accepts it as leader, so no newer leader can have committed anything it has not seen.
func (n *Node) ReadIndex(ctx context.Context) (int, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return 0, ErrStopped
	}
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	// Until the leader commits an entry of its own term (its no-op), its commit index can lag
	// behind entries that earlier leaders already committed.
	if n.termAt(n.commitIndex) != n.currentTerm {
		n.mu.Unlock()
		return 0, ErrNotReady
	}
	req := &readReq{seq: n.sendSeq, index: n.commitIndex, done: make(chan error, 1)}
	if n.confirmed(req.seq) { // a single-node cluster needs no one else
		n.mu.Unlock()
		return req.index, nil
	}
	n.reads = append(n.reads, req)
	n.broadcastAppendEntries()
	n.mu.Unlock()

	select {
	case err := <-req.done:
		if err != nil {
			return 0, err
		}
		return req.index, nil
	case <-ctx.Done():
		n.dropRead(req)
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrStopped
	}
}

// confirmed reports whether a majority (counting this node) has answered a message sent after seq
// (the lock must be held).
func (n *Node) confirmed(seq int) bool {
	count := 0
	for _, m := range n.voters() {
		if m.ID == n.cfg.ID || n.ackSeq[m.ID] > seq {
			count++
		}
	}
	return n.isMajority(count)
}

// acked records that a peer answered the message with this sequence number in our term, and
// releases any reads that this completes (the lock must be held).
func (n *Node) acked(peer, seq int) {
	if seq > n.ackSeq[peer] {
		n.ackSeq[peer] = seq
	}
	n.lastContact[peer] = time.Now()
	kept := n.reads[:0]
	for _, r := range n.reads {
		if n.confirmed(r.seq) {
			r.done <- nil
		} else {
			kept = append(kept, r)
		}
	}
	n.reads = kept
}

// failReads gives up on every waiting read (the lock must be held).
func (n *Node) failReads(err error) {
	for _, r := range n.reads {
		r.done <- err
	}
	n.reads = nil
}

func (n *Node) dropRead(req *readReq) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, r := range n.reads {
		if r == req {
			n.reads = append(n.reads[:i], n.reads[i+1:]...)
			return
		}
	}
}

// hasQuorumContact reports whether a majority of the voters, this node included, has answered
// within the last election timeout (the lock must be held).
func (n *Node) hasQuorumContact(now time.Time) bool {
	count := 0
	for _, m := range n.voters() {
		if m.ID == n.cfg.ID || now.Sub(n.lastContact[m.ID]) <= n.cfg.ElectionTimeoutMax {
			count++
		}
	}
	return n.isMajority(count)
}
