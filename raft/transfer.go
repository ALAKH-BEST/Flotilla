package raft

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrBadTransferTarget means the node asked to take over is not a voting member other than the leader.
	ErrBadTransferTarget = errors.New("raft: leadership can only be handed to another voting member")
	// ErrTransferInProgress means a hand-over is already running on this leader.
	ErrTransferInProgress = errors.New("raft: a leadership transfer is already in progress")
	// ErrTransferTimeout means the target did not take over in time, so this node stays leader.
	ErrTransferTimeout = errors.New("raft: the target did not take over in time")
)

// TimeoutNowArgs tells a follower to start an election at once, without waiting for its timer.
// The leader sends it only when the follower's log is as long as its own, so the follower wins.
type TimeoutNowArgs struct {
	Term     int
	LeaderID int
}

type TimeoutNowReply struct {
	Term int
}

// TransferLeadership hands leadership to the voting member target, so a leader can be drained for
// maintenance without clients waiting out an election timeout. The leader stops accepting new
// commands, brings the target's log up to its own, then tells it to start an election immediately.
// It returns nil once this node has stepped down in favour of a newer term. If the target does not
// take over within Config.TransferTimeout, the transfer is cancelled, this node carries on as
// leader and the error is ErrTransferTimeout.
func (n *Node) TransferLeadership(ctx context.Context, target int) error {
	n.mu.Lock()
	switch {
	case n.stopped:
		n.mu.Unlock()
		return ErrStopped
	case n.role != Leader:
		n.mu.Unlock()
		return ErrNotLeader
	case n.transferring:
		n.mu.Unlock()
		return ErrTransferInProgress
	case target == n.cfg.ID:
		n.mu.Unlock()
		return nil
	case !n.isVoter(target):
		n.mu.Unlock()
		return ErrBadTransferTarget
	}
	n.transferring = true
	n.transferTarget = target
	n.transferDeadline = time.Now().Add(n.cfg.TransferTimeout)
	n.transferSentAt = time.Time{}
	done := make(chan error, 1)
	n.transferDone = done
	n.broadcastAppendEntries() // the target may be behind: start catching it up
	n.maybeSendTimeoutNow(time.Now())
	n.mu.Unlock()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		n.mu.Lock()
		if n.transferDone == done {
			n.endTransfer(ctx.Err())
		}
		n.mu.Unlock()
		return <-done
	case <-n.stopCh:
		return ErrStopped
	}
}

// maybeSendTimeoutNow sends the go-ahead to the target once its log matches ours, and again if the
// message may have been lost (the lock must be held).
func (n *Node) maybeSendTimeoutNow(now time.Time) {
	if !n.transferring || n.matchIndex[n.transferTarget] < n.lastLogIndex() {
		return
	}
	if !n.transferSentAt.IsZero() && now.Sub(n.transferSentAt) < n.cfg.ElectionTimeoutMin {
		return
	}
	n.transferSentAt = now
	args := TimeoutNowArgs{Term: n.currentTerm, LeaderID: n.cfg.ID}
	target := n.transferTarget
	go func() { _, _ = n.cfg.Transport.TimeoutNow(target, &args) }()
}

// checkTransfer gives up on a transfer that took too long, and otherwise sends the go-ahead as soon
// as the target is ready (the lock must be held, on the leader).
func (n *Node) checkTransfer(now time.Time) {
	if !n.transferring {
		return
	}
	if !now.Before(n.transferDeadline) {
		n.endTransfer(ErrTransferTimeout)
		return
	}
	n.maybeSendTimeoutNow(now)
}

// endTransfer finishes a transfer, telling the caller how it went (the lock must be held).
func (n *Node) endTransfer(err error) {
	if !n.transferring {
		return
	}
	n.transferring = false
	n.transferDone <- err
}

// HandleTimeoutNow starts an election right away, but only on the word of the leader of the
// current term, and skipping the pre-vote: the leader chose this moment, so the usual check that
// the leader has gone quiet does not apply.
func (n *Node) HandleTimeoutNow(args *TimeoutNowArgs) *TimeoutNowReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &TimeoutNowReply{Term: n.currentTerm}
	if n.stopped || args.Term != n.currentTerm || n.role == Leader || n.leaderID != args.LeaderID || !n.isVoter(n.cfg.ID) {
		return reply
	}
	n.startElection()
	reply.Term = n.currentTerm
	return reply
}
