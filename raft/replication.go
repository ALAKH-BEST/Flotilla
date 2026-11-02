package raft

// broadcastAppendEntries sends entries (or a heartbeat) to every follower (the lock must be held).
func (n *Node) broadcastAppendEntries() {
	for _, peer := range n.cfg.Peers {
		if peer != n.cfg.ID {
			go n.replicateTo(peer)
		}
	}
}

// replicateTo sends one AppendEntries to a follower and handles the reply.
func (n *Node) replicateTo(peer int) {
	n.mu.Lock()
	if n.stopped || n.role != Leader {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[peer]
	prev := next - 1
	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prev,
		PrevLogTerm:  n.log[prev].Term,
		Entries:      append([]Entry(nil), n.log[next:]...),
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.cfg.Transport.AppendEntries(peer, args)
	if err != nil {
		return // lost; the next heartbeat tries again
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.currentTerm != args.Term || n.role != Leader {
		return // we are no longer the leader that sent this
	}
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		n.resetElectionDeadline()
		return
	}
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > n.matchIndex[peer] { // replies can arrive out of order
			n.matchIndex[peer] = match
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommit()
		return
	}
	if n.nextIndex[peer] != next {
		return // a newer reply already moved this follower
	}

	// Log mismatch: back up using the follower's hint.
	ni := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		for i := len(n.log) - 1; i >= 1; i-- {
			if n.log[i].Term == reply.ConflictTerm {
				ni = i + 1 // we have that term: resume after our last entry of it
				break
			}
			if n.log[i].Term < reply.ConflictTerm {
				break
			}
		}
	}
	if ni >= next {
		ni = next - 1 // always make progress
	}
	if ni < 1 {
		ni = 1
	}
	n.nextIndex[peer] = ni
	go n.replicateTo(peer)
}

// advanceCommit commits the highest entry of the current term stored on a majority (the lock must be held).
func (n *Node) advanceCommit() {
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			break // only entries from the current term are counted directly
		}
		count := 0
		for _, peer := range n.cfg.Peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count*2 > len(n.cfg.Peers) {
			n.commitIndex = idx
			n.applyCond.Broadcast()
			return
		}
	}
}

// HandleAppendEntries handles replication and heartbeats from a leader.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || args.Term < n.currentTerm {
		return &AppendEntriesReply{Term: n.currentTerm}
	}
	// A valid leader for this term: follow it (this also turns a candidate back into a follower).
	n.becomeFollower(args.Term)
	n.leaderID = args.LeaderID
	n.resetElectionDeadline()

	reply := &AppendEntriesReply{Term: n.currentTerm}
	if args.PrevLogIndex > n.lastLogIndex() {
		reply.ConflictIndex = n.lastLogIndex() + 1 // our log is too short
		return reply
	}
	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = n.log[args.PrevLogIndex].Term
		reply.ConflictIndex = args.PrevLogIndex
		for reply.ConflictIndex > 1 && n.log[reply.ConflictIndex-1].Term == reply.ConflictTerm {
			reply.ConflictIndex--
		}
		return reply
	}

	// Append new entries. Only truncate at a real conflict: a delayed or duplicate
	// message must never remove entries that are already correct.
	changed := false
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= n.lastLogIndex() {
			if n.log[idx].Term == entry.Term {
				continue
			}
			n.log = n.log[:idx]
		}
		n.log = append(n.log, args.Entries[i:]...)
		changed = true
		break
	}
	if changed {
		n.persist()
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		// Never move the commit index backwards: a delayed message can carry an old view.
		if newCommit := min(args.LeaderCommit, lastNew); newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.applyCond.Broadcast()
		}
	}
	reply.Success = true
	return reply
}
