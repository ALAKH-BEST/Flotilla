package raft

// broadcastAppendEntries sends entries (or a heartbeat) to every follower (the lock must be held).
func (n *Node) broadcastAppendEntries() {
	for _, peer := range n.cfg.Peers {
		if peer != n.cfg.ID {
			go n.replicateTo(peer)
		}
	}
}

// replicateTo brings one follower up to date: with log entries, or with the snapshot
// when the entries it needs have already been compacted away.
func (n *Node) replicateTo(peer int) {
	n.mu.Lock()
	if n.stopped || n.role != Leader {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[peer]
	if next <= n.snapshotIndex {
		n.sendSnapshot(peer) // releases the lock
		return
	}
	prev := next - 1
	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prev,
		PrevLogTerm:  n.termAt(prev),
		Entries:      n.entriesFrom(next),
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
		if n.nextIndex[peer] < n.matchIndex[peer]+1 {
			n.nextIndex[peer] = n.matchIndex[peer] + 1
		}
		n.advanceCommit()
		return
	}
	if n.nextIndex[peer] != next {
		return // a newer reply already moved this follower
	}

	// Log mismatch: back up using the follower's hint.
	ni := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		for i := n.lastLogIndex(); i > n.snapshotIndex; i-- {
			if n.termAt(i) == reply.ConflictTerm {
				ni = i + 1 // we have that term: resume after our last entry of it
				break
			}
			if n.termAt(i) < reply.ConflictTerm {
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
	n.nextIndex[peer] = ni // at or below the snapshot, the next round sends the snapshot
	go n.replicateTo(peer)
}

// sendSnapshot sends the snapshot to a follower that is behind it. It is called with
// the lock held and releases it.
func (n *Node) sendSnapshot(peer int) {
	args := &InstallSnapshotArgs{
		Term:              n.currentTerm,
		LeaderID:          n.cfg.ID,
		LastIncludedIndex: n.snapshotIndex,
		LastIncludedTerm:  n.snapshotTerm,
		Data:              n.snapshot,
	}
	n.mu.Unlock()

	reply, err := n.cfg.Transport.InstallSnapshot(peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.currentTerm != args.Term || n.role != Leader {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		n.resetElectionDeadline()
		return
	}
	if args.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = args.LastIncludedIndex
	}
	if n.nextIndex[peer] < n.matchIndex[peer]+1 {
		n.nextIndex[peer] = n.matchIndex[peer] + 1
	}
	n.advanceCommit()
	go n.replicateTo(peer) // send whatever follows the snapshot
}

// advanceCommit commits the highest entry of the current term stored on a majority (the lock must be held).
func (n *Node) advanceCommit() {
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		if n.termAt(idx) != n.currentTerm {
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
	prev, entries := args.PrevLogIndex, args.Entries

	if prev < n.snapshotIndex {
		// The leader is behind our snapshot. Everything up to the snapshot is committed
		// and so identical on every node: skip what we already have.
		skip := min(n.snapshotIndex-prev, len(entries))
		entries = entries[skip:]
		prev += skip
		if prev < n.snapshotIndex {
			reply.Success = true // nothing here goes beyond the snapshot
			return reply
		}
	} else {
		if prev > n.lastLogIndex() {
			reply.ConflictIndex = n.lastLogIndex() + 1 // our log is too short
			return reply
		}
		if n.termAt(prev) != args.PrevLogTerm {
			reply.ConflictTerm = n.termAt(prev)
			reply.ConflictIndex = prev
			for reply.ConflictIndex > n.snapshotIndex+1 && n.termAt(reply.ConflictIndex-1) == reply.ConflictTerm {
				reply.ConflictIndex--
			}
			return reply
		}
	}

	// Append new entries. Only truncate at a real conflict: a delayed or duplicate
	// message must never remove entries that are already correct.
	changed := false
	for i, entry := range entries {
		idx := prev + 1 + i
		if idx <= n.lastLogIndex() {
			if n.termAt(idx) == entry.Term {
				continue
			}
			n.log = n.log[:idx-n.snapshotIndex]
		}
		n.log = append(n.log, entries[i:]...)
		changed = true
		break
	}
	if changed {
		n.persist()
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew := prev + len(entries)
		// Never move the commit index backwards: a delayed message can carry an old view.
		if newCommit := min(args.LeaderCommit, lastNew); newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.applyCond.Broadcast()
		}
	}
	reply.Success = true
	return reply
}

// HandleInstallSnapshot replaces this node's state with a snapshot from the leader.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || args.Term < n.currentTerm {
		return &InstallSnapshotReply{Term: n.currentTerm}
	}
	n.becomeFollower(args.Term)
	n.leaderID = args.LeaderID
	n.resetElectionDeadline()

	reply := &InstallSnapshotReply{Term: n.currentTerm}
	if args.LastIncludedIndex <= n.commitIndex {
		return reply // we already have everything this snapshot covers
	}

	// Keep the entries after the snapshot if they agree with it; otherwise drop the whole log.
	if args.LastIncludedIndex <= n.lastLogIndex() && n.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		n.log = append([]Entry{{Term: args.LastIncludedTerm}}, n.log[args.LastIncludedIndex-n.snapshotIndex+1:]...)
	} else {
		n.log = []Entry{{Term: args.LastIncludedTerm}}
	}
	n.snapshotIndex = args.LastIncludedIndex
	n.snapshotTerm = args.LastIncludedTerm
	n.snapshot = append([]byte(nil), args.Data...)
	n.commitIndex = args.LastIncludedIndex
	n.lastApplied = args.LastIncludedIndex
	n.pendingSnapshot = n.snapshotMsg()
	n.persist()
	n.applyCond.Broadcast()
	return reply
}
