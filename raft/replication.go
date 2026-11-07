package raft

import "time"

// broadcastAppendEntries sends entries (or a heartbeat) to every follower (the lock must be held).
func (n *Node) broadcastAppendEntries() {
	for _, m := range n.members {
		if m.ID != n.cfg.ID {
			go n.replicateTo(m.ID)
		}
	}
	n.notifyDeparting()
}

// replicateTo brings one follower up to date: with log entries, or with the snapshot
// when the entries it needs have already been compacted away.
func (n *Node) replicateTo(peer int) {
	n.mu.Lock()
	if n.stopped || n.role != Leader || (!n.isMember(peer) && n.departing[peer] == nil) {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[peer]
	if next <= n.snapshotIndex {
		n.sendSnapshot(peer) // releases the lock
		return
	}
	prev := next - 1
	n.sendSeq++
	seq := n.sendSeq
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
	n.acked(peer, seq) // any answer in our term shows the peer accepts us as leader
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > n.matchIndex[peer] { // replies can arrive out of order
			n.matchIndex[peer] = match
		}
		if n.nextIndex[peer] < n.matchIndex[peer]+1 {
			n.nextIndex[peer] = n.matchIndex[peer] + 1
		}
		if d := n.departing[peer]; d != nil && match >= d.index && args.LeaderCommit >= d.index {
			delete(n.departing, peer) // it has the removal and knows it is committed
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

// sendSnapshot streams the snapshot to a follower that is behind it, one chunk at a time. It is
// called with the lock held and releases it. Only one transfer per follower runs at once; each
// chunk's reply says where to continue, and every reply counts as the follower answering us.
func (n *Node) sendSnapshot(peer int) {
	if _, busy := n.sendingSnap[peer]; busy {
		n.mu.Unlock()
		return
	}
	n.sendSeq++
	owner := n.sendSeq
	n.sendingSnap[peer] = owner
	term := n.currentTerm
	index, lastTerm := n.snapshotIndex, n.snapshotTerm
	data, members := n.snapshot, n.snapshotMembers // never modified once set
	offset := 0
	if p := n.snapOffset[peer]; p.index == index && p.term == lastTerm {
		offset = min(p.offset, len(data)) // resume an interrupted transfer
	}
	n.mu.Unlock()

	finish := func() { // the lock must be held
		if n.sendingSnap[peer] == owner {
			delete(n.sendingSnap, peer)
		}
	}
	for {
		end := min(offset+n.cfg.SnapshotChunkSize, len(data))
		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderID:          n.cfg.ID,
			LastIncludedIndex: index,
			LastIncludedTerm:  lastTerm,
			Offset:            offset,
			Data:              data[offset:end],
			Done:              end == len(data),
			Members:           members,
		}
		n.mu.Lock()
		n.sendSeq++
		seq := n.sendSeq
		n.mu.Unlock()

		reply, err := n.cfg.Transport.InstallSnapshot(peer, args)

		n.mu.Lock()
		if err != nil || n.stopped || n.currentTerm != term || n.role != Leader {
			if err != nil {
				n.snapOffset[peer] = snapProgress{index, lastTerm, offset}
			}
			finish()
			n.mu.Unlock()
			return // lost; the next heartbeat starts again, and the follower says where it was
		}
		if reply.Term > n.currentTerm {
			finish()
			n.becomeFollower(reply.Term)
			n.resetElectionDeadline()
			n.mu.Unlock()
			return
		}
		n.acked(peer, seq)
		if reply.Installed {
			if index > n.matchIndex[peer] {
				n.matchIndex[peer] = index
			}
			if n.nextIndex[peer] < n.matchIndex[peer]+1 {
				n.nextIndex[peer] = n.matchIndex[peer] + 1
			}
			finish()
			if d := n.departing[peer]; d != nil && index >= d.index {
				delete(n.departing, peer) // the snapshot told it it is gone
			}
			n.advanceCommit()
			n.mu.Unlock()
			go n.replicateTo(peer) // send whatever follows the snapshot
			return
		}
		if n.snapshotIndex != index || (!n.isMember(peer) && n.departing[peer] == nil) {
			finish()
			n.mu.Unlock()
			go n.replicateTo(peer) // our snapshot moved on (or the peer left): start over
			return
		}
		offset = max(0, min(reply.NextOffset, len(data)))
		n.snapOffset[peer] = snapProgress{index, lastTerm, offset}
		n.mu.Unlock()
	}
}

// advanceCommit commits the highest entry of the current term stored on a majority (the lock must be held).
func (n *Node) advanceCommit() {
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		if n.termAt(idx) != n.currentTerm {
			break // only entries from the current term are counted directly
		}
		count := 0
		for _, m := range n.voters() { // a removed leader does not count itself; learners never count
			if n.matchIndex[m.ID] >= idx {
				count++
			}
		}
		if n.isMajority(count) {
			old := n.commitIndex
			n.commitIndex = idx
			n.noteCommit(old)
			n.applyCond.Broadcast()
			if !n.isVoter(n.cfg.ID) {
				// The membership that dropped this leader has committed, so its job is done.
				n.becomeFollower(n.currentTerm)
				n.resetElectionDeadline()
			}
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
	n.lastLeaderContact = time.Now()
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
		n.refreshMembers() // an appended entry may change the membership; a truncated one may undo it
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew := prev + len(entries)
		// Never move the commit index backwards: a delayed message can carry an old view.
		if newCommit := min(args.LeaderCommit, lastNew); newCommit > n.commitIndex {
			old := n.commitIndex
			n.commitIndex = newCommit
			n.noteCommit(old)
			n.applyCond.Broadcast()
		}
	}
	reply.Success = true
	return reply
}

// snapProgress is how many bytes of one snapshot have been sent to a follower.
type snapProgress struct{ index, term, offset int }

// incomingSnapshot is a snapshot being received from a leader, a chunk at a time.
type incomingSnapshot struct {
	term, leader int // the transfer belongs to one leader in one term
	index, last  int // LastIncludedIndex and LastIncludedTerm
	data         []byte
}

// HandleInstallSnapshot receives a snapshot from the leader, chunk by chunk, and replaces this
// node's state with it once the last chunk has arrived.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || args.Term < n.currentTerm {
		return &InstallSnapshotReply{Term: n.currentTerm}
	}
	n.becomeFollower(args.Term)
	n.leaderID = args.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionDeadline()

	reply := &InstallSnapshotReply{Term: n.currentTerm}
	if args.LastIncludedIndex <= n.commitIndex {
		n.incoming = nil
		reply.Installed = true // we already have everything this snapshot covers
		return reply
	}

	// Chunks must arrive in order from the same transfer; anything else is answered with how much
	// we hold, so the leader can carry on from there. Offset 0 always starts a transfer afresh.
	in := n.incoming
	if args.Offset == 0 {
		in = &incomingSnapshot{term: args.Term, leader: args.LeaderID, index: args.LastIncludedIndex, last: args.LastIncludedTerm}
		n.incoming = in
	}
	if in == nil || in.term != args.Term || in.leader != args.LeaderID ||
		in.index != args.LastIncludedIndex || in.last != args.LastIncludedTerm {
		n.incoming = nil
		return reply // NextOffset 0: start from the beginning
	}
	if args.Offset != len(in.data) {
		reply.NextOffset = len(in.data) // a duplicate or a gap
		return reply
	}
	in.data = append(in.data, args.Data...)
	reply.NextOffset = len(in.data)
	if !args.Done {
		return reply
	}
	n.incoming = nil

	committedBefore := n.membersAt(n.commitIndex)

	// Keep the entries after the snapshot if they agree with it; otherwise drop the whole log.
	if args.LastIncludedIndex <= n.lastLogIndex() && n.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		n.log = append([]Entry{{Term: args.LastIncludedTerm}}, n.log[args.LastIncludedIndex-n.snapshotIndex+1:]...)
	} else {
		n.log = []Entry{{Term: args.LastIncludedTerm}}
	}
	n.snapshotIndex = args.LastIncludedIndex
	n.snapshotTerm = args.LastIncludedTerm
	if len(args.Members) > 0 {
		n.snapshotMembers = args.Members
	}
	n.snapshot = in.data
	n.noteRemoval(committedBefore, n.snapshotMembers)
	n.commitIndex = args.LastIncludedIndex
	n.lastApplied = args.LastIncludedIndex
	n.pendingSnapshot = n.snapshotMsg()
	n.refreshMembers()
	n.persist()
	n.applyCond.Broadcast()
	reply.Installed = true
	return reply
}
