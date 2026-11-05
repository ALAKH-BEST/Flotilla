package raft

import (
	"errors"
	"sort"
)

var (
	// ErrChangeInProgress means an earlier membership change has not committed yet. Changes
	// go one server at a time, which is what keeps any two majorities overlapping.
	ErrChangeInProgress = errors.New("raft: a membership change is already in progress")
	// ErrAlreadyMember and ErrNotMember report a change that would do nothing.
	ErrAlreadyMember = errors.New("raft: that server is already a member")
	ErrNotMember     = errors.New("raft: that server is not a member")
	// ErrLastMember means the change would leave the cluster with no members.
	ErrLastMember = errors.New("raft: cannot remove the last member")
)

func sortedMembers(ms []Member) []Member {
	out := append([]Member(nil), ms...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func sameMembers(a, b []Member) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// membersAt returns the membership in effect at a log index: the newest membership entry at or
// before it, or the snapshot's membership if there is none (the lock must be held).
func (n *Node) membersAt(index int) []Member {
	for i := index; i > n.snapshotIndex; i-- {
		if m := n.log[i-n.snapshotIndex].Members; len(m) > 0 {
			return m
		}
	}
	return n.snapshotMembers
}

// refreshMembers recomputes the membership from the log. It must run whenever the log's tail
// changes, because a node uses the newest membership entry as soon as it has it, and a
// truncated entry stops counting (the lock must be held).
func (n *Node) refreshMembers() {
	next := n.membersAt(n.lastLogIndex())
	if sameMembers(next, n.members) {
		return
	}
	old := n.members
	n.members = next
	had := make(map[int]bool, len(old))
	for _, m := range old {
		had[m.ID] = true
	}
	for _, m := range next {
		if !had[m.ID] { // a server joining: start replicating to it from the end of the log
			n.nextIndex[m.ID] = n.lastLogIndex() + 1
			n.matchIndex[m.ID] = 0
			n.ackSeq[m.ID] = 0
		}
	}
	if n.cfg.OnMembership != nil {
		n.cfg.OnMembership(append([]Member(nil), next...))
	}
}

func (n *Node) isMember(id int) bool {
	for _, m := range n.members {
		if m.ID == id {
			return true
		}
	}
	return false
}

func (n *Node) memberIDs() []int {
	ids := make([]int, 0, len(n.members))
	for _, m := range n.members {
		ids = append(ids, m.ID)
	}
	return ids
}

// isMajority reports whether count nodes are a majority of the current membership.
func (n *Node) isMajority(count int) bool { return count*2 > len(n.members) }

// Members returns the voting members this node currently uses.
func (n *Node) Members() []Member {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Member(nil), n.members...)
}

// AddServer starts adding a server to the cluster. It returns the index and term of the
// membership entry, which takes effect on this leader at once and is done when that index
// commits. Start the new node (with Config.Join) first, so it can receive the log.
func (n *Node) AddServer(m Member) (index, term int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.canChange(); err != nil {
		return 0, 0, err
	}
	if n.isMember(m.ID) {
		return 0, 0, ErrAlreadyMember
	}
	return n.proposeMembers(sortedMembers(append(append([]Member(nil), n.members...), m)))
}

// RemoveServer starts removing a server from the cluster, with the same guarantees as AddServer.
// A leader can remove itself: it keeps leading until the change commits, then steps down.
func (n *Node) RemoveServer(id int) (index, term int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.canChange(); err != nil {
		return 0, 0, err
	}
	if !n.isMember(id) {
		return 0, 0, ErrNotMember
	}
	if len(n.members) == 1 {
		return 0, 0, ErrLastMember
	}
	var rest []Member
	for _, m := range n.members {
		if m.ID != id {
			rest = append(rest, m)
		}
	}
	return n.proposeMembers(rest)
}

// canChange checks the conditions for starting a membership change (the lock must be held).
func (n *Node) canChange() error {
	if n.stopped {
		return ErrStopped
	}
	if n.role != Leader {
		return ErrNotLeader
	}
	// Until the leader commits an entry of its own term it cannot tell which earlier entries,
	// including membership entries, are committed.
	if n.termAt(n.commitIndex) != n.currentTerm {
		return ErrNotReady
	}
	for i := n.commitIndex + 1; i <= n.lastLogIndex(); i++ {
		if len(n.log[i-n.snapshotIndex].Members) > 0 {
			return ErrChangeInProgress
		}
	}
	return nil
}

func (n *Node) proposeMembers(members []Member) (index, term int, err error) {
	n.log = append(n.log, Entry{Term: n.currentTerm, Members: members})
	n.persist()
	index = n.lastLogIndex()
	n.matchIndex[n.cfg.ID] = index
	n.refreshMembers() // the new membership applies from the moment it is in the log
	n.broadcastAppendEntries()
	n.advanceCommit()
	return index, n.currentTerm, nil
}
